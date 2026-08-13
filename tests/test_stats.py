"""Tests for _stats: on-disk reporting, which the cache itself never touches."""
import pytest

from dbcache import database_cache, stats
from dbcache._stats import size_expr


@pytest.fixture
def db(tmp_path):
	return tmp_path / "cache.sqlite"


def test_stats_reports_headroom(db):
	@database_cache(db)
	def small(x: int) -> int:
		return x * 2

	for i in range(5):
		small(i)
	s = stats(small)
	assert s.rows == 5
	assert not s.overflowing
	assert s.page_limit == 4061
	assert 'headroom' in str(s)


def test_stats_detects_overflow(db):
	@database_cache(db)
	def big(x: int) -> bytes:
		return bytes(5000)  # past maxLocal even for a rowid table

	big(1)
	s = stats(big)
	assert s.overflowing
	assert s.max_record > s.page_limit
	assert 'OVERFLOWING' in str(s)


def test_stats_breaks_down_by_column(db):
	"""The point of the rewrite: which column is eating the space."""
	@database_cache(db)
	def fetch(url: str, lang: str) -> str:
		return 'lorem ipsum ' * 40

	fetch('https://example.com', 'en')
	s = stats(fetch)
	sizes = {name: largest for name, _mean, largest in s.columns}
	assert sizes['url'] == len('https://example.com')
	assert sizes['lang'] == 2
	assert sizes['return'] == len('lorem ipsum ' * 40)
	assert sizes['timestamp'] <= 8
	# the body dominates, and the report says so
	assert max(sizes, key=sizes.get) == 'return'
	assert 'url' in str(s) and 'return' in str(s)


def test_stats_estimate_is_close_to_the_real_record(db):
	"""Within a few bytes of the exact serial-type arithmetic _core.py did."""
	@database_cache(db)
	def f(x: int) -> str:
		return 'y' * 900

	f(1)
	s = stats(f)
	# x(1) + return(900) + timestamp(4) plus headers: ~910, and certainly not
	# off by enough to move the overflow verdict
	assert 900 < s.max_record < 930


def test_stats_sizes_integers_by_magnitude_not_digits(db):
	"""A timestamp is 4 bytes and 10 characters; octet_length would say 10.
	This is the distinction the whole int-vs-REAL timestamp choice rested on."""
	@database_cache(db)
	def f(small: int, big: int) -> int:
		return 0

	f(1, 1786560274)
	sizes = {name: largest for name, _mean, largest in stats(f).columns}
	assert sizes['small'] == 0      # 0 and 1 have dedicated zero-width serial types
	assert sizes['big'] == 4        # < 2**31
	assert sizes['return'] == 0     # the returned 0
	assert sizes['timestamp'] == 4  # not 8, and certainly not 10


def test_stats_does_not_need_octet_length(db):
	"""octet_length() only exists from SQLite 3.43; length(cast(x AS BLOB))
	is equivalent for text and blobs and has always been there."""
	from dbcache._stats import size_expr

	@database_cache(db)
	def f(x: str) -> bytes:
		return b'abc'

	assert not any('octet_length' in size_expr(col) for col in f.columns)
	text = 'héllo'  # written escaped so no file encoding can mangle it
	assert len(text) == 5 and len(text.encode()) == 6
	f(text)
	sizes = {name: largest for name, _mean, largest in stats(f).columns}
	assert sizes['x'] == 6  # bytes, not the 5 characters that length() would count
	assert sizes['return'] == 3


def test_stats_on_an_empty_cache(db):
	@database_cache(db)
	def f(x: int) -> str:
		return str(x)

	s = stats(f)
	assert s.rows == 0
	assert not s.overflowing
	assert str(s)  # and it renders rather than dividing by zero


def test_stats_with_a_nullable_column(db):
	"""octet_length(NULL) is NULL; a NULL occupies no bytes."""
	@database_cache(db)
	def f(x: int) -> str | None:
		return None

	f(1)
	s = stats(f)
	assert dict((n, m) for n, m, _ in s.columns)['return'] == 0
