"""Regression tests.

Every test marked REGRESSION pins a bug that was present before the src-layout
rewrite; each one failed against the original dbcache.py.
"""
import sqlite3
from dataclasses import dataclass
from datetime import timedelta
from typing import Annotated

import pytest

from dbcache import CacheMiss, database_cache
from dbcache._core import Cache


@pytest.fixture
def db(tmp_path):
	return tmp_path / "cache.sqlite"


# --- eviction -----------------------------------------------------------------

def test_eviction_removes_exactly_the_batch(db):
	"""REGRESSION: OFFSET n landed on the n+1'th oldest row and `<=` included
	it, so eviction always removed one row too many."""
	@database_cache(db, max_size=10, evict_batch=3)
	def f(x: int) -> int:
		return x * 2

	for i in range(10):
		f(i)
	# pin the timestamps so this asserts eviction arithmetic, not clock behaviour
	f.conn.execute('UPDATE "f" SET timestamp = "x"')
	f.conn.commit()
	assert len(f.contents()) == 10

	f(100)  # the 11th entry trips eviction; its timestamp is time.time(), newest
	assert len(f.contents()) == 8
	assert f.size == 8
	assert {row[0] for row in f.contents()} == {3, 4, 5, 6, 7, 8, 9, 100}


def test_eviction_survives_a_burst_of_writes(db):
	"""REGRESSION: timestamps were whole seconds, so a burst of entries written
	inside one second all tied and `timestamp <= cutoff` emptied the table.
	Sub-second timestamps keep the cutoff discriminating."""
	@database_cache(db, max_size=10, evict_batch=3)
	def f(x: int) -> int:
		return x * 2

	for i in range(11):  # written as fast as the loop allows
		f(i)

	# the scenario actually under test: these all land in the same wall second
	assert len({int(row[-1]) for row in f.contents()}) <= 2
	assert len(f.contents()) == 8
	assert f.size == 8


def test_evict_beyond_table_size_is_clamped(db):
	"""A count past the end made the subquery NULL, and `<= NULL` deleted
	nothing at all rather than everything."""
	@database_cache(db, max_size=10)
	def f(x: int) -> int:
		return x

	for i in range(5):
		f(i)
	f.evict(500)
	assert f.contents() == []
	assert f.size == 0


def test_eviction_batch_is_never_zero(db):
	"""REGRESSION: int(0.2 * max_size) is 0 for max_size < 5, so eviction
	deleted nothing and the cache stayed permanently over its limit."""
	@database_cache(db, max_size=4)
	def f(x: int) -> int:
		return x

	for i in range(10):
		f(i)
	assert f.evict_batch == 1
	assert len(f.contents()) <= 4


def test_max_size_can_shrink_between_runs(db):
	"""REGRESSION: evict() ran before evict_cmd was assigned (AttributeError),
	and the vacuum() that followed hit "cannot VACUUM from within a transaction"."""
	@database_cache(db, name="f", max_size=100)
	def f(x: int) -> int:
		return x * 2

	for i in range(30):
		f(i)
	f.close()

	@database_cache(db, name="f", max_size=10)
	def reopened(x: int) -> int:
		return x * 2

	assert reopened.size == 10
	assert len(reopened.contents()) == 10


def test_vacuum_after_delete(db):
	"""REGRESSION: vacuum() committed after the VACUUM, so a preceding DELETE
	left a transaction open and VACUUM raised OperationalError."""
	@database_cache(db, max_size=10)
	def f(x: int) -> int:
		return x

	for i in range(5):
		f(i)
	f.evict(2)
	f.vacuum()
	assert len(f.contents()) == 3


# --- SQL identifier quoting ---------------------------------------------------

def test_sql_keyword_as_function_and_parameter_name(db):
	"""REGRESSION: identifiers were interpolated raw, so `add` and `order`
	produced `OperationalError: near "add": syntax error`."""
	@database_cache(db)
	def add(order: int, group: str) -> int:
		return order

	assert add(1, "a") == 1
	assert add(1, "a") == 1  # cache hit


def test_quote_in_table_name(db):
	"""REGRESSION: a caller-supplied name was an injection vector."""
	@database_cache(db, name='we"ird name')
	def f(x: int) -> int:
		return x

	assert f(1) == 1
	assert f(1) == 1


# --- value round-tripping -----------------------------------------------------

def test_bool_round_trips(db):
	"""REGRESSION: bool was stored as INTEGER and read back as 0/1, so a cache
	hit returned int where the fresh call returned bool."""
	@database_cache(db)
	def is_even(x: int) -> bool:
		return x % 2 == 0

	assert is_even(4) is True
	assert is_even(4) is True  # cache hit
	assert is_even(3) is False
	assert is_even(3) is False


def test_nullable_bool_round_trips(db):
	"""REGRESSION: a nullable bool must come back None, not False."""
	@database_cache(db)
	def maybe(x: int) -> bool | None:
		return None if x == 0 else x > 0

	assert maybe(0) is None
	assert maybe(0) is None
	assert maybe(5) is True
	assert maybe(5) is True


def test_dataclass_return(db):
	@dataclass
	class Point:
		x: int
		y: str

	@database_cache(db)
	def mk(n: int) -> Point:
		return Point(n, str(n))

	assert mk(3) == Point(3, "3")
	assert mk(3) == Point(3, "3")


def test_tuple_return(db):
	@database_cache(db)
	def pair(n: int) -> tuple[int, str]:
		return n, str(n)

	assert pair(3) == (3, "3")
	assert pair(3) == (3, "3")


def test_nullable_output(db):
	@database_cache(db)
	def f(x: int) -> str | None:
		return None if x == 0 else str(x)

	assert f(0) is None
	assert f(0) is None
	assert f(2) == "2"
	assert f(2) == "2"


def test_nullable_input_rejected(db):
	"""REGRESSION: an optional parameter is part of the WITHOUT ROWID primary
	key, so this used to fail at call time with a bare IntegrityError."""
	with pytest.raises(ValueError, match="cannot be nullable"):
		@database_cache(db)
		def f(x: int | None) -> str:
			return str(x)


def test_nullable_input_via_annotated_is_allowed(db):
	"""The documented escape hatch: map the optional onto a non-null column."""
	def encode(v: int | None) -> int:
		return -1 if v is None else v

	@database_cache(db)
	def f(x: Annotated[int | None, encode]) -> str:
		return str(x)

	assert f(None) == "None"
	assert f(None) == "None"
	assert f(2) == "2"


def test_future_annotations(db):
	"""REGRESSION: `from __future__ import annotations` makes every annotation a
	string, which failed with "unsupported type: 'int'"."""
	from _future_annotations import Point, build

	make = build(db)
	assert make(3) == Point(3, "3")
	assert make(3) == Point(3, "3")


# --- keyword arguments --------------------------------------------------------

def test_keyword_arguments(db):
	"""REGRESSION: __call__ took only *args, so a keyword call raised
	`TypeError: got an unexpected keyword argument`."""
	calls = []

	@database_cache(db)
	def plus(a: int, b: int) -> int:
		calls.append((a, b))
		return a + b

	assert plus(1, 2) == 3
	assert plus(1, b=2) == 3
	assert plus(a=1, b=2) == 3
	assert plus(b=2, a=1) == 3
	assert calls == [(1, 2)]  # all four spellings are one cache key


def test_defaults_are_applied(db):
	@database_cache(db)
	def f(a: int, b: int = 5) -> int:
		return a + b

	assert f(1) == 6
	assert f(1, 5) == 6
	assert f(1, b=5) == 6
	assert len(f.contents()) == 1


def test_keyword_only_parameter(db):
	"""REGRESSION: a keyword-only parameter decorated fine but was uncallable by
	any spelling -- both f(1, b=2) and f(1, 2) raised TypeError."""
	@database_cache(db)
	def f(a: int, *, b: int) -> int:
		return a + b

	assert f(1, b=2) == 3
	assert f(1, b=2) == 3  # cache hit
	assert len(f.contents()) == 1


def test_positional_only_parameter(db):
	@database_cache(db)
	def f(a: int, /, b: int) -> int:
		return a + b

	assert f(1, 2) == 3
	assert f(1, b=2) == 3
	assert len(f.contents()) == 1  # both spellings are one key


def test_var_positional_rejected(db):
	"""REGRESSION: *args has no fixed arity, so this decorated cleanly and then
	failed at call time with `type 'tuple' is not supported`."""
	with pytest.raises(ValueError, match="variadic"):
		@database_cache(db)
		def f(*xs: int) -> int:
			return sum(xs)


def test_var_keyword_rejected(db):
	"""REGRESSION: as above, failing with `type 'dict' is not supported`."""
	with pytest.raises(ValueError, match="variadic"):
		@database_cache(db)
		def f(a: int, **kw: int) -> int:
			return a


def test_reserved_parameter_names_rejected(db):
	"""REGRESSION: a parameter named max_age/cache/cache_only was silently
	swallowed as a control flag now that **kwargs are forwarded."""
	with pytest.raises(ValueError, match="reserved"):
		@database_cache(db, max_age=60)
		def fetch(url: str, max_age: int) -> str:
			return url


# --- error reporting ----------------------------------------------------------

def test_missing_table_reports_signature_change(db):
	@database_cache(db)
	def h(x: int) -> int:
		return x

	h(1)
	h.conn.execute('ALTER TABLE "h" RENAME TO "h_old"')
	with pytest.raises(ValueError, match="signature has changed"):
		h(2)


def test_changed_return_type_reports_signature_change(db):
	"""REGRESSION: quoting every identifier exposed SQLite's double-quoted
	string-literal misfeature. The missing columns were read as literals, so this
	returned P(a='return$0', b='return$1') without ever calling the function."""
	@database_cache(db, name="f")
	def v1(x: int) -> int:
		return x * 100

	v1(5)
	v1.close()

	@dataclass
	class P:
		a: int
		b: int

	@database_cache(db, name="f")
	def v2(x: int) -> P:
		return P(1, 2)

	with pytest.raises(ValueError, match="signature has changed"):
		v2(5)


def test_renamed_parameter_reports_signature_change(db):
	"""REGRESSION: as above, but the missing column sat in the WHERE clause,
	where it silently compared a string literal and reported a cache miss."""
	@database_cache(db, name="f")
	def v1(x: int) -> int:
		return x

	v1(5)
	v1.close()

	@database_cache(db, name="f")
	def v2(y: int) -> int:
		return y

	with pytest.raises(ValueError, match="signature has changed"):
		v2(5)


def test_other_operational_errors_are_not_disguised(db):
	"""REGRESSION: every OperationalError was reported as a signature change,
	hiding "database is locked", disk I/O errors and the like."""
	@database_cache(db)
	def h(x: int) -> int:
		return x

	class Locked:
		def execute(self, *args, **kwargs):
			raise sqlite3.OperationalError("database is locked")

	h.conn = Locked()
	with pytest.raises(sqlite3.OperationalError, match="locked"):
		h(1)


def test_cache_is_abstract():
	"""REGRESSION: Cache subclassed ABC but declared nothing abstract, so it was
	directly instantiable despite having no __call__."""
	with pytest.raises(TypeError):
		Cache(lambda x: x, ":memory:", "t")


def test_missing_return_annotation_rejected(db):
	with pytest.raises(ValueError, match="return type must be given"):
		@database_cache(db)
		def f(x: int):
			return x


def test_missing_parameter_annotation_rejected(db):
	with pytest.raises(ValueError, match="type of x must be given"):
		@database_cache(db)
		def f(x) -> int:
			return x


def test_bare_tuple_return_rejected(db):
	with pytest.raises(ValueError, match="must be parameterized"):
		@database_cache(db)
		def f(x: int) -> tuple:
			return (x,)


# --- cache_only / max_age -----------------------------------------------------

def test_cache_only_raises_on_miss(db):
	@database_cache(db)
	def f(x: int) -> int:
		return x

	with pytest.raises(CacheMiss):
		f(1, cache_only=True)
	f(1)
	assert f(1, cache_only=True) == 1


def test_cache_only_with_cache_false_is_rejected(db):
	"""REGRESSION: this combination silently called the function, the exact
	opposite of what cache_only asks for."""
	@database_cache(db)
	def f(x: int) -> int:
		return x

	with pytest.raises(ValueError, match="meaningless"):
		f(1, cache=False, cache_only=True)


def test_cache_false_refreshes(db):
	calls = []

	@database_cache(db)
	def f(x: int) -> int:
		calls.append(x)
		return x

	f(1)
	f(1)
	assert calls == [1]
	f(1, cache=False)
	assert calls == [1, 1]


def test_expired_entry_is_refreshed(db):
	calls = []

	@database_cache(db, max_age=60)
	def f(x: int) -> int:
		calls.append(x)
		return x * 10

	assert f(1) == 10
	assert f(1) == 10
	assert calls == [1]

	f.conn.execute('UPDATE "f" SET timestamp = timestamp - 120')
	f.conn.commit()
	assert f(1) == 10
	assert calls == [1, 1]


def test_per_call_max_age_override(db):
	calls = []

	@database_cache(db, max_age=timedelta(hours=1))
	def f(x: int) -> int:
		calls.append(x)
		return x

	f(1)
	f.conn.execute('UPDATE "f" SET timestamp = timestamp - 300')
	f.conn.commit()

	f(1, max_age=3600)  # still fresh
	assert calls == [1]
	f(1, max_age=10)  # stale under a shorter window
	assert calls == [1, 1]


def test_max_age_accepts_timedelta(db):
	@database_cache(db, max_age=timedelta(hours=1))
	def f(x: int) -> int:
		return x

	assert f.max_age == 3600


def test_refresh_does_not_grow_size(db):
	@database_cache(db, max_age=60)
	def f(x: int) -> int:
		return x

	f(1)
	f.conn.execute('UPDATE "f" SET timestamp = timestamp - 120')
	f.conn.commit()
	f(1)
	assert f.size == 1
	assert len(f.contents()) == 1


# --- Annotated serializers ----------------------------------------------------

def to_csv(v: tuple) -> str:
	return ",".join(map(str, v))


def from_csv(s):
	return tuple(int(p) for p in s.split(",")) if s else ()


def test_annotated_serializer_pair(db):
	@database_cache(db)
	def f(n: int) -> Annotated[tuple, (to_csv, from_csv)]:
		return tuple(range(n))

	assert f(3) == (0, 1, 2)
	assert f(3) == (0, 1, 2)


def test_annotated_serializer_only(db):
	"""Characterises the serializer-only form: deserialize is identity, so a
	cache hit yields the stored representation rather than the original value."""
	def as_text(v: object) -> str:
		return str(v)

	@database_cache(db)
	def f(n: int) -> Annotated[object, as_text]:
		return n * 2

	assert f(3) == 6  # fresh call, real value
	assert f(3) == "6"  # cache hit, stored representation


def test_annotated_on_input(db):
	@database_cache(db)
	def f(v: Annotated[tuple, to_csv]) -> int:
		return sum(v)

	assert f((1, 2, 3)) == 6
	assert f((1, 2, 3)) == 6


def test_annotated_with_too_much_metadata_rejected(db):
	"""REGRESSION: this died on an opaque tuple-unpack ValueError."""
	with pytest.raises(ValueError, match="single serializer annotation"):
		@database_cache(db)
		def f(n: int) -> Annotated[int, to_csv, from_csv]:
			return n


def test_annotated_with_unsized_metadata_rejected(db):
	"""REGRESSION: len(meta) raised TypeError instead of the intended ValueError."""
	with pytest.raises(ValueError, match="serializer function"):
		@database_cache(db)
		def f(n: int) -> Annotated[int, 5]:
			return n


def test_serializer_without_return_type_rejected(db):
	with pytest.raises(ValueError, match="serializer must have return type"):
		@database_cache(db)
		def f(n: int) -> Annotated[int, lambda v: str(v)]:
			return n


# --- misc ---------------------------------------------------------------------

def test_unsupported_type_rejected(db):
	with pytest.raises(ValueError, match="unsupported type"):
		@database_cache(db)
		def f(x: int) -> complex:
			return complex(x)


def test_non_optional_union_rejected(db):
	with pytest.raises(ValueError, match="Union not allowed"):
		@database_cache(db)
		def f(x: int) -> int | str:
			return x


def test_clear(db):
	@database_cache(db, max_size=10)
	def f(x: int) -> int:
		return x

	for i in range(5):
		f(i)
	f.clear()
	assert f.contents() == []
	assert f.size == 0


def test_wrapper_metadata_preserved(db):
	@database_cache(db)
	def documented(x: int) -> int:
		"""Doc."""
		return x

	assert documented.__name__ == "documented"
	assert documented.__doc__ == "Doc."
