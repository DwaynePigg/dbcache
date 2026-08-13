"""The _core regression suite, ported to _core2.

Dropped: stats()/record-size tests, the rowid flag tests, the evict_batch
tests and the ABC test -- those features were deliberately removed. Eviction
expectations changed: the cache now never exceeds max_size and evicts exactly
the overage, not a batch.
"""
import sqlite3
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Annotated

import pytest

from dbcache._core import CacheMiss, database_cache, make_codec


@pytest.fixture
def db(tmp_path):
	return tmp_path / "cache.sqlite"


# --- eviction -----------------------------------------------------------------

def test_eviction_removes_only_the_oldest(db):
	@database_cache(db, max_size=10)
	def f(x: int) -> int:
		return x * 2

	for i in range(10):
		f(i)
	# pin the timestamps so this asserts eviction arithmetic, not clock behaviour
	f.conn.execute('UPDATE "f" SET timestamp = "x"')
	f.conn.commit()
	assert len(f.contents()) == 10

	f(100)  # the 11th entry evicts exactly one row: the oldest
	assert len(f.contents()) == 10
	assert f.size == 10
	assert {row[0] for row in f.contents()} == {1, 2, 3, 4, 5, 6, 7, 8, 9, 100}


def test_eviction_is_exact_when_every_timestamp_ties(db):
	"""Timestamps are whole seconds, so a burst written inside one second all
	ties. Deleting by key caps the count at LIMIT, so ties cannot widen the
	delete."""
	@database_cache(db, max_size=10)
	def f(x: int) -> int:
		return x * 2

	for i in range(10):
		f(i)
	f.conn.execute('UPDATE "f" SET timestamp = 1700000000')  # force a total tie
	f.conn.commit()

	f(100)
	assert len(f.contents()) == 10
	assert f.size == 10


def test_eviction_survives_a_real_burst(db):
	"""The same scenario without forcing it: entries written as fast as the loop
	allows share a wall-clock second."""
	@database_cache(db, max_size=10)
	def f(x: int) -> int:
		return x * 2

	for i in range(11):
		f(i)
	assert len({row[-1] for row in f.contents()}) <= 2  # they really did tie
	assert len(f.contents()) == 10


def test_cache_never_exceeds_max_size(db):
	@database_cache(db, max_size=4)
	def f(x: int) -> int:
		return x

	for i in range(10):
		f(i)
		assert len(f.contents()) <= 4
	assert len(f.contents()) == 4
	assert f.size == 4


def test_evict_beyond_table_size_empties_it(db):
	@database_cache(db, max_size=10)
	def f(x: int) -> int:
		return x

	for i in range(5):
		f(i)
	f.evict(500)
	assert f.contents() == []
	assert f.size == 0


def test_timestamp_is_a_whole_second(db):
	"""The column is a small int, not an 8-byte REAL."""
	@database_cache(db, max_age=60)
	def f(x: int) -> int:
		return x

	f(1)
	(stored,) = f.conn.execute('SELECT timestamp FROM "f"').fetchone()
	assert isinstance(stored, int)
	# truncation errs early, never late: stored is never after the real write
	assert stored <= time.time()


def test_every_cache_has_a_timestamp_column(db):
	"""The unification: no max_age/max_size needed for the column to exist."""
	@database_cache(db)
	def f(x: int) -> int:
		return x

	f(1)
	assert f.conn.execute('SELECT timestamp FROM "f"').fetchone() is not None


# --- codec ---------------------------------------------------------------------

def test_make_codec_round_trips_without_a_database():
	@dataclass
	class P:
		a: int
		b: str

	for return_type, value in ((int, 7), (tuple[int, str], (7, 'x')), (P, P(7, 'x'))):
		codec = make_codec(return_type)
		row = codec.encode(value)
		assert len(row) == len(codec.columns)
		assert codec.decode(row) == value


# --- size management ------------------------------------------------------------

def test_max_size_can_shrink_between_runs(db):
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
	@database_cache(db, max_size=10)
	def f(x: int) -> int:
		return x

	for i in range(5):
		f(i)
	f.evict(2)
	f.vacuum()
	assert len(f.contents()) == 3


def test_len(db):
	@database_cache(db)
	def f(x: int) -> int:
		return x

	for i in range(5):
		f(i)
	assert len(f) == 5


# --- SQL identifier quoting ---------------------------------------------------

def test_sql_keyword_as_function_and_parameter_name(db):
	@database_cache(db)
	def add(order: int, group: str) -> int:
		return order

	assert add(1, "a") == 1
	assert add(1, "a") == 1  # cache hit


def test_quote_in_table_name(db):
	"""A caller-supplied name must not be an injection vector."""
	@database_cache(db, name='we"ird name')
	def f(x: int) -> int:
		return x

	assert f(1) == 1
	assert f(1) == 1


def test_quote_in_table_name_with_max_size(db):
	"""The eviction index name is derived from the table name; quote it too."""
	@database_cache(db, name='we"ird name', max_size=2)
	def f(x: int) -> int:
		return x

	for i in range(5):
		f(i)
	assert len(f.contents()) == 2


# --- value round-tripping -----------------------------------------------------

def test_bool_round_trips(db):
	@database_cache(db)
	def is_even(x: int) -> bool:
		return x % 2 == 0

	assert is_even(4) is True
	assert is_even(4) is True  # cache hit
	assert is_even(3) is False
	assert is_even(3) is False


def test_nullable_bool_round_trips(db):
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
	from _future_annotations import Point, build

	make = build(db)
	assert make(3) == Point(3, "3")
	assert make(3) == Point(3, "3")


# --- keyword arguments --------------------------------------------------------

def test_keyword_arguments(db):
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
	with pytest.raises(ValueError, match="variadic"):
		@database_cache(db)
		def f(*xs: int) -> int:
			return sum(xs)


def test_var_keyword_rejected(db):
	with pytest.raises(ValueError, match="variadic"):
		@database_cache(db)
		def f(a: int, **kw: int) -> int:
			return a


def test_reserved_parameter_names_rejected(db):
	"""A parameter named max_age/refresh/cache_only would be silently swallowed
	as a control flag, since **kwargs are forwarded."""
	with pytest.raises(ValueError, match="reserved"):
		@database_cache(db, max_age=60)
		def fetch(url: str, max_age: int) -> str:
			return url


def test_timestamp_parameter_rejected(db):
	"""New in _core2: every table has a timestamp column, so the name is taken."""
	with pytest.raises(ValueError, match="reserved"):
		@database_cache(db)
		def f(timestamp: int) -> int:
			return timestamp


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

	with pytest.raises(ValueError) as exc:
		v2(5)

	# the error names both signatures
	assert "signature has changed" in str(exc.value)
	assert "cached: x INTEGER, return INTEGER, timestamp INTEGER" in str(exc.value)
	assert "wanted: x INTEGER, return$0 INTEGER, return$1 INTEGER, timestamp INTEGER" in str(exc.value)


def test_renamed_parameter_reports_signature_change(db):
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
	@database_cache(db)
	def h(x: int) -> int:
		return x

	class Locked:
		def execute(self, *args, **kwargs):
			raise sqlite3.OperationalError("database is locked")

	h.conn = Locked()
	with pytest.raises(sqlite3.OperationalError, match="locked"):
		h(1)


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


def test_nonpositive_settings_rejected(db):
	with pytest.raises(ValueError, match="max_age must be positive"):
		@database_cache(db, max_age=0)
		def f(x: int) -> int:
			return x

	with pytest.raises(ValueError, match="max_size must be positive"):
		@database_cache(db, max_size=0)
		def g(x: int) -> int:
			return x


# --- cache_only / max_age -----------------------------------------------------

def test_cache_only_raises_on_miss(db):
	@database_cache(db)
	def f(x: int) -> int:
		return x

	with pytest.raises(CacheMiss):
		f(1, cache_only=True)
	f(1)
	assert f(1, cache_only=True) == 1


def test_refresh_with_cache_only_is_rejected(db):
	@database_cache(db)
	def f(x: int) -> int:
		return x

	with pytest.raises(ValueError, match="contradictory"):
		f(1, refresh=True, cache_only=True)


def test_refresh_recomputes(db):
	calls = []

	@database_cache(db)
	def f(x: int) -> int:
		calls.append(x)
		return x

	f(1)
	f(1)
	assert calls == [1]
	f(1, refresh=True)
	assert calls == [1, 1]


def test_explicit_refresh_does_not_grow_size(db):
	@database_cache(db, max_size=10)
	def f(x: int) -> int:
		return x

	f(1)
	assert f.size == 1
	f(1, refresh=True)
	assert f.size == 1
	assert len(f.contents()) == 1


def test_control_flags_are_uniform_across_cache_types(tmp_path):
	"""One class now, but the per-call API must stay independent of the settings."""
	@database_cache(tmp_path / "a.db")
	def simple(x: int) -> int:
		return x

	@database_cache(tmp_path / "b.db", max_age=60)
	def timed(x: int) -> int:
		return x

	for f in (simple, timed):
		with pytest.raises(CacheMiss):
			f(1, cache_only=True)
		assert f(1) == 1
		assert f(1, cache_only=True) == 1
		assert f(1, refresh=True) == 1
		with pytest.raises(ValueError, match="contradictory"):
			f(1, refresh=True, cache_only=True)


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


def test_per_call_max_age_on_an_unlimited_cache(db):
	"""max_age is a per-call flag even when the decorator set no default."""
	calls = []

	@database_cache(db)
	def f(x: int) -> int:
		calls.append(x)
		return x

	f(1)
	f.conn.execute('UPDATE "f" SET timestamp = timestamp - 300')
	f.conn.commit()

	f(1)  # no default limit: still a hit
	assert calls == [1]
	f(1, max_age=10)  # stale under an explicit window
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


def test_serializer_only_rejected_on_a_return_type(db):
	"""A return value is read back on every hit, so a one-way serializer would
	make the cached call return the stored form instead of the real value --
	f(3) == 6 fresh but f(3) == "6" on the hit. Reject it at decoration time."""
	def as_text(v: object) -> str:
		return str(v)

	with pytest.raises(ValueError, match="needs both directions"):
		@database_cache(db)
		def f(n: int) -> Annotated[object, as_text]:
			return n * 2


def test_annotated_on_input(db):
	"""A parameter is never read back, so one direction is all it needs."""
	@database_cache(db)
	def f(v: Annotated[tuple, to_csv]) -> int:
		return sum(v)

	assert f((1, 2, 3)) == 6
	assert f((1, 2, 3)) == 6


def test_serializer_pair_on_input_is_also_fine(db):
	@database_cache(db)
	def f(v: Annotated[tuple, (to_csv, from_csv)]) -> int:
		return sum(v)

	assert f((1, 2, 3)) == 6
	assert f((1, 2, 3)) == 6


def test_annotated_with_too_much_metadata_rejected(db):
	with pytest.raises(ValueError, match="serialize, deserialize"):
		@database_cache(db)
		def f(n: int) -> Annotated[int, to_csv, from_csv]:
			return n


def test_annotated_with_unsized_metadata_rejected(db):
	with pytest.raises(ValueError, match="serialize, deserialize"):
		@database_cache(db)
		def f(n: int) -> Annotated[int, 5]:
			return n


def test_serializer_without_return_type_rejected(db):
	"""On a parameter, where the one-way form is legal, so this reaches the
	return-annotation check rather than tripping the pair check first."""
	with pytest.raises(ValueError, match="must have a return type"):
		@database_cache(db)
		def f(n: Annotated[int, lambda v: str(v)]) -> int:
			return 1


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


def test_wal_is_enabled(db):
	@database_cache(db)
	def f(x: int) -> int:
		return x

	f(1)
	assert f.conn.execute('PRAGMA journal_mode').fetchone()[0] == 'wal'
	assert f(1) == 1  # and reads still work


def test_eviction_actually_uses_the_timestamp_index(db):
	"""The whole point of the index. Naming the key columns in the ORDER BY
	defeats it, and SQLite silently falls back to sorting the whole table."""
	@database_cache(db, max_size=100)
	def f(a: int, b: int) -> int:
		return a + b

	plan = '\n'.join(
		row[-1] for row in f.conn.execute(f'EXPLAIN QUERY PLAN {f.evict_cmd}', (1,)))
	assert 'COVERING INDEX' in plan
	assert 'TEMP B-TREE FOR ORDER BY' not in plan


@pytest.mark.parametrize('name', ['rowid', 'oid', '_rowid_'])
def test_rowid_parameter_rejected(db, name):
	"""A column so named shadows the implicit rowid that eviction deletes by,
	which would silently make evict() delete the wrong rows."""
	namespace = {}
	exec(f"def g({name}: int) -> int:\n\treturn {name}\n", namespace)
	with pytest.raises(ValueError, match='reserved'):
		database_cache(db)(namespace['g'])


def test_eviction_order_is_oldest_first(db):
	@database_cache(db, max_size=3)
	def f(x: int) -> int:
		return x

	for i in range(3):
		f(i)
	f.conn.execute('UPDATE "f" SET timestamp = 1700000000 + "x"')  # 0 oldest
	f.conn.commit()
	f(99)
	assert {row[0] for row in f.contents()} == {1, 2, 99}


def test_per_call_max_age_none_accepts_any_age(db):
	"""max_age=None per call means what it means on the decorator: no limit."""
	calls = []

	@database_cache(db, max_age=60)
	def f(x: int) -> int:
		calls.append(x)
		return x

	f(1)
	f.conn.execute('UPDATE "f" SET timestamp = timestamp - 6000')
	f.conn.commit()
	f(1)                 # stale under the cache's own 60s limit
	assert calls == [1, 1]
	f.conn.execute('UPDATE "f" SET timestamp = timestamp - 6000')
	f.conn.commit()
	f(1, max_age=None)   # explicitly asking for an entry of any age
	assert calls == [1, 1]
