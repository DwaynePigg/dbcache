"""SQLite-backed function cache.

	@database_cache('cache.db', max_age=timedelta(days=1))
	def geocode(address: str) -> tuple[float, float]:
		...

The type annotations define the table: one column per parameter, which
together form the primary key, one or more `return` columns for the result,
and a timestamp. Each call looks its arguments up in the table and runs the
function only on a miss.

Second, slimmer draft of _core; see that file for the history of the edge
cases handled here.
"""
import functools
import inspect
import math
import sqlite3
import time
import typing
from dataclasses import astuple, dataclass, fields, is_dataclass
from datetime import timedelta
from types import NoneType, UnionType

_EMPTY = inspect.Parameter.empty

# __call__ forwards **kwargs to the wrapped function, so a parameter sharing a
# name with one of the cache's own keyword arguments would be swallowed as one
# and silently corrupt the cache key. `timestamp` would collide with the
# cache's own column, and a column named `rowid` (or either of its aliases)
# shadows the implicit one that eviction deletes by. Reject all of them at
# decoration time, where the message can say so.
_RESERVED = frozenset({
	'refresh', 'cache_only', 'max_age', 'timestamp', 'rowid', 'oid', '_rowid_'})


def database_cache(file, name=None, max_age=None, max_size=None):
	"""Cache a function's results in an SQLite database, one call per row.

	Every parameter and the return type must be annotated: the annotations
	become the table's columns, and the parameters its primary key, so calling
	again with the same arguments reads the stored result back instead of
	running the function. Supported types are bool, int, float, str and bytes;
	`T | None` marks a return column nullable, and Annotated (see Column) maps
	anything else onto a supported type.

	file: path of the database, shared freely between caches.
	name: table name, defaulting to the function's own.
	max_age: seconds (or a timedelta) after which an entry is stale and gets
		recomputed on its next call. None means entries never expire.
	max_size: most entries kept; the oldest is evicted to make room.

	The wrapped function gains three per-call keyword arguments: refresh=True
	recomputes and overwrites the entry, cache_only=True raises CacheMiss
	instead of ever calling the function, and max_age overrides the cache-wide
	setting for that one call.
	"""
	def decorate(func):
		return DatabaseCache(func, file, name, max_age, max_size)
	return decorate


class CacheMiss(Exception):
	"""Raised by a cache_only call when there is no fresh entry.

	Carries the (positional, keyword) arguments of the rejected call.
	"""


class DatabaseCache:
	"""What database_cache wraps a function in: the table and its statements."""

	def __init__(self, func, file, name=None, max_age=None, max_size=None):
		functools.update_wrapper(self, func)
		self.func = func
		self.table = name or func.__name__
		self.qtable = quote(self.table)
		self.max_age = as_seconds(max_age)
		if self.max_age <= 0:
			raise ValueError(f"max_age must be positive: {max_age!r}")
		self.max_size = math.inf if max_size is None else max_size
		if self.max_size <= 0:
			raise ValueError(f"max_size must be positive: {max_size!r}")

		self.signature = inspect.signature(func)
		reserved = sorted(_RESERVED.intersection(self.signature.parameters))
		if reserved:
			raise ValueError(f"{self.table} has parameter(s) {reserved}, which are reserved by the cache; rename them")
		variadic = [
			p.name for p in self.signature.parameters.values()
			if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD)]
		if variadic:
			raise ValueError(
				f"{self.table} has variadic parameter(s) {variadic}; every parameter must map onto "
				f"one column, so *args and **kwargs cannot be cached")

		# get_type_hints resolves string annotations (`from __future__ import annotations`)
		hints = typing.get_type_hints(func, include_extras=True)
		self.input_columns = [Column(p, hints.get(p, _EMPTY)) for p in self.signature.parameters]
		nullable = [col.name for col in self.input_columns if col.nullable]
		if nullable:
			raise ValueError(
				f"{self.table} has optional parameter(s) {nullable}; parameters form the primary "
				f"key and cannot be nullable, since SQL NULL never compares equal and a None "
				f"argument could never hit. Use Annotated to map None onto a real value instead.")
		if 'return' not in hints:
			raise ValueError(f"{self.table}: return type must be given")
		self.codec = make_codec(hints['return'])
		self.columns = [*self.input_columns, *self.codec.columns, Column('timestamp', int)]

		self.conn = sqlite3.connect(file)
		# Off, a double-quoted name that matches no column is an error. On (the
		# legacy default), SQLite reads it as a string literal, which would turn
		# a missing column into silently wrong data instead of the ValueError
		# raised in fetch().
		self.conn.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DDL, False)
		self.conn.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DML, False)
		# Every call commits, and under the stock settings that commit is by far
		# the most expensive thing a cached call does -- around 150x the cost of
		# the lookup and the write together. These two pragmas trade durability
		# for that: a power failure can cost the last few entries written. For a
		# cache that is the right trade, since a lost entry is only recomputed.
		self.conn.execute('PRAGMA journal_mode = WAL')
		self.conn.execute('PRAGMA synchronous = NORMAL')

		key = column_names(self.input_columns)
		self.conn.execute(
			f"CREATE TABLE IF NOT EXISTS {self.qtable} "
			f"({', '.join(col.definition for col in self.columns)}, PRIMARY KEY ({key}))")
		if max_size is not None:
			# keeps finding the oldest entries cheap as the cache fills
			self.conn.execute(
				f"CREATE INDEX IF NOT EXISTS {quote(self.table + '$by_age')} "
				f"ON {self.qtable} (timestamp)")
		self.conn.commit()

		self.lookup_cmd = (
			f"SELECT {column_names(self.codec.columns)}, timestamp FROM {self.qtable} "
			f"WHERE {' AND '.join(f'{col.quoted} = ?' for col in self.input_columns)}")
		self.store_cmd = (
			f"INSERT OR REPLACE INTO {self.qtable} ({column_names(self.columns)}) "
			f"VALUES ({', '.join('?' for _ in self.columns)})")
		# Deleting by rowid is what lets the index above do its job. Naming the
		# key columns in the ORDER BY instead defeats it -- they are not in the
		# index, so SQLite gives up and sorts the whole table on every single
		# eviction. LIMIT keeps the delete exact however many timestamps tie,
		# and the index's own (timestamp, rowid) order breaks those ties by
		# insertion order, so the oldest entry really does go first.
		self.evict_cmd = (
			f"DELETE FROM {self.qtable} WHERE rowid IN "
			f"(SELECT rowid FROM {self.qtable} ORDER BY timestamp LIMIT ?)")

		self.size = self.conn.execute(f"SELECT COUNT(*) FROM {self.qtable}").fetchone()[0]
		if self.size > self.max_size:  # max_size shrank since the last run
			self.evict(self.size - self.max_size)
			self.vacuum()

	def __call__(self, *args, refresh=False, cache_only=False, max_age=None, **kwargs):
		if refresh and cache_only:
			raise ValueError('refresh=True and cache_only=True are contradictory')

		key = self.encode_key(args, kwargs)
		# runs even under refresh: whether a row already exists is what tells
		# the size accounting below if the write adds a row or replaces one
		cached = self.fetch(key)
		if cached is not None and not refresh:
			*outputs, timestamp = cached
			if time.time() - timestamp <= (self.max_age if max_age is None else as_seconds(max_age)):
				return self.codec.decode(outputs)
		if cache_only:
			raise CacheMiss(args, kwargs)

		result = self.func(*args, **kwargs)
		# whole seconds: never later than the real write time, so an entry can
		# only ever expire a moment early, not late
		self.conn.execute(self.store_cmd, [*key, *self.codec.encode(result), int(time.time())])
		if cached is None:
			self.size += 1
			self.evict(self.size - self.max_size)
		self.conn.commit()
		return result

	def encode_key(self, args, kwargs):
		"""Serialize the arguments of one call, in declared parameter order."""
		bound = self.signature.bind(*args, **kwargs)
		bound.apply_defaults()  # also re-sorts keyword arguments into declaration order
		return [col.serialize(v) for col, v in zip(self.input_columns, bound.arguments.values(), strict=True)]

	def fetch(self, key):
		"""The stored (outputs..., timestamp) row for a key, or None."""
		try:
			return self.conn.execute(self.lookup_cmd, key).fetchone()
		except sqlite3.OperationalError as e:
			# only a missing table or column implies the signature moved; locks,
			# I/O errors and the like pass through untouched
			if not str(e).startswith(('no such table', 'no such column')):
				raise
			cached = self.conn.execute(
				'SELECT name, type FROM pragma_table_info(?)', (self.table,)).fetchall()
			raise ValueError(
				f"{self.table}: the function signature has changed incompatibly\n"
				f"  cached: {', '.join(f'{name} {tp}' for name, tp in cached) or '(table is missing)'}\n"
				f"  wanted: {', '.join(f'{col.name} {col.sql_type}' for col in self.columns)}") from e

	def evict(self, count):
		"""Delete the `count` oldest entries."""
		if count < 1:
			return
		self.size -= self.conn.execute(self.evict_cmd, (count,)).rowcount

	def clear(self):
		"""Empty the cache."""
		self.conn.execute(f"DELETE FROM {self.qtable}")
		self.conn.commit()
		self.size = 0

	def contents(self):
		"""Every stored row, oldest first."""
		return self.conn.execute(
			f"SELECT {column_names(self.columns)} FROM {self.qtable} ORDER BY timestamp").fetchall()

	def stats(self):
		"""How many bytes each column takes up, and how near a row is to spilling.

		SQLite keeps a row inside its page only while the record fits in the
		limit below; past that the tail spills to a chain of overflow pages,
		each a whole page however little of it is used. Crossing the limit by
		one byte can therefore multiply the file size, and shaving a column to
		get back under it can halve the file again -- but nothing in ordinary
		use makes any of that visible. Print this to see it.
		"""
		page_size = self.conn.execute('PRAGMA page_size').fetchone()[0]
		page_count = self.conn.execute('PRAGMA page_count').fetchone()[0]
		# octet_length is exact for text and blobs, which are the columns that
		# vary; a number occupies at most 8 bytes however many digits it prints
		sizes = ', '.join(
			f"avg({e}), max({e})" for e in (size_expr(col) for col in self.columns))
		rows, *measured = self.conn.execute(
			f"SELECT count(*), {sizes} FROM {self.qtable}").fetchone()
		columns = [
			(col.name, mean or 0, largest or 0)
			for col, mean, largest in zip(self.columns, measured[::2], measured[1::2])]
		return CacheStats(
			table=self.table,
			rows=rows,
			file_bytes=page_size * page_count,
			columns=columns,
			max_record=record_bytes(self.columns, [largest for _, _, largest in columns]),
			# a row lives in the table b-tree, which gets all of the page but its header
			page_limit=page_size - 35)

	def vacuum(self):
		"""Give the space freed by deleted entries back to the filesystem."""
		self.conn.commit()  # VACUUM cannot run inside a transaction
		self.conn.execute('VACUUM')

	def close(self):
		self.conn.close()

	def __len__(self):
		return self.size


class Codec:
	"""Converts a return value to one stored value per column and back.

	The value is handled as a sequence of elements: a tuple already is one, a
	dataclass is taken apart into its fields, and a plain value is a 1-tuple.
	"""

	def __init__(self, columns, split=tuple, join=tuple):
		self.columns = columns
		self.split = split
		self.join = join

	def encode(self, value):
		return [col.serialize(v) for col, v in zip(self.columns, self.split(value), strict=True)]

	def decode(self, values):
		elements = [col.deserialize(v) for col, v in zip(self.columns, values, strict=True)]
		return self.join(elements)


def make_codec(return_type):
	"""Pick the columns and conversions for a return annotation.

	The column names contain `$` or the keyword `return`, neither of which can
	appear in a parameter name, so they can never collide with an input column.
	"""
	if return_type is tuple:
		raise ValueError('a tuple return type must be parameterized, e.g. tuple[int, str]')
	if typing.get_origin(return_type) is tuple:
		return Codec([Column(f'return${i}', tp) for i, tp in enumerate(typing.get_args(return_type))])
	if is_dataclass(return_type):
		# resolve the field annotations too; under `from __future__ import
		# annotations` they are strings
		hints = typing.get_type_hints(return_type, include_extras=True)
		return Codec(
			[Column(f'return${i}', hints[f.name]) for i, f in enumerate(fields(return_type))],
			split=astuple, join=lambda elements: return_type(*elements))
	return Codec(
		[Column('return', return_type)],
		split=lambda value: (value,), join=lambda elements: elements[0])


@dataclass(frozen=True)
class CacheStats:
	"""What stats() reports; print it."""

	table: str
	rows: int
	file_bytes: int
	columns: list  # (name, mean bytes, largest bytes)
	max_record: int
	page_limit: int

	@property
	def overflowing(self):
		return self.max_record > self.page_limit

	def __str__(self):
		per_row = f" ({self.file_bytes // self.rows}/row)" if self.rows else ""
		lines = [f"{self.table}: {self.rows:,} rows, {self.file_bytes:,} bytes on disk{per_row}"]
		lines += [f"  {name:<14} mean {mean:>8,.1f}b  max {largest:>7,}b"
		          for name, mean, largest in self.columns]
		if self.overflowing:
			lines.append(
				f"  {'OVERFLOWING':<14} the widest record is {self.max_record:,}b against a "
				f"{self.page_limit:,}b limit, so {self.max_record - self.page_limit:,}b spill "
				f"to an overflow page")
		else:
			lines.append(
				f"  {'headroom':<14} {self.page_limit - self.max_record:,}b, on a widest record "
				f"of {self.max_record:,}b, before rows start spilling to overflow pages")
		return "\n".join(lines)


def size_expr(column):
	"""SQL for the bytes one value of a column occupies. NULL occupies none."""
	if column.sql_type in ('TEXT', 'BLOB'):
		return f"coalesce(octet_length({column.quoted}), 0)"
	return f"min(8, coalesce(octet_length({column.quoted}), 0))"


def record_bytes(columns, sizes):
	"""Roughly the bytes one record occupies, its type header included."""
	total = 1  # the header's own length, as a varint
	for column, size in zip(columns, sizes):
		# a text or blob serial type encodes the length, so it grows with it;
		# every number fits in a one-byte serial type
		serial = 13 + 2 * size if column.sql_type in ('TEXT', 'BLOB') else 8
		total += (1 if serial < 128 else 2 if serial < 16384 else 3) + size
	return total


SQL_TYPES = {
	bool: 'INTEGER',
	int: 'INTEGER',
	float: 'REAL',
	str: 'TEXT',
	bytes: 'BLOB',
	bytearray: 'BLOB',
}


class Column:
	"""One table column: a name, an SQLite type, and how values pass through.

	The type is taken from an annotation. `T | None` makes the column nullable.
	`Annotated[T, serialize]` or `Annotated[T, (serialize, deserialize)]` passes
	values through the given functions; the column then stores whatever the
	serializer's return annotation says.
	"""

	def __init__(self, name, annotation):
		if annotation is _EMPTY:
			raise ValueError(f"type of {name} must be given")
		self.name = name
		self.quoted = quote(name)
		self.serialize = self.deserialize = identity
		if typing.get_origin(annotation) is typing.Annotated:
			_base, *extras = typing.get_args(annotation)
			self.serialize, self.deserialize = read_serializer(name, extras)
			try:
				annotation = typing.get_type_hints(self.serialize)['return']
			except (KeyError, TypeError, AttributeError):
				raise ValueError(f"the serializer for {name} must have a return type") from None
		base, self.nullable = split_optional(annotation)
		try:
			self.sql_type = SQL_TYPES[base]
		except KeyError:
			raise ValueError(f"unsupported type: {annotation}") from None
		if base is bool and self.deserialize is identity:
			self.deserialize = to_bool  # SQLite hands back 0/1, not False/True

	@property
	def definition(self):
		return f"{self.quoted} {self.sql_type}" + ("" if self.nullable else " NOT NULL")


def read_serializer(name, extras):
	"""The (serialize, deserialize) pair from Annotated metadata.

	A lone function only serializes: a cache hit then yields the stored form
	as-is. A pair converts both ways.
	"""
	match extras:
		case [serialize] if callable(serialize):
			return serialize, identity
		case [(serialize, deserialize)] if callable(serialize) and callable(deserialize):
			return serialize, deserialize
	raise ValueError(
		f"the annotation on {name} must be a single serializer function or a "
		f"(serialize, deserialize) pair, got {extras!r}")


def split_optional(annotation):
	"""Split `T | None` into (T, True); no other unions are supported."""
	if typing.get_origin(annotation) not in (UnionType, typing.Union):
		return annotation, False
	rest = set(typing.get_args(annotation)) - {NoneType}
	if len(rest) == 1:
		return rest.pop(), True
	raise ValueError(f"Union not allowed except to express a nullable type: {annotation}")


def as_seconds(age):
	"""A max_age as a number of seconds; None means entries never expire."""
	if age is None:
		return math.inf
	if isinstance(age, timedelta):
		return age.total_seconds()
	return age


def quote(identifier):
	"""Quote an SQL identifier; caller-supplied names must never reach SQL raw."""
	escaped = identifier.replace('"', '""')
	return f'"{escaped}"'


def column_names(columns):
	return ', '.join(col.quoted for col in columns)


def identity(value):
	return value


def to_bool(value):
	return value if value is None else bool(value)  # None must survive for nullable columns
