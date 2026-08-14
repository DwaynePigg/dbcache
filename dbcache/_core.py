"""SQLite-backed function cache.

	@database_cache('cache.db', max_age=timedelta(days=1))
	def geocode(address: str) -> tuple[float, float]:
		...

The type annotations define the table: one column per parameter, which
together form the primary key, one or more `return` columns for the result,
and a timestamp. Each call looks its arguments up in the table and runs the
function only on a miss.
"""
import functools
import inspect
import math
import sqlite3
import time
import typing
from dataclasses import astuple, fields, is_dataclass
from datetime import timedelta
from types import NoneType, UnionType


_EMPTY = inspect.Parameter.empty
_UNSET = object()
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
	setting for that one call -- including max_age=None to accept an entry of
	any age from a cache that normally expires them.
	"""
	def decorate(func):
		return DatabaseCache(func, file, name, max_age, max_size)
	return decorate


class CacheMiss(Exception):
	"""Raised by a cache_only call when there is no fresh entry.

	Carries the (positional, keyword) arguments of the rejected call.
	"""


class SignatureChanged(ValueError):
	"""The table on disk no longer has the columns the function needs.

	`found` and `expected` are both [(column name, SQL type), ...] -- what the
	table has, and what the function wants. Catch this to rebuild a stale
	cache, and read the two lists rather than the message, which is free to
	change. Empty `found` means the table is not there at all.
	"""

	def __init__(self, table, found, expected):
		self.table = table
		self.found = found
		self.expected = expected
		super().__init__(
			f"{table}: the function signature has changed incompatibly\n"
			f"  found:    {describe(found) or '(table is missing)'}\n"
			f"  expected: {describe(expected)}")


def describe(columns):
	return ', '.join(f'{name} {sql_type}' for name, sql_type in columns)


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
		reserved = _RESERVED.intersection(self.signature.parameters)
		if reserved:
			raise ValueError(f"{self.table} has parameter(s) {sorted(reserved)}, which are reserved by the cache; rename them")
		variadic = [
			p.name for p in self.signature.parameters.values()
			if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD)]
		if variadic:
			raise ValueError(
				f"{self.table} has variadic parameter(s) {variadic}; every parameter must map onto "
				f"one column, so *args and **kwargs cannot be cached")

		hints = typing.get_type_hints(func, include_extras=True)
		# read_back=False: a parameter is written to the key columns and compared
		# against them, but never turned back into a Python value, so a one-way
		# serializer is all it needs
		self.input_columns = [
			Column(p, hints.get(p, _EMPTY), read_back=False) for p in self.signature.parameters]
		nullable = [col.name for col in self.input_columns if col.nullable]
		if nullable:
			raise ValueError(
				f"{self.table} has optional parameter(s) {nullable}; parameters form the primary "
				f"key and cannot be nullable, since SQL NULL never compares equal and a None "
				f"argument could never hit. Use Annotated to map None onto a real value instead.")
		# the membership test rather than try/except: make_codec looks up
		# hints[f.name] per dataclass field, and catching around the call turns
		# any KeyError from in there into a wrong "return type must be given"
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
		self.evict_cmd = (
			f"DELETE FROM {self.qtable} WHERE rowid IN "
			f"(SELECT rowid FROM {self.qtable} ORDER BY timestamp LIMIT ?)")

		self.size = self.conn.execute(f"SELECT COUNT(*) FROM {self.qtable}").fetchone()[0]
		if self.size > self.max_size:
			self.evict(self.size - self.max_size)
			self.vacuum()

	def __call__(self, *args, refresh=False, cache_only=False, max_age=_UNSET, **kwargs):
		if refresh and cache_only:
			raise ValueError('refresh=True and cache_only=True are contradictory')

		bound = self.signature.bind(*args, **kwargs)
		bound.apply_defaults()
		key = [col.serialize(v) for col, v in zip(self.input_columns, bound.arguments.values(), strict=True)]
		cached = self._fetch(key)
		if cached is not None and not refresh:
			*outputs, timestamp = cached
			limit = self.max_age if max_age is _UNSET else as_seconds(max_age)
			if time.time() - timestamp <= limit:
				return self.codec.decode(outputs)
		if cache_only:
			raise CacheMiss(args, kwargs)

		result = self.func(*args, **kwargs)
		self.conn.execute(self.store_cmd, [*key, *self.codec.encode(result), int(time.time())])
		if cached is None:
			self.size += 1
			self.evict(self.size - self.max_size)
		self.conn.commit()
		return result

	def _fetch(self, key):
		try:
			return self.conn.execute(self.lookup_cmd, key).fetchone()
		except sqlite3.OperationalError as e:
			# only a missing table or column implies the signature moved; locks,
			# I/O errors and the like pass through untouched
			if not str(e).startswith(('no such table', 'no such column')):
				raise
			found = self.conn.execute(
				'SELECT name, type FROM pragma_table_info(?)', (self.table,)).fetchall()
			raise SignatureChanged(
				self.table, found,
				[(col.name, col.sql_type) for col in self.columns]) from e

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
		return self.join([col.deserialize(v) for col, v in zip(self.columns, values, strict=True)])


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
		hints = typing.get_type_hints(return_type, include_extras=True)
		return Codec(
			# f.type isn't good enough if future annotations are used
			[Column(f'return${i}', hints[f.name]) for i, f in enumerate(fields(return_type))],
			split=astuple, join=lambda elements: return_type(*elements))
	return Codec(
		[Column('return', return_type)],
		split=lambda value: (value,), join=lambda elements: elements[0])


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
	`Annotated[T, (serialize, deserialize)]` passes values through the given
	functions; the column then stores whatever the serializer's return
	annotation says. `Annotated[T, serialize]` gives only the one direction,
	which is allowed on a column that is never read back -- pass read_back=False
	to say so.
	"""

	def __init__(self, name, annotation, *, read_back=True):
		if annotation is _EMPTY:
			raise ValueError(f"type of {name} must be given")
		self.name = name
		self.quoted = quote(name)
		self.serialize = self.deserialize = identity
		if typing.get_origin(annotation) is typing.Annotated:
			_base, *extras = typing.get_args(annotation)
			self.serialize, self.deserialize = read_serializer(name, extras, read_back)
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
			self.deserialize = to_bool

	@property
	def definition(self):
		return f"{self.quoted} {self.sql_type}" + ("" if self.nullable else " NOT NULL")


def read_serializer(name, extras, read_back):
	"""The (serialize, deserialize) pair from Annotated metadata.

	A pair converts both ways. A lone function converts only on the way in,
	which is enough for a column that is never read back, and wrong for one
	that is -- a cache hit would hand the caller the stored form instead of
	the value the function actually returned.
	"""
	match extras:
		case [(serialize, deserialize)] if callable(serialize) and callable(deserialize):
			return serialize, deserialize
		case [serialize] if callable(serialize):
			if not read_back:
				return serialize, identity
			raise ValueError(
				f"the annotation on {name} needs both directions: this value is read back "
				f"on a cache hit, so a lone serializer would return the stored form rather "
				f"than the real value. Give a (serialize, deserialize) pair.")
	raise ValueError(
		f"the annotation on {name} must be a (serialize, deserialize) pair"
		f"{'' if read_back else ' or a single serializer function'}, got {extras!r}")


def split_optional(annotation):
	if typing.get_origin(annotation) not in (UnionType, typing.Union):
		return annotation, False
	rest = set(typing.get_args(annotation)) - {NoneType}
	if len(rest) == 1:
		return rest.pop(), True
	raise ValueError(f"Union not allowed except to express a nullable type: {annotation}")


def as_seconds(age):
	if age is None:
		return math.inf
	if isinstance(age, timedelta):
		return age.total_seconds()
	return age


def quote(identifier):
	"""Quote an SQL identifier; caller-supplied names must never reach SQL raw."""
	return f'"{identifier.replace('"', '""')}"'


def column_names(columns):
	return ', '.join(col.quoted for col in columns)


def identity(value):
	return value


def to_bool(value):
	return None if value is None else bool(value)
