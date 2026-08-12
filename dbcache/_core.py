import functools
import inspect
import sqlite3
import sys
import time
import types
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass, is_dataclass, astuple, fields
from datetime import timedelta
from types import NoneType


_UNSET = object()
_EMPTY = inspect.Parameter.empty

# FIX: __call__ now forwards **kwargs to the wrapped function (keyword arguments
# used to raise TypeError). That makes a parameter sharing a name with one of the
# cache's own flags ambiguous -- it would be swallowed as a flag and silently
# produce a wrong cache key. Reject it loudly at decoration time instead. All
# three names are reserved by both subclasses so that a function does not become
# un-wrappable just by switching between SimpleCache and TimestampCache.
_CONTROL_KEYWORDS = frozenset({'cache', 'cache_only', 'max_age'})


def database_cache(file, name=None, max_age=None, max_size=None, evict_batch=None, rowid=False):
	if max_age is None and max_size is None:
		return lambda func: SimpleCache(func, file, name, rowid=rowid)
	return lambda func: TimestampCache(func, file, name, max_age, max_size, evict_batch, rowid=rowid)


def quote(identifier):
	"""Quote an SQL identifier.

	FIX: table and column names were interpolated into SQL raw, so a function
	named `add` or a parameter named `order` raised OperationalError, and the
	caller-supplied `name` was an injection vector.
	"""
	escaped = identifier.replace('"', '""')
	return f'"{escaped}"'


class Cache(ABC):

	def __init__(self, func, file, name, *, timestamp=False, rowid=False):
		self.func = func
		self.rowid = rowid
		self.conn = sqlite3.connect(file)
		# FIX: SQLite's legacy misfeature reads a double-quoted token that matches
		# no identifier as a string literal instead of failing. Since every
		# identifier here is quoted, that turned "no such column" into silently
		# returning the column *name* as data -- a changed return type would
		# deserialize 'return$0' as a cached value rather than raising. Turning
		# DQS off makes quoted tokens strictly identifiers. (Python 3.12+.)
		self.conn.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DDL, False)
		self.conn.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DML, False)
		self.table = name or func.__name__
		self.qtable = quote(self.table)
		functools.update_wrapper(self, func)
		sig = inspect.signature(func)

		clashing = sorted(_CONTROL_KEYWORDS.intersection(sig.parameters))
		if clashing:
			raise ValueError(f"{self.table} has parameter(s) {clashing} reserved by the cache; rename them")

		# FIX: a row has fixed arity, a variadic parameter does not. One column
		# was reserved for it and the bound tuple/dict then went straight to
		# sqlite3, so these decorated cleanly and failed at call time with an
		# opaque "Error binding parameter N: type 'tuple' is not supported".
		# Reject the shape up front. (Keyword-only and positional-only
		# parameters are fine: both have fixed arity and bind normally.)
		variadic = [
			p.name for p in sig.parameters.values()
			if p.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)]
		if variadic:
			raise ValueError(
				f"{self.table} has variadic parameter(s) {variadic}; every parameter must map to "
				f"one fixed column, so *args and **kwargs cannot be cached")

		# FIX: annotations were read straight off __annotations__, so a caller
		# module using `from __future__ import annotations` (which turns every
		# annotation into a string) failed with "unsupported type: 'int'".
		# get_type_hints resolves them; include_extras keeps Annotated intact.
		hints = typing.get_type_hints(func, include_extras=True)
		input_columns = [Column(name, hints.get(name, _EMPTY)) for name in sig.parameters]

		# FIX: SQLite requires every PRIMARY KEY column of a WITHOUT ROWID table
		# to be NOT NULL, so an optional parameter used to blow up at call time
		# with a bare "NOT NULL constraint failed". Dropping WITHOUT ROWID would
		# not help: SQLite treats each NULL key as distinct, so the row would be
		# re-inserted on every call and never hit. Reject it up front instead.
		nullable_inputs = [col.name for col in input_columns if col.nullable]
		if nullable_inputs:
			raise ValueError(
				f"{self.table} has optional parameter(s) {nullable_inputs}; parameters form the "
				f"primary key and cannot be nullable. Use Annotated to map them onto a non-null "
				f"column if you need it.")

		try:
			return_type = hints['return']
		except KeyError:
			raise ValueError('return type must be given')

		if return_type is tuple:
			raise ValueError('tuple return type must be parameterized')

		if is_dataclass(return_type):
			# FIX: field.type is whatever was written in the class body, which is
			# a string under `from __future__ import annotations`. Resolve the
			# dataclass's hints too, then index them in declared field order.
			field_hints = typing.get_type_hints(return_type, include_extras=True)
			output_columns = [Column(f"return${i}", field_hints[f.name]) for i, f in enumerate(fields(return_type))]
			def concat(row, dc):
				row.extend(col.serialize(val) for col, val in zip(output_columns, astuple(dc)))

			def compose(values):
				return return_type(*(col.deserialize(val) for col, val in zip(output_columns, values)))

		elif typing.get_origin(return_type) is tuple:
			output_columns = [Column(f"return${i}", a) for i, a in enumerate(typing.get_args(return_type))]
			def concat(row, values):
				row.extend(col.serialize(val) for col, val in zip(output_columns, values, strict=True))

			def compose(values):
				return tuple(col.deserialize(val) for col, val in zip(output_columns, values, strict=True))

		else:
			only_return_column = Column(f"return", return_type)
			output_columns = [only_return_column]
			def concat(row, only):
				row.append(only_return_column.serialize(only))

			def compose(values):
				(only,) = values
				return only_return_column.deserialize(only)

		# Whole seconds. Eviction gets its exactness from LIMIT capping the row
		# count (see evict_cmd), not from timestamps being unique, so sub-second
		# resolution would buy nothing and cost 4 bytes a row -- REAL is always 8
		# bytes where an int this size is 4. Truncating also errs the safe way:
		# the stored value is never later than the real write time, so an entry
		# expires up to a second early rather than a second late.
		lookup_columns = [*output_columns, Column('timestamp', int)] if timestamp else output_columns
		columns = [*input_columns, *lookup_columns]

		# CREATE TABLE IF NOT EXISTS silently does nothing when the table already
		# exists, so flipping `rowid` against an existing cache would look like it
		# worked and change nothing. A table's rowid-ness cannot be altered, so
		# say so rather than pretend.
		existing = self.conn.execute(
			"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (self.table,)).fetchone()
		if existing is not None:
			was_rowid = not existing[0].upper().rstrip().endswith('WITHOUT ROWID')
			if was_rowid != rowid:
				raise ValueError(
					f"{self.table} already exists as a {'rowid' if was_rowid else 'WITHOUT ROWID'} table "
					f"but rowid={rowid} was requested; a table's rowid-ness cannot be changed in place, "
					f"so the cache must be rebuilt")

		self.conn.execute(
			f"CREATE TABLE IF NOT EXISTS {self.qtable} "
			f"({", ".join(col.definition for col in columns)}, "
			f"PRIMARY KEY({column_names(input_columns)})){"" if rowid else " WITHOUT ROWID"}")
		self.conn.commit()

		self.lookup_cmd = (
			f"SELECT {column_names(lookup_columns)} "
			f"FROM {self.qtable} "
			f"WHERE {" AND ".join(f"{col.quoted}=?" for col in input_columns)}")
		self.insert_cmd = (
			f"INSERT INTO {self.qtable} ({column_names(columns)}) "
			f"VALUES ({", ".join("?" for _ in columns)}) "
			f"ON CONFLICT({column_names(input_columns)}) "
			f"DO UPDATE SET {", ".join(f"{col.quoted}=excluded.{col.quoted}" for col in lookup_columns)}")
		self.signature = sig
		self.input_columns = input_columns
		self.columns = columns
		self.concat = concat
		self.compose = compose

	@abstractmethod
	def __call__(self, *args, **kwargs):
		"""FIX: Cache subclassed ABC but declared nothing abstract, so it was
		directly instantiable despite having no __call__ of its own."""

	def serialize_input(self, args, kwargs):
		# FIX: kwargs are now bound too. apply_defaults rebuilds `arguments` in
		# declared parameter order, so this stays aligned with input_columns even
		# when the caller passes keywords out of order.
		bound = self.signature.bind(*args, **kwargs)
		bound.apply_defaults()
		return [col.serialize(val) for col, val in zip(self.input_columns, bound.arguments.values(), strict=True)]

	def get_cached(self, values):
		try:
			return self.conn.execute(self.lookup_cmd, values).fetchone()
		except sqlite3.OperationalError as e:
			# FIX: every OperationalError was reported as a signature change,
			# hiding "database is locked", disk I/O errors and the like. Only a
			# missing table or column actually implies the signature moved.
			if str(e).startswith(('no such table', 'no such column')):
				raise ValueError(f"{self.table} function signature has changed incompatibly") from e
			raise

	def clear(self):
		self.conn.execute(f"DELETE FROM {self.qtable}")
		self.conn.commit()

	def vacuum(self):
		# FIX: VACUUM cannot run inside a transaction and any preceding DELETE
		# leaves one open, so committing afterwards raised OperationalError.
		# Commit first so this is safe from every call site.
		self.conn.commit()
		self.conn.execute('VACUUM')

	def contents(self):
		return self.conn.execute(f"SELECT {column_names(self.columns)} FROM {self.qtable}").fetchall()

	def close(self):
		# FIX: the connection was held for the lifetime of the process with no
		# way to release it.
		self.conn.close()

	def stats(self, sample=1000):
		"""Report how the rows are actually sitting on disk.

		SQLite stores a row whole inside a page only while the record fits in
		`maxLocal`; past that the tail spills to a chain of overflow pages, each
		a full page however little of it is used. One byte over the limit can
		therefore multiply the file size, and nothing in ordinary use makes that
		visible -- the cache just quietly gets bigger. This surfaces it.
		"""
		page_size = self.conn.execute('PRAGMA page_size').fetchone()[0]
		page_count = self.conn.execute('PRAGMA page_count').fetchone()[0]
		rows = self.conn.execute(f"SELECT COUNT(*) FROM {self.qtable}").fetchone()[0]
		sampled = self.conn.execute(
			f"SELECT {column_names(self.columns)} FROM {self.qtable} LIMIT {int(sample)}").fetchall()
		sizes = [record_size(row) for row in sampled] or [0]
		return CacheStats(
			table=self.table,
			rowid=self.rowid,
			rows=rows,
			page_size=page_size,
			file_bytes=page_size * page_count,
			max_record=max(sizes),
			mean_record=sum(sizes) // len(sizes),
			max_local=max_local(page_size, self.rowid),
			sampled=len(sampled))


class SimpleCache(Cache):

	def __call__(self, *args, cache=True, cache_only=False, **kwargs):
		# FIX: this combination used to skip the lookup and call the function,
		# the exact opposite of what cache_only asks for.
		if cache_only and not cache:
			raise ValueError('cache_only=True is meaningless with cache=False')

		values = self.serialize_input(args, kwargs)
		if cache:
			cached_output = self.get_cached(values)
			if cached_output is not None:
				return self.compose(cached_output)
			if cache_only:
				raise CacheMiss(args, kwargs)

		result = self.func(*args, **kwargs)
		self.concat(values, result)
		self.conn.execute(self.insert_cmd, values)
		self.conn.commit()
		return result


class TimestampCache(Cache):

	def __init__(self, func, file, name, max_age=None, max_size=None, evict_batch=None, *, rowid=False):
		super().__init__(func, file, name, timestamp=True, rowid=rowid)
		self.max_age = get_age(max_age)
		if self.max_age < 1:
			raise ValueError(f"{max_age=}")

		# FIX: eviction was `DELETE ... WHERE timestamp <= (nth oldest timestamp)`.
		# With whole-second timestamps every entry written in the same second tied,
		# and `<=` matched all of them -- a single eviction emptied the table.
		# (It was also off by one even with distinct timestamps: OFFSET n lands on
		# the n+1'th row and `<=` includes it.)
		#
		# Delete by key instead: the subquery names exactly the `count` oldest rows
		# and LIMIT caps it, so ties cannot widen the delete no matter how many
		# entries share a timestamp. Exactness comes from the query rather than
		# from timestamp resolution, which is what lets the column stay a 4-byte
		# int. The key is the ORDER BY tiebreak so the choice among equal
		# timestamps is at least deterministic.
		pk = column_names(self.input_columns)
		self.evict_cmd = (
			f"DELETE FROM {self.qtable} WHERE ({pk}) IN ("
			f"SELECT {pk} FROM {self.qtable} "
			f"ORDER BY timestamp ASC, {pk} LIMIT ?)")

		self.size = self.conn.execute(f"SELECT COUNT(*) FROM {self.qtable}").fetchone()[0]
		if not max_size:
			self.max_size = sys.maxsize
			self.evict_batch = None
		else:
			self.max_size = max_size
			# FIX: int(0.2 * max_size) is 0 for max_size < 5, so eviction deleted
			# nothing and the cache stayed permanently over its limit.
			self.evict_batch = evict_batch or max(1, int(0.2 * max_size))
			# if max_size changed between runs, reduce the size now
			# FIX: this ran before evict_cmd was assigned, so shrinking max_size
			# between runs raised AttributeError.
			if self.size > max_size:
				self.evict(self.size - max_size)
				self.vacuum()

	def __call__(self, *args, max_age=_UNSET, cache_only=False, **kwargs):
		values = self.serialize_input(args, kwargs)
		cached_output = self.get_cached(values)
		if cached_output is not None:
			*return_values, timestamp = cached_output
			max_age = self.max_age if max_age is _UNSET else get_age(max_age)
			# FIX: get_age was applied a second time to an already-converted value.
			if time.time() - timestamp <= max_age:
				return self.compose(return_values)

		if cache_only:
			raise CacheMiss(args, kwargs)

		result = self.func(*args, **kwargs)
		self.concat(values, result)
		values.append(int(time.time()))
		self.conn.execute(self.insert_cmd, values)
		if cached_output is None:
			self.size += 1
			if self.size > self.max_size:
				self.evict(self.evict_batch)

		self.conn.commit()
		return result

	def evict(self, count):
		# LIMIT handles a count past the end on its own -- the subquery just
		# returns every row -- so only the degenerate case needs guarding.
		if count < 1:
			return
		cur = self.conn.execute(self.evict_cmd, (count,))
		self.size -= cur.rowcount

	def clear(self):
		super().clear()
		self.size = 0

	def contents(self):
		return self.conn.execute(
			f"SELECT {column_names(self.columns)} "
			f"FROM {self.qtable} "
			"ORDER BY timestamp ASC").fetchall()


class CacheMiss(Exception):
	"""Raised by a cache_only call when there is no cached entry.

	FIX: `.args` is now (positional_args, keyword_args) rather than the bare
	positional arguments, since keyword arguments now reach the wrapped function
	and belong in the report.
	"""


def column_names(columns):
	return ', '.join(col.quoted for col in columns)


def max_local(page_size, rowid):
	"""Largest record that still fits inside a page, past which rows overflow.

	A rowid table keeps rows in a table b-tree and gets nearly the whole page.
	A WITHOUT ROWID table keeps them in an *index* b-tree, which reserves far
	more room for tree structure -- roughly a quarter as much usable space.
	Assumes no reserved bytes per page, which is the default.
	"""
	return (page_size - 35) if rowid else ((page_size - 12) * 64 // 255) - 23


def varint_len(n):
	length = 1
	while n >= 128:
		n >>= 7
		length += 1
	return length


def serial_type(value):
	"""(serial type, bytes in the record body) for one stored value."""
	if value is None:
		return 0, 0
	if isinstance(value, int):
		# 0 and 1 have dedicated serial types and occupy no body bytes at all
		if value == 0:
			return 8, 0
		if value == 1:
			return 9, 0
		for bits, tp, width in ((8, 1, 1), (16, 2, 2), (24, 3, 3), (32, 4, 4), (48, 5, 6)):
			if -(1 << (bits - 1)) <= value < (1 << (bits - 1)):
				return tp, width
		return 6, 8
	if isinstance(value, float):
		return 7, 8
	if isinstance(value, (bytes, bytearray)):
		return 12 + 2 * len(value), len(value)
	if isinstance(value, str):
		width = len(value.encode())
		return 13 + 2 * width, width
	raise TypeError(f"not an SQLite storage class: {value!r}")


def record_size(values):
	"""Bytes SQLite uses for one record: header varints plus the body."""
	parts = [serial_type(v) for v in values]
	body = sum(width for _, width in parts)
	header = sum(varint_len(tp) for tp, _ in parts)
	# the header's own length varint counts toward the header, so settle it
	for extra in (1, 2, 3, 4):
		if varint_len(header + extra) == extra:
			header += extra
			break
	return header + body


@dataclass(frozen=True, slots=True)
class CacheStats:
	"""A snapshot of how one cache's rows are sitting on disk."""

	table: str
	rowid: bool
	rows: int
	page_size: int
	file_bytes: int
	max_record: int
	mean_record: int
	max_local: int
	sampled: int

	@property
	def overflowing(self) -> bool:
		return self.max_record > self.max_local

	@property
	def headroom(self) -> int:
		"""Bytes a record may still grow before it starts spilling."""
		return self.max_local - self.max_record

	def __str__(self):
		layout = 'rowid' if self.rowid else 'WITHOUT ROWID'
		note = f" (sampled {self.sampled})" if self.sampled < self.rows else ""
		lines = [
			f"{self.table}: {self.rows} rows, {layout}, page_size={self.page_size}",
			f"  file        {self.file_bytes:,} bytes"
			f"{f' ({self.file_bytes // self.rows:,}/row)' if self.rows else ''}",
			f"  record      max {self.max_record}, mean {self.mean_record}{note}",
			f"  maxLocal    {self.max_local}",
		]
		if self.overflowing:
			spill = self.max_record - self.max_local
			lines.append(f"  OVERFLOWING -- {spill} bytes spill to an overflow page per row")
			bigger = next((p for p in (8192, 16384, 32768, 65536)
			               if max_local(p, self.rowid) >= self.max_record), None)
			fix = ["rowid=True"] if not self.rowid and max_local(self.page_size, True) >= self.max_record else []
			if bigger:
				fix.append(f"page_size={bigger}")
			if fix:
				lines.append(f"  try         {' or '.join(fix)}")
		else:
			lines.append(f"  headroom    {self.headroom} bytes before rows start overflowing")
		return "\n".join(lines)


def identity(x):
	return x


def to_bool(value):
	# None passes through so a nullable bool column round-trips as None, not False.
	return value if value is None else bool(value)


SQLITE_TYPES = {
	bool: 'INTEGER',
	int: 'INTEGER',
	float: 'REAL',
	str: 'TEXT',
	bytes: 'BLOB',
	bytearray: 'BLOB',
}


class Column:

	def __init__(self, name: str, tp: type):
		self.name = name
		self.quoted = quote(name)
		if tp is _EMPTY:
			raise ValueError(f"type of {name} must be given")
		if typing.get_origin(tp) is typing.Annotated:
			# FIX: get_args was unpacked as exactly two values, so Annotated with
			# more than one piece of metadata died on an opaque tuple-unpack
			# error instead of the intended message.
			_base, *extras = typing.get_args(tp)
			if len(extras) != 1:
				raise ValueError(f"expected a single serializer annotation on {name} but got {extras}")
			meta = extras[0]
			if callable(meta):
				self.serialize = meta
				self.deserialize = identity
			# FIX: len(meta) raised TypeError for un-sized metadata (an int, say)
			# instead of falling through to the ValueError below.
			elif isinstance(meta, tuple) and len(meta) == 2 and all(callable(f) for f in meta):
				self.serialize, self.deserialize = meta
			else:
				raise ValueError(
					f"type annotation must be a serializer function or a tuple of (serializer, deserializer) but got {meta}")
			try:
				# FIX: read through get_type_hints for the same reason as above,
				# and tolerate callables that have no __annotations__ at all.
				tp = typing.get_type_hints(self.serialize)['return']
			except (KeyError, TypeError, AttributeError):
				raise ValueError('serializer must have return type')
		else:
			self.serialize = identity
			self.deserialize = identity

		origin = typing.get_origin(tp)
		if origin is types.UnionType or origin is typing.Union:
			self.base_type = unwrap_union(tp)
			self.nullable = True
		else:
			self.base_type = tp
			self.nullable = False

		try:
			self.sql_type = SQLITE_TYPES[self.base_type]
		except KeyError:
			raise ValueError(f"unsupported type: {tp}")

		# FIX: bool stored as INTEGER came back as 0/1, so a cache hit returned
		# int where the fresh call returned bool. Only applied when the caller
		# has not supplied a deserializer of their own.
		if self.base_type is bool and self.deserialize is identity:
			self.deserialize = to_bool

	@property
	def definition(self):
		return f"{self.quoted} {self.sql_type}{"" if self.nullable else " NOT NULL"}"

	def __repr__(self):
		# FIX: __name__ is missing on partials and callable objects.
		def label(f):
			return getattr(f, '__name__', repr(f))
		return f"({self.definition}: >>{label(self.serialize)}, <<{label(self.deserialize)})"


def unwrap_union(tp: type) -> type:
	args = typing.get_args(tp)
	if len(args) == 2:
		if args[1] is NoneType:
			return args[0]
		if args[0] is NoneType:
			return args[1]
	raise ValueError(f"Union not allowed except to express nullable type: {tp}")


def get_age(age):
	if age is None:
		return sys.maxsize
	if isinstance(age, timedelta):
		return age.total_seconds()
	return age
