import functools
import inspect
import itertools
import sqlite3
import sys
import time
import types
import typing
from abc import ABC, abstractmethod
from dataclasses import is_dataclass, astuple, fields
from datetime import timedelta
from pathlib import Path
from types import NoneType


_UNSET = object()


def database_cache(file, name=None, max_age=None, max_size=None, evict_batch=None):
	if max_age is None and max_size is None:
		return lambda func: SimpleCache(func, file, name)
	return lambda func: TimestampCache(func, file, name, max_age, max_size, evict_batch)


class Cache(ABC):

	def __init__(self, func, file, name, *, timestamp=False):
		self.func = func
		self.conn = sqlite3.connect(file)
		self.table = name or func.__name__
		functools.update_wrapper(self, func)
		sig = inspect.signature(func)
		input_columns = [Column(name, param.annotation) for name, param in sig.parameters.items()]
		
		try:
			return_type = func.__annotations__['return']
		except KeyError:
			raise ValueError('return type must be given')

		if return_type is tuple:
			raise ValueError('tuple return type must be parameterized')

		if is_dataclass(return_type):
			output_columns = [Column(f"return${i}", f.type) for i, f in enumerate(fields(return_type))]
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

		lookup_columns = [*output_columns, Column('timestamp', int)] if timestamp else output_columns
		columns = [*input_columns, *lookup_columns]
		self.conn.execute(
			f"CREATE TABLE IF NOT EXISTS {self.table} "
			f"({', '.join(col.definition for col in columns)}, "
			f"PRIMARY KEY({column_names(input_columns)})) WITHOUT ROWID")
		self.conn.commit()
	
		self.lookup_cmd = (
			f"SELECT {column_names(lookup_columns)} "
			f"FROM {self.table} "
			f"WHERE {' AND '.join(f'{col.name}=?' for col in input_columns)}")
		self.insert_cmd = (
			f"INSERT INTO {self.table} ({column_names(columns)}) "
			f"VALUES ({', '.join('?' for _ in columns)}) "
			f"ON CONFLICT({column_names(input_columns)}) "
			f"DO UPDATE SET {', '.join(f"{col.name}=excluded.{col.name}" for col in lookup_columns)}")
		self.signature = sig
		self.input_columns = input_columns
		self.columns = columns
		self.concat = concat
		self.compose = compose

	def serialize_input(self, args):
		bound = self.signature.bind(*args)
		bound.apply_defaults()
		return [col.serialize(val) for col, val in zip(self.input_columns, bound.arguments.values(), strict=True)]
	
	def get_cached(self, values):
		try:
			return self.conn.execute(self.lookup_cmd, values).fetchone()
		except sqlite3.OperationalError as e:
			raise ValueError(f"{self.table} function signature has changed incompatibly") from e
		
	def clear(self):
		self.conn.execute(f"DELETE FROM {self.table}")
		self.conn.commit()

	def vacuum(self):
		self.conn.execute('VACUUM')
		self.conn.commit()

	def contents(self):
		return self.conn.execute(f"SELECT {column_names(self.columns)} FROM {self.table}").fetchall()


class SimpleCache(Cache):
	
	def __call__(self, *args, cache=True, cache_only=False):
		values = self.serialize_input(args)
		if cache:
			cached_output = self.get_cached(values)
			if cached_output is not None:
				return self.compose(cached_output)
			if cache_only:
				raise CacheMiss(*args)

		result = self.func(*args)
		self.concat(values, result)
		self.conn.execute(self.insert_cmd, values)
		self.conn.commit()
		return result


class TimestampCache(Cache):

	def __init__(self, func, file, name, max_age=None, max_size=None, evict_batch=None):
		super().__init__(func, file, name, timestamp=True)
		self.max_age = get_age(max_age)
		if self.max_age < 1:
			raise ValueError(f"{max_age=}")

		self.size = self.conn.execute(f"SELECT COUNT(*) FROM {self.table}").fetchone()[0]
		if not max_size:
			self.max_size = sys.maxsize
			self.evict_batch = None
		else:
			self.max_size = max_size
			self.evict_batch = evict_batch or int(0.2 * max_size)
			# if max_size changed between runs, reduce the size now
			if self.size > max_size:
				self.evict(self.size - max_size)
				self.vacuum()
		
		self.evict_cmd = (
			f"DELETE FROM {self.table} "
			f"WHERE timestamp <= (SELECT timestamp FROM {self.table} "
			"ORDER BY timestamp ASC LIMIT 1 OFFSET ?)")

	def __call__(self, *args, max_age=_UNSET, cache_only=False):
		values = self.serialize_input(args)
		cached_output = self.get_cached(values)
		if cached_output is not None:
			*return_values, timestamp = cached_output
			max_age = self.max_age if max_age is _UNSET else get_age(max_age)
			if time.time() - timestamp <= get_age(max_age):
				return self.compose(return_values)

		if cache_only:
			raise CacheMiss(*args)

		result = self.func(*args)
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
		cur = self.conn.execute(self.evict_cmd, (count,))
		self.size -= cur.rowcount

	def clear(self):
		super().clear()
		self.size = 0

	def contents(self):
		return self.conn.execute(
			f"SELECT {column_names(self.columns)} "
			f"FROM {self.table} "
			"ORDER BY timestamp ASC").fetchall()


class CacheMiss(Exception):
	pass


def column_names(columns):
	return ', '.join(col.name for col in columns)


def identity(x):
	return x


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
		if tp is inspect._empty:
			raise ValueError(f"type of {name} must be given")
		if typing.get_origin(tp) is typing.Annotated:
			_base, meta = typing.get_args(tp)
			if callable(meta):
				self.serialize = meta
				self.deserialize = identity
			elif len(meta) == 2 and all(callable(f) for f in meta):
				self.serialize , self.deserialize = meta
			else:
				raise ValueError(
					f"type annotation must be a serializer function or a tuple of (serializer, deserializer) but got {meta}")
			try:
				tp = self.serialize.__annotations__['return']
			except KeyError:
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

	@property
	def definition(self):
		return f"{self.name} {self.sql_type}{'' if self.nullable else ' NOT NULL'}"
	
	def __repr__(self):
		return f"({self.definition}: >>{self.serialize.__name__}, <<{self.deserialize.__name__})"


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
