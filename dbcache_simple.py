import sqlite3
import inspect
import itertools
import functools
import types
import typing
from dataclasses import is_dataclass, astuple, fields
from pathlib import Path
from types import NoneType


def database_cache(file):
	def decorator(func):
		sig = inspect.signature(func)
		table = func.__name__
		columns = []
		for name, param in sig.parameters.items():
			if param.annotation is inspect._empty:
				raise ValueError(f"type of parameter {name} must be given")
			columns.append(f"{name} {_get_type(param.annotation)}")
		try:
			return_type = func.__annotations__['return']
		except KeyError:
			raise ValueError('return type must be given')

		if return_type is tuple:
			raise ValueError('tuple return type must be parameterized')
		if is_dataclass(return_type):
			return_fields = fields(return_type)
			columns.extend(f"return${i} {_get_type(f.type)}" for i, f in enumerate(return_fields))
			return_columns = [f"return${i}" for i in range(len(return_fields))]
			result_concat = _extend_dataclass
			get_return = lambda r: return_type(*r)
		elif typing.get_origin(return_type) is tuple:
			args = typing.get_args(return_type)
			columns.extend(f"return${i} {_get_type(a)}" for i, a in enumerate(args))
			return_columns = [f"return${i}" for i in range(len(args))]
			result_concat = list.extend
			get_return = lambda r: r
		else:
			columns.append(f"return {_get_type(return_type)}")
			return_columns = ['return']
			result_concat = list.append
			get_return = lambda r: r[0]
		
		if not return_columns:
			raise ValueError('return type of empty tuple/dataclass not allowed. Are you just testing me?')

		conn = sqlite3.connect(file)
		conn.execute(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns)}, PRIMARY KEY({', '.join(sig.parameters.keys())}))")
		conn.commit()
		
		selectors = ' AND '.join(f'{name}=?' for name in sig.parameters.keys())
		lookup = f"SELECT {', '.join(return_columns)} FROM {table} WHERE {selectors}"

		insert_placeholders = ', '.join('?' for _ in range(len(sig.parameters) + len(return_columns)))
		insert_columns = ', '.join(itertools.chain(sig.parameters.keys(), return_columns))
		insert = f"INSERT OR REPLACE INTO {table} ({insert_columns}) VALUES ({insert_placeholders})"

		@functools.wraps(func)
		def wrapper(*args, cache=True):
			bound = sig.bind(*args)
			bound.apply_defaults()
			values = list(bound.arguments.values())
			if cache:
				cur = conn.execute(lookup, values)
				return_values = cur.fetchone()
				if return_values is not None:
					return get_return(return_values)

			result = func(*args)
			result_concat(values, result)
			conn.execute(insert, values)
			conn.commit()
			return result

		return wrapper

	return decorator


SQLITE_TYPES = {
	bool: 'INTEGER',
	int: 'INTEGER',
	float: 'REAL',
	str: 'TEXT',
	bytes: 'BLOB',
	bytearray: 'BLOB',
}

def _get_type(tp: type) -> str:
	origin = typing.get_origin(tp)
	if origin is types.UnionType or origin is typing.Union:
		inner = _get_union(tp)
		nullable = True
	else:
		inner = tp
		nullable = False

	try:
		sql_type = SQLITE_TYPES[inner]
	except KeyError:
		raise ValueError(f"unsupported type: {tp}")
	return f"{sql_type} NOT NULL" if not nullable else sql_type


def _get_union(tp: type) -> type:
	args = typing.get_args(tp)
	if len(args) == 2:
		if args[1] is NoneType:
			return args[0]
		if args[0] is NoneType:
			return args[1]
	raise ValueError(f"Union not allowed except to express nullable type: {tp}")


def _extend_dataclass(lst, dc):
	lst.extend(astuple(dc))
	