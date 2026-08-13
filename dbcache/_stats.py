"""Reporting on how a cache's rows are actually sitting on disk.

	from dbcache import database_cache, stats

	@database_cache('cache.db')
	def fetch(url: str) -> str:
		...

	print(stats(fetch))

None of this is part of caching: nothing here runs on a lookup or a write,
and _core2 does not import it. It reads a cache's columns and connection from
the outside, which is why it can live in its own file -- the dependency only
points this way.

What it is for: SQLite keeps a row inside its page only while the record fits
in `page_limit` below. Past that the tail spills into a chain of overflow
pages, each a whole page however little of it is used, so crossing the limit
by one byte can multiply the file size and shaving a column to get back under
it can halve the file again. Nothing in ordinary use makes any of that
visible; this does.
"""
from dataclasses import dataclass

# Widest value each integer serial type holds. 0 and 1 have dedicated serial
# types and occupy no bytes at all, and a float is always 8.
INT_BOUNDS = ((127, 1), (32767, 2), (8388607, 3), (2147483647, 4), (140737488355327, 6))


def stats(cache):
	"""Measure one cache: bytes per column, and how near a row is to spilling."""
	page_size = cache.conn.execute('PRAGMA page_size').fetchone()[0]
	page_count = cache.conn.execute('PRAGMA page_count').fetchone()[0]
	sizes = ', '.join(
		f"avg({e}), max({e})" for e in (size_expr(col) for col in cache.columns))
	rows, *measured = cache.conn.execute(
		f"SELECT count(*), {sizes} FROM {cache.qtable}").fetchone()
	columns = [
		(col.name, mean or 0, largest or 0)
		for col, mean, largest in zip(cache.columns, measured[::2], measured[1::2])]
	return CacheStats(
		table=cache.table,
		rows=rows,
		file_bytes=page_size * page_count,
		columns=columns,
		max_record=record_bytes(cache.columns, [largest for _, _, largest in columns]),
		# a row lives in the table b-tree, which gets all of the page but its header
		page_limit=page_size - 35)


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
	"""SQL for the bytes one stored value occupies. A NULL occupies none.

	Measured, not estimated: an integer takes as many bytes as its magnitude
	needs, which is not how many digits it prints -- a timestamp is 4 bytes and
	ten characters. length(cast(x AS BLOB)) rather than octet_length() because
	length() alone counts characters for text, and octet_length() only exists
	from SQLite 3.43 (2023), which requires-python = ">=3.12" does not promise.
	"""
	col = column.quoted
	if column.sql_type in ('TEXT', 'BLOB'):
		return f"coalesce(length(cast({col} AS BLOB)), 0)"
	if column.sql_type == 'REAL':
		return f"(CASE WHEN {col} IS NULL THEN 0 ELSE 8 END)"
	widths = ' '.join(f"WHEN {col} BETWEEN {-bound - 1} AND {bound} THEN {width}"
	                  for bound, width in INT_BOUNDS)
	return f"(CASE WHEN {col} IS NULL OR {col} IN (0, 1) THEN 0 {widths} ELSE 8 END)"


def record_bytes(columns, sizes):
	"""Roughly the bytes one record occupies, its type header included."""
	total = 1  # the header's own length, as a varint
	for column, size in zip(columns, sizes, strict=True):
		# a text or blob serial type encodes the length, so it grows with it;
		# every number fits in a one-byte serial type
		serial = 13 + 2 * size if column.sql_type in ('TEXT', 'BLOB') else 8
		total += (1 if serial < 128 else 2 if serial < 16384 else 3) + size
	return total
