"""Reporting on how a cache's rows are actually sitting on disk.

	from dbcache import database_cache, stats

	@database_cache('cache.db')
	def fetch(url: str) -> str:
		...

	print(stats(fetch))

None of this is part of caching: nothing here runs on a lookup or a write,
and _core does not import it. It reads a cache's columns and connection from
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
	# a row lives in the table b-tree, which gets all of the page but its header
	page_limit = page_size - 35

	# Every figure is measured per row and then aggregated, rather than summing
	# each column's maximum -- those maxima can come from different rows, so
	# their sum is an upper bound that may describe no record that exists.
	alias = rowid_alias(cache)
	record = record_expr(cache.columns, alias)
	per_column = ', '.join(
		f"avg({e}), max({e})" for e in (size_expr(c, alias) for c in cache.columns))
	rows, mean_record, max_record, spilling, max_rowid, *measured = cache.conn.execute(
		f"SELECT count(*), avg({record}), max({record}), sum({record} > {page_limit}), "
		f"max(rowid), {per_column} FROM {cache.qtable}").fetchone()

	return CacheStats(
		table=cache.table,
		rows=rows,
		file_bytes=page_size * page_count,
		columns=[(col.name, mean or 0, largest or 0)
		         for col, mean, largest in zip(cache.columns, measured[::2], measured[1::2])],
		mean_record=mean_record or 0,
		max_record=max_record or 0,
		spilling=spilling or 0,
		max_rowid=max_rowid or 0,
		page_size=page_size)


@dataclass(frozen=True)
class CacheStats:
	"""What stats() reports; print it.

	There are two thresholds, and for most caches the nearer one is `per page`.
	A row costs a whole page's worth of space the moment one fewer of them fits
	in a page, which happens long before it is big enough to overflow.
	"""

	table: str
	rows: int
	file_bytes: int
	columns: list  # (name, mean bytes, largest bytes)
	mean_record: float
	max_record: int
	spilling: int  # rows whose record does not fit inside a page
	max_rowid: int
	page_size: int

	@property
	def page_limit(self):
		"""Largest record that still fits inside a page rather than overflowing."""
		return self.page_size - 35

	@property
	def overflowing(self):
		return self.spilling > 0

	@property
	def headroom(self):
		"""Bytes the widest row may still grow before it starts spilling."""
		return self.page_limit - self.max_record

	@property
	def rows_per_page(self):
		"""How many rows of the widest size share one page."""
		if self.overflowing or not self.max_record:
			return 1
		return max(1, (self.page_size - PAGE_HEADER) // self._cell_bytes(self.max_record))

	@property
	def room_before_fewer_per_page(self):
		"""Bytes the widest record may grow before one fewer row fits per page.

		None once only one row fits, where the next threshold is overflow.
		"""
		if self.rows_per_page <= 1:
			return None
		widest = ((self.page_size - PAGE_HEADER) // self.rows_per_page
		          - (self._cell_bytes(self.max_record) - self.max_record))
		return widest - self.max_record

	def _cell_bytes(self, record):
		# a cell carries its payload length and its rowid as varints, and the
		# page keeps a two-byte pointer to it
		return record + varint_width(record) + varint_width(self.max_rowid) + CELL_POINTER

	def __str__(self):
		per_row = f" ({self.file_bytes // self.rows:,}/row)" if self.rows else ""
		lines = [f"{self.table}: {self.rows:,} rows, {self.file_bytes:,} bytes on disk{per_row}"]
		lines += [f"  {name:<14} mean {mean:>9,.1f}b  max {largest:>8,}b"
		          for name, mean, largest in self.columns]
		lines.append(f"  {'record':<14} mean {self.mean_record:>9,.1f}b  max {self.max_record:>8,}b")
		room = self.room_before_fewer_per_page
		lines.append(
			f"  {'per page':<14} {self.rows_per_page} row(s)"
			+ (f"; {room:,}b of growth before only {self.rows_per_page - 1} fit" if room is not None
			   else ""))
		if self.overflowing:
			lines.append(
				f"  {'OVERFLOWING':<14} {self.spilling:,} of {self.rows:,} rows spill onto an "
				f"overflow page, the widest by {-self.headroom:,}b")
		else:
			lines.append(
				f"  {'headroom':<14} {self.headroom:,}b before the widest row spills onto an "
				f"overflow page")
		return "\n".join(lines)


PAGE_HEADER = 8  # a table-leaf page header; interior pages use 12
CELL_POINTER = 2


def varint_width(n):
	"""Bytes SQLite uses to encode n as a varint."""
	width = 1
	while n >= 128:
		n >>= 7
		width += 1
	return width


def rowid_alias(cache):
	"""The column SQLite keeps as the rowid instead of in the record, if any.

	When a rowid table's primary key is exactly one INTEGER column, that column
	becomes an alias for the rowid: it is stored once, in the cell's rowid
	varint, and occupies no bytes at all inside the record. Counting it as a
	stored value overstates every record in the table.
	"""
	inputs = cache.input_columns
	if len(inputs) == 1 and inputs[0].sql_type == 'INTEGER':
		return inputs[0]
	return None


def size_expr(column, alias=None):
	"""SQL for the bytes one stored value occupies. A NULL occupies none.

	Measured, not estimated: an integer takes as many bytes as its magnitude
	needs, which is not how many digits it prints -- a timestamp is 4 bytes and
	ten characters. length(cast(x AS BLOB)) rather than octet_length() because
	length() alone counts characters for text, and octet_length() only exists
	from SQLite 3.43 (2023), which requires-python = ">=3.12" does not promise.
	"""
	if column is alias:
		return '0'  # kept as the row's rowid, not inside the record
	col = column.quoted
	if column.sql_type in ('TEXT', 'BLOB'):
		return f"coalesce(length(cast({col} AS BLOB)), 0)"
	if column.sql_type == 'REAL':
		return f"(CASE WHEN {col} IS NULL THEN 0 ELSE 8 END)"
	widths = ' '.join(f"WHEN {col} BETWEEN {-bound - 1} AND {bound} THEN {width}"
	                  for bound, width in INT_BOUNDS)
	return f"(CASE WHEN {col} IS NULL OR {col} IN (0, 1) THEN 0 {widths} ELSE 8 END)"


def record_expr(columns, alias=None):
	"""SQL for the total bytes one record occupies, its type header included.

	A record is a header of one varint per column, itself preceded by its own
	length, and then the values. A number always fits a one-byte serial type;
	a text or blob serial type encodes the length, so its varint widens as the
	value grows -- at 58 bytes and again at 8,186. A rowid alias still takes
	its header byte, as serial type 0, but contributes no value bytes.
	"""
	parts = ['1']  # the header's own length
	for column in columns:
		size = size_expr(column, alias)
		if column is not alias and column.sql_type in ('TEXT', 'BLOB'):
			parts.append(f"(CASE WHEN {size} <= 57 THEN 1 WHEN {size} <= 8185 THEN 2 ELSE 3 END)")
		else:
			parts.append('1')
		parts.append(size)
	return ' + '.join(parts)
