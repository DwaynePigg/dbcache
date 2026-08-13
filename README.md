# dbcache

A persistent, SQLite-backed replacement for `functools.lru_cache`. Results survive
process restarts, and each cached function gets its own table with real columns —
so the cache is queryable with ordinary SQL.

```python
from dbcache import database_cache

@database_cache('cache.sqlite')
def slow(user_id: int, verbose: bool = False) -> str:
    ...

slow(42)          # calls through, stores the result
slow(42)          # hits the cache
```

Both the parameters and the return value must be annotated: the annotations
become the table's column types. Parameters form the primary key, the return
value fills one or more `return` columns, and every table also carries a
`timestamp`.

## Expiry and size limits

```python
from datetime import timedelta

@database_cache('cache.sqlite', max_age=timedelta(hours=6), max_size=10_000)
def fetch(url: str) -> bytes:
    ...
```

- `max_age` — seconds, or a `timedelta`. Entries older than this are recomputed.
- `max_size` — maximum rows. This is exact: once the cache is full, each new
  entry evicts the oldest one, so the table never holds more than `max_size`.

Eviction costs 14–22 µs and stays flat as the cache grows, because `max_size`
also creates an index on `timestamp` and the oldest row is found by descending
it rather than by sorting the table. There is deliberately no batching knob: the
write path only runs on a miss, and a miss means you just ran the wrapped
function, which has to cost far more than that or caching it is a pessimization.
Capacity is worth more than the microseconds batching would save.

## Per-call flags

`refresh`, `cache_only` and `max_age` are reserved keyword arguments, as are
`timestamp`, `rowid`, `oid` and `_rowid_`, which would collide with the cache's
own columns. A wrapped function may not use any of those parameter names; it is
rejected at decoration time rather than silently misbehaving.

```python
slow(42, refresh=True)       # ignore any cached entry, recompute and store
slow(42, cache_only=True)    # raise CacheMiss rather than calling through
fetch(url, max_age=60)       # accept an entry only if it is under a minute old
fetch(url, max_age=None)     # accept an entry of any age
```

All three work on every cache, so which you reach for never depends on how the
cache was created.

## Supported types

`bool`, `int`, `float`, `str`, `bytes`, `bytearray`, dataclasses of those, and
`tuple[...]` of those. Return values may be optional (`str | None`); parameters
may not, since they form the primary key and SQL `NULL` never compares equal, so
a `None` argument could never hit.

Anything else needs an explicit serializer via `Annotated`. The serializer must
have a return annotation — that is what determines the column type.

```python
from typing import Annotated

def dump(v: dict) -> str: return json.dumps(v)
def load(s): return json.loads(s)

@database_cache('cache.sqlite')
def config(env: str) -> Annotated[dict, (dump, load)]:
    ...
```

A **return value needs both directions**, since it is read back on every cache
hit; a lone serializer there is rejected. A **parameter** may use a lone
serializer, because it is only ever written to the key columns and compared
against them, never turned back into a Python value:

```python
def normalize(card) -> int:                    # one direction is enough
    return card if isinstance(card, int) else card.tcgpid

@database_cache('cache.sqlite')
def price(card: Annotated[Any, normalize]) -> float:
    ...
```

That is also the escape hatch for an optional parameter: map `None` onto a real
value and the column stays non-null.

Note that a `bytearray` return comes back as `bytes` on a cache hit. Mutating a
cached value never reaches the database anyway, so the immutable type describes
the real contract.

## Storage layout

Tables are ordinary rowid tables. SQLite keeps a row inside its page only while
the record fits in `page_size - 35` — 4061 bytes by default. Past that the tail
spills into a chain of overflow pages, each a whole page however little of it is
used, so crossing the limit by one byte can multiply the file size and shaving a
column to get back under it can halve it again.

Nothing in ordinary use makes any of that visible — a cache that has fallen off
the cliff looks completely normal from the outside. `stats()` shows it:

```
>>> from dbcache import stats
>>> print(stats(get_products))
get_products: 300 rows, 630,784 bytes on disk (2102/row)
  card           mean      3.0b  max       3b
  finish         mean      0.0b  max       0b
  offset         mean      0.0b  max       0b
  return         mean  1,900.0b  max   1,900b
  timestamp      mean      4.0b  max       4b
  headroom       2,147b, on a widest record of 1,914b, before rows start spilling to overflow pages
```

Sizes are what SQLite actually stores, not what the values print as: an integer
takes as many bytes as its magnitude needs, and `0` and `1` have dedicated serial
types that occupy none at all. The per-column breakdown tells you which column to
attack when a record is too wide:

```
render: 300 rows, 1,392,640 bytes on disk (4642/row)
  image_id       mean      1.6b  max       2b
  return         mean  4,200.0b  max   4,200b
  timestamp      mean      4.0b  max       4b
  OVERFLOWING    the widest record is 4,211b against a 4,061b limit, so 150b spill to an overflow page
```

`CacheStats` exposes `.overflowing`, `.rows`, `.file_bytes`, `.max_record`,
`.page_limit` and `.columns` alongside that rendering. None of it is part of
caching — `_stats.py` is a separate module that `_core.py` does not import.

## Journal mode

Every write commits, and under SQLite's stock settings that commit dominates
everything else a cached call does — around 150x the cost of the lookup and the
write together. Caches are therefore opened in WAL mode with
`synchronous=NORMAL`, which makes a cached write roughly 40 µs instead of 6 ms.

The trade is that a power failure can lose the last few entries written, which
for a cache means recomputing them. Two consequences worth knowing:

- While a cache is open you will see `cache.sqlite-wal` and `cache.sqlite-shm`
  beside the database. They are removed on a clean exit, and after a crash the
  next open recovers from them — never delete them by hand. Ignore them in
  version control (`*.db*` or similar covers it).
- WAL needs real shared memory, so it does not work on a network filesystem.
  SQLite silently stays in the old journal mode there; you lose the speedup but
  nothing breaks.

## Cache objects

The decorated function is a `DatabaseCache`, with `.clear()`, `.vacuum()`,
`.contents()`, `.close()` and `len()`.

`.contents()` returns the raw stored rows, oldest first — parameters, then
return columns, then the timestamp — without deserializing them.

You will not normally call `.close()`. Writes are committed as they happen, so
nothing is pending at exit and the interpreter closes the connection for you —
a cache decorated at import and left open for the life of the process is the
intended usage. It matters when something needs the file itself, since deleting
or replacing a cache fails on Windows while a connection is open, and when
caches are created dynamically rather than once at import. It is terminal:
calling the function afterwards raises `ProgrammingError`.

## Caveats

- Every parameter maps to one column, so `*args` and `**kwargs` cannot be cached.
  Keyword-only and positional-only parameters are fine. Call spelling does not
  matter: `f(1, 2)`, `f(1, b=2)` and `f(b=2, a=1)` are bound through the
  signature and share one cache entry.
- Not thread-safe: the SQLite connection is created with the default
  `check_same_thread=True` and belongs to the thread that applied the decorator.
- Two handles on the same table each track their own row count, so `max_size`
  is enforced against a stale number if one cache is opened twice.
- Changing a function's signature or return type invalidates its table. The
  mismatch is reported at the first call, naming both the stored and the wanted
  columns; delete the file to rebuild.
- Methods are not supported — `self` has no annotation.
- Exceptions are not cached.

## Development

```
pip install -e ".[test]"
pytest
```

Requires CPython 3.12 or newer; tested on 3.12 and 3.13.

The package directory sits at the repo root, so `import dbcache` from here resolves
to the working tree and shadows whatever is installed. That is convenient day to
day, but it means the test run above cannot catch a packaging mistake. Before
cutting a release, check the built artifact from somewhere else:

```
python -m build --wheel && pip install dist/*.whl && cd /tmp && pytest path/to/tests
```
