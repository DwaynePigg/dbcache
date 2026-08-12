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
become the table's column types.

## Expiry and size limits

Passing `max_age` or `max_size` adds a timestamp column and turns on eviction.

```python
from datetime import timedelta

@database_cache('cache.sqlite', max_age=timedelta(hours=6), max_size=10_000)
def fetch(url: str) -> bytes:
    ...
```

- `max_age` — seconds, or a `timedelta`. Entries older than this are recomputed.
- `max_size` — maximum rows. Once exceeded, the oldest `evict_batch` entries are
  deleted (`evict_batch` defaults to 5% of `max_size`, minimum 1).

### Choosing `evict_batch`

Eviction pays for a scan and sort of the whole table however few rows it actually
removes, so the batch exists to amortise that scan over many inserts. Keeping it
proportional to `max_size` is what holds the per-insert cost flat as a cache
grows — a fixed batch gets steadily worse:

    max_size            5,000    20,000    80,000
    5% batch            5.9 µs     2.4       2.9     flat
    fixed batch of 200  5.5 µs     9.9      45.8     degrades

The batch is also capacity you give up: the cache oscillates between
`max_size - evict_batch` and `max_size`, so a 20% batch averages 90% occupancy
while 5% averages 97.5%. Since the amortised cost is a few microseconds either
way — nothing beside a function expensive enough to be worth caching — the
default favours capacity. Raise it only if profiling says eviction is actually
costing you.

## Per-call flags

`refresh`, `cache_only` and `max_age` are reserved keyword arguments; a wrapped
function may not use those parameter names.

```python
slow(42, refresh=True)       # ignore any cached entry, recompute and store
slow(42, cache_only=True)    # raise CacheMiss rather than calling through
fetch(url, max_age=60)       # accept an entry only if it is under a minute old
```

`refresh` and `cache_only` work on every cache, so which of them you reach for
never depends on how the cache was created. `max_age` is the exception: it needs
a cache created with `max_age` or `max_size`, since only those store a timestamp.

## Supported types

`bool`, `int`, `float`, `str`, `bytes`, `bytearray`, dataclasses of those, and
`tuple[...]` of those. Return values may be optional (`str | None`); parameters
may not, since they form the primary key.

Anything else needs an explicit serializer via `Annotated`, either as a
`(serialize, deserialize)` pair or as a lone `serialize`. The serializer must
have a return annotation — that is what determines the column type.

```python
from typing import Annotated

def dump(v: dict) -> str: return json.dumps(v)
def load(s): return json.loads(s)

@database_cache('cache.sqlite')
def config(env: str) -> Annotated[dict, (dump, load)]:
    ...
```

With a lone serializer, reads return the *stored* representation rather than the
original value.

## Storage layout

Tables are `WITHOUT ROWID` by default, which is the right choice for the small
records a cache usually holds — the row lives directly in the primary-key B-tree,
with no rowid and no separate index.

It stops being the right choice for large values. A `WITHOUT ROWID` row is stored
in an *index* B-tree, which reserves far more of each page for tree structure, so
the largest record that still fits inside a page is only

    ((page_size - 12) * 64 / 255) - 23      # 1002 bytes at the default 4096

against `page_size - 35` (4061) for an ordinary rowid table. Exceed it and every
row spills onto overflow pages, each a whole page however little of it is used —
which can more than double the file. Pass `rowid=True` for those caches:

```python
@database_cache('cache.sqlite', rowid=True)
def fetch(url: str) -> bytes:
    ...
```

A table's rowid-ness cannot be changed in place, so switching the flag on an
existing cache raises rather than silently keeping the old layout. Delete the
file to rebuild.

## Cache objects

The decorated function is a `Cache`, with `.clear()`, `.vacuum()`, `.contents()`
and `.close()`.

You will not normally call `.close()`. Writes are committed as they happen, so
nothing is pending at exit and the interpreter closes the connection for you —
a cache decorated at import and left open for the life of the process is the
intended usage. It matters when something needs the file itself, since deleting
or replacing a cache fails on Windows while a connection is open, and when
caches are created dynamically rather than once at import. It is terminal:
calling the function afterwards raises `ProgrammingError`.

`.stats()` reports how rows are actually sitting on disk, which is otherwise
invisible — a cache that has fallen off the overflow cliff looks completely
normal from the outside:

```
>>> get_products.stats()
cache: 4096 rows, WITHOUT ROWID, page_size=4096
  file        19,177,472 bytes (4,682/row)
  record      max 1919, mean 1918
  maxLocal    1002
  OVERFLOWING -- 917 bytes spill to an overflow page per row
  try         rowid=True or page_size=8192
```

It exposes `.overflowing` and `.headroom` alongside the raw numbers.

## Caveats

- Every parameter maps to one column, so `*args` and `**kwargs` cannot be cached.
  Keyword-only and positional-only parameters are fine. Call spelling does not
  matter: `f(1, 2)`, `f(1, b=2)` and `f(b=2, a=1)` are bound through the
  signature and share one cache entry.
- Not thread-safe: the SQLite connection is created with the default
  `check_same_thread=True` and belongs to the thread that applied the decorator.
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

