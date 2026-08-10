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
  deleted (`evict_batch` defaults to 20% of `max_size`, minimum 1).

## Per-call flags

`cache`, `cache_only` and `max_age` are reserved keyword arguments; a wrapped
function may not use those parameter names.

```python
fetch(url, max_age=60)       # accept an entry only if it is under a minute old
fetch(url, cache_only=True)  # raise CacheMiss rather than calling through
slow(42, cache=False)        # skip the lookup, recompute and overwrite
```

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

## Cache objects

The decorated function is a `Cache`, with `.clear()`, `.vacuum()`, `.contents()`
and `.close()`.

## Caveats

- Not thread-safe: the SQLite connection is created with the default
  `check_same_thread=True` and belongs to the thread that applied the decorator.
- Methods are not supported — `self` has no annotation.
- Exceptions are not cached.

## Development

```
pip install -e ".[test]"
pytest
```

Tested on CPython 3.10, 3.11 and 3.13.
