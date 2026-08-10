"""SQLite-backed function cache decorator.

The public API is exactly these two names; the implementation lives in _core.
"""

from ._core import database_cache, CacheMiss

__all__ = ['database_cache', 'CacheMiss']
