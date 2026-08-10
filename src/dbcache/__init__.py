"""SQLite-backed function cache decorator."""

# The old __init__.py sat at the repo root next to pyproject.toml. Setuptools'
# flat-layout discovery picked up dbcache.py as a top-level *module* and dropped
# the file entirely -- it was in neither the wheel nor SOURCES.txt -- and its
# `from .dbcache import ...` could never have worked anyway, since a file at the
# project root has no parent package. Under src layout it is a real package
# __init__, so this is now the curated public surface it always claimed to be.

from ._core import database_cache, CacheMiss

__all__ = ['database_cache', 'CacheMiss']
