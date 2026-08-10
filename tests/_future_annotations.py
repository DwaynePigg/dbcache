"""Helper for the `from __future__ import annotations` regression test.

Must live in its own module: the future import is per-module, and it is what
turns every annotation in the file into a plain string.
"""
from __future__ import annotations

from dataclasses import dataclass

from dbcache import database_cache


@dataclass
class Point:
	x: int
	y: str


def build(db):
	@database_cache(db)
	def make(n: int) -> Point:
		return Point(n, str(n))
	return make
