from __future__ import annotations
from dataclasses import dataclass
from typing import TypeAlias


@dataclass(frozen=True)
class String:
    val: str


@dataclass(frozen=True)
class Integer:
    val: int


@dataclass(frozen=True)
class Err:
    val: str


@dataclass(frozen=True)
class Null:
    pass


@dataclass(frozen=True)
class Arr:
    elements: list[Obj]


Obj: TypeAlias = Arr | Err | Integer | Null | String
