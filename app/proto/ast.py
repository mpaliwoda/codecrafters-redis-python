from __future__ import annotations
from dataclasses import dataclass
from typing import Literal, TypeAlias

@dataclass
class String:
    val: str
    tok: Literal["+"] = "+"

    def encode(self) -> bytes:
        return f"{self.tok}{self.val}\r\n".encode()

@dataclass
class Integer:
    val: int
    tok: Literal[":"] = ":"

    def encode(self) -> bytes:
        return f"{self.tok}{self.val}\r\n".encode()

@dataclass
class Err:
    val: str
    tok: Literal["-"] = "-"

    def encode(self) -> bytes:
        return f"{self.tok}{self.val}\r\n".encode()

@dataclass
class Null:
    tok: Literal["$"] | Literal["*"]
    val: Literal[-1] = -1

    def encode(self) -> bytes:
        return f"{self.tok}{self.val}\r\n".encode()

@dataclass
class Bulk:
    val: str
    size: int
    tok: Literal["$"] = "$"

    def encode(self) -> bytes:
        return f"{self.tok}{self.size}\r\n{self.val}\r\n".encode()

@dataclass
class Arr:
    elements: list[Ast]
    size: int
    tok: Literal["*"] = "*"

    def encode(self) -> bytes:
        s = f"{self.tok}{self.size}\r\n".encode()
        el = b"\r\n".join(map(lambda elem: elem.encode(), self.elements))
        return s + el


Ast: TypeAlias = (
     Arr
    | Bulk
    | Err
    | Integer
    | Null
    | String
)
