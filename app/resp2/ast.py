from __future__ import annotations
from dataclasses import dataclass
from typing import Literal, TypeAlias

@dataclass
class String:
    val: str

    def encode(self) -> bytes:
        return f"${len(self.val)}\r\n{self.val}\r\n".encode()

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
    val: Literal[-1] = -1

    def encode(self) -> bytes:
        return f"${self.val}\r\n".encode()

@dataclass
class Arr:
    elements: list[Ast]
    tok: Literal["*"] = "*"

    def encode(self) -> bytes:
        s = f"{self.tok}{len(self.elements)}\r\n".encode()
        el = b"\r\n".join(map(lambda elem: elem.encode(), self.elements))
        return s + el


Ast: TypeAlias = (
     Arr
    | Err
    | Integer
    | Null
    | String
)
