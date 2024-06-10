from dataclasses import dataclass
from typing import Literal
from textwrap import dedent


@dataclass
class Address:
    host: str
    port: int


@dataclass
class ServerInfo:
    role: Literal["master", "slave"]
    addr: Address
    master_addr: Address

    def to_string(self) -> str:
        s = f"""
            role:{self.role}
        """

        return dedent(s).removeprefix("\n").removesuffix("\n")
