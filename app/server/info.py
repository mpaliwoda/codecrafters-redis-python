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
    master_replid: str
    master_repl_offset: int

    def to_string(self) -> str:
        s = f"""
            role:{self.role}
            master_replid:{self.master_replid}
            master_repl_offset:{self.master_repl_offset}
        """

        return dedent(s).removeprefix("\n").removesuffix("\n")
