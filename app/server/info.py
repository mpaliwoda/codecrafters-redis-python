from dataclasses import dataclass
from typing import Literal
from textwrap import dedent


@dataclass
class ServerInfo:
    role: Literal["master", "slave"]

    def to_string(self) -> str:
        s = f"""
            role:{self.role}
        """

        return dedent(s).removeprefix("\n").removesuffix("\n")
