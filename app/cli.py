from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Final

from app.server.info import Address


@dataclass
class RedisArgs:
    addr: Address
    replicaof: Address | None

    @classmethod
    def from_namespace(cls, ns: argparse.Namespace) -> RedisArgs:
        host: Final[str] = "localhost"
        port = int(ns.port) if hasattr(ns, "port") else 6379

        addr = Address(host, port)

        if hasattr(ns, "replicaof") and isinstance(ns.replicaof, str):
            # splitting in order to explicitly panic when args are malformed
            master_host, master_port = ns.replicaof.split(" ")
            replicaof = Address(master_host, int(master_port))
        else:
            replicaof = None

        return RedisArgs(addr, replicaof)


def _arg_parser() -> argparse.ArgumentParser:
    arg_parser = argparse.ArgumentParser("Really sucky redis 🚀")

    arg_parser.add_argument(
        "--port",
        type=int,
        nargs="?",
        default=6379,
        help="Specifies the port at which to run server on",
    )

    arg_parser.add_argument(
        "--replicaof",
        type=str,
        nargs="?",
        default=None,
        help="Specifies the master to replicate",
    )

    return arg_parser


def parse_args() -> RedisArgs:
    parser = _arg_parser()
    args = parser.parse_args()
    return RedisArgs.from_namespace(args)
