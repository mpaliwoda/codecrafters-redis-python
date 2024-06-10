import asyncio

from app.kv_store import KVStore
from app.server import Server
from app.cli import parse_args


if __name__ == "__main__":
    args = parse_args()

    kv_store = KVStore()

    server = Server(
        addr=args.addr,
        kv_store=kv_store,
        replicaof=args.replicaof,
    )

    asyncio.run(server.run())
