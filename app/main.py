import argparse
import asyncio

from app.kv_store import KVStore
from app.server import Server

arg_parser = argparse.ArgumentParser("Really sucky redis 🚀")
arg_parser.add_argument(
    "--port",
    type=int,
    nargs="?",
    default=6379,
    help="Specifies the port at which to run server on",
)


if __name__ == "__main__":
    args = arg_parser.parse_args()

    kv_store = KVStore()
    server = Server(addr=("localhost", args.port), kv_store=kv_store)

    asyncio.run(server.run())
