import argparse
import asyncio

from app.server import run_server

arg_parser = argparse.ArgumentParser("Really sucky redis 🚀")
arg_parser.add_argument("--port", type=int, nargs="?", default=6379, help="Specifies the port at which to run server on",)


if __name__ == "__main__":
    args = arg_parser.parse_args()
    asyncio.run(run_server(host="localhost", port=args.port))
