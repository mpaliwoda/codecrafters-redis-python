import asyncio
import hashlib
import time

from app.kv_store import KVStore
from app.resp2.evaluator import Evaluator
from app.server import ServerWrapper
from app.cli import RedisArgs, parse_args
from app.server.handshake import handshake
from app.server.info import Address, ServerInfo

from typing import cast


def _gen_id() -> str:
    now = str(time.time()).encode()
    return hashlib.sha256(now).hexdigest()[:40]


async def run_master(args: RedisArgs) -> None:
    meta = ServerInfo(
        role="master",
        addr=args.addr,
        master_addr=args.addr,
        master_replid=_gen_id(),
        master_repl_offset=0,
    )

    kv_store = KVStore()
    eval = Evaluator(kv_store, meta)
    server_wrapper = ServerWrapper(meta.addr, eval)
    await (await server_wrapper.prepare()).serve_forever()


async def run_slave(args: RedisArgs) -> None:
    meta = ServerInfo(
        role="slave",
        addr=args.addr,
        master_addr=cast(Address, args.replicaof),
        master_replid=_gen_id(),
        master_repl_offset=0,
    )

    kv_store = KVStore()
    eval = Evaluator(kv_store, meta)
    server_wrapper = ServerWrapper(meta.addr, eval)
    server = await server_wrapper.prepare()
    master_io = await asyncio.open_connection(meta.master_addr.host, meta.master_addr.port)
    await handshake(master_io, meta)
    await server.serve_forever()



if __name__ == "__main__":
    args = parse_args()
    kv_store = KVStore()

    match args.replicaof:
        case None:
            asyncio.run(run_master(args))
        case Address():
            asyncio.run(run_slave(args))
