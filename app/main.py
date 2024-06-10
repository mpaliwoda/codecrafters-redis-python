import asyncio
import hashlib
import time

from app.kv_store import KVStore
from app.resp2 import obj
from app.resp2.encoder import encode_resp2
from app.resp2.evaluator import Evaluator
from app.server import Server
from app.cli import RedisArgs, parse_args
from app.server.info import Address, ServerInfo
from app.server.tcp_client import TcpClient

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
    await Server(meta.addr, eval).run()


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
    master_client = await TcpClient.create(meta.master_addr)
    await send_handshake(master_client)
    await Server(meta.addr, eval).run()


async def send_handshake(master_client: TcpClient) -> None:
    command = obj.Arr(elements=[obj.String("PING")])
    await master_client.send(encode_resp2(command))


if __name__ == "__main__":
    args = parse_args()
    kv_store = KVStore()

    match args.replicaof:
        case None:
            asyncio.run(run_master(args))
        case Address():
            asyncio.run(run_slave(args))
