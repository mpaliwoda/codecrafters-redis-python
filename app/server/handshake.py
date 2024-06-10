import asyncio
from app.resp2 import obj
from app.resp2.encoder import encode_resp2
from app.server.info import ServerInfo


async def handshake(master_io: tuple[asyncio.StreamReader, asyncio.StreamWriter], meta: ServerInfo) -> None:
    reader, writer = master_io

    async def cmd(command: bytes, expected: bytes) -> None:
        if not isinstance(command, bytes):
            raise RuntimeError(f"kurwa: {command}")

        writer.write(command)
        await writer.drain()

        response = await reader.read(1024)
        if response != expected:
            print(f"{response=}, {expected=}")
            raise RuntimeError("fail")

    await cmd(_ping(), b"+PONG\r\n"),
    await cmd(_replconf_port(meta), b"+OK\r\n"),
    await cmd(_replconf_capa(), b"+OK\r\n"),


def _ping() -> bytes:
    return encode_resp2(obj.Arr(elements=[obj.String("PING", simple=True)]))


def _replconf_port(meta: ServerInfo) -> bytes:
    return encode_resp2(
        obj.Arr(
            elements=[
                obj.String("REPLCONF"),
                obj.String("listening-port"),
                obj.String(f"{meta.addr.port}"),
            ]
        )
    )


def _replconf_capa() -> bytes:
    return encode_resp2(
        obj.Arr(
            elements=[
                obj.String("REPLCONF"),
                obj.String("capa"),
                obj.String("psync2"),
            ]
        )
    )
