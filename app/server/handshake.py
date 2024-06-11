import asyncio
from app import assert_that
from app.resp2 import obj
from app.resp2.encoder import encode_resp2
from app.server.info import ServerInfo


async def handshake(master_io: tuple[asyncio.StreamReader, asyncio.StreamWriter], meta: ServerInfo) -> None:
    reader, writer = master_io

    async def cmd(command: bytes, expected: bytes | None) -> None:
        assert_that(
            lambda: isinstance(command, bytes),
            f"Expected command to be of type: 'bytes', got: {type(command)}",
        )

        writer.write(command)
        await writer.drain()

        response = await reader.read(1024)
        if expected:
            assert_that(
                lambda: response == expected,
                f"expected response: {expected!r}, got: {response!r}",
            )

    await cmd(_ping(), b"+PONG\r\n")
    await cmd(_replconf_port(meta), b"+OK\r\n")
    await cmd(_replconf_capa(), b"+OK\r\n")
    await cmd(_psync(meta), None)


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


def _psync(meta: ServerInfo) -> bytes:
    assert_that(
        lambda: meta.master_replid == "?",
        message=f"Invalid master_replid, expected: '?', got: '{meta.master_replid}'",
    )
    assert_that(
        lambda: meta.master_repl_offset == -1,
        f"Invalid master_repl_offset, expected: -1, got: {meta.master_repl_offset}",
    )

    return encode_resp2(
        obj.Arr(
            elements=[
                obj.String("PSYNC"),
                obj.String(meta.master_replid),
                obj.String(str(meta.master_repl_offset)),
            ]
        )
    )
