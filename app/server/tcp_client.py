from __future__ import annotations
import asyncio

from app.server.info import Address


class TcpClient:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer

    @classmethod
    async def create(cls, addr: Address) -> TcpClient:
        reader, writer = await asyncio.open_connection(addr.host, addr.port)
        return TcpClient(reader, writer)

    async def send(self, message: bytes) -> None:
        self._writer.write(message)
        await self._writer.drain()

    async def read(self) -> bytes:
        return await self._reader.read()
