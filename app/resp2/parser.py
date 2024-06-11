import asyncio
from typing import Final
from app import die
from app.resp2 import obj


class Token:
    ARRAY = ord("*")
    BULK = ord("$")
    ERROR = ord("-")
    INTEGER = ord(":")
    STRING = ord("+")


CRLF_OFFSET: Final[int] = -len(b"\r\n")


class Parser:
    def __init__(self, reader: asyncio.StreamReader) -> None:
        self._reader = reader
        self._chunk = b""

    def reset(self) -> None:
        self._chunk = b""

    async def parse_statement(self) -> obj.Obj:
        await self.read_chunk()
        return await self._parse()

    async def _parse(self) -> obj.Obj:
        match self.peek_first_char():
            case Token.BULK:
                return await self.read_bulk()
            case Token.STRING:
                return self.read_string()
            case Token.ERROR:
                return self.read_err()
            case Token.INTEGER:
                return self.read_int()
            case Token.ARRAY:
                return await self.read_arr()
            case tok:
                die(f"failed to parse message, got token: {tok}")

    def peek_first_char(self) -> int:
        return self._chunk[0]

    async def read_chunk(self) -> None:
        self._chunk = await self._reader.readuntil(b"\r\n")

    async def read_bulk(self) -> obj.String:
        size = int(self._chunk[1:CRLF_OFFSET].decode())
        await self.read_chunk()
        return obj.String(self._chunk[:size].decode())

    def read_string(self) -> obj.String:
        raw_val = self._chunk[1:CRLF_OFFSET]
        return obj.String(raw_val.decode())

    def read_int(self) -> obj.Integer:
        raw_val = self._chunk[1:CRLF_OFFSET]
        return obj.Integer(int(raw_val.decode()))

    def read_err(self) -> obj.Err:
        raw_val = self._chunk[1:CRLF_OFFSET]
        return obj.Err(raw_val.decode())

    async def read_arr(self) -> obj.Arr:
        size = int(self._chunk[1:CRLF_OFFSET].decode())
        elements = []
        for _ in range(size):
            await self.read_chunk()
            element = await self._parse()
            elements.append(element)
        return obj.Arr(elements)
