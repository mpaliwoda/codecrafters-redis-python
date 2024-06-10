from __future__ import annotations
import asyncio
from typing import TypeAlias, TYPE_CHECKING
from app.server.info import Address
from app.resp2.encoder import encode_resp2
from app.resp2.parser import Parser
if TYPE_CHECKING:
    from app.resp2.evaluator import Evaluator

ClientId: TypeAlias = str
ClientTask: TypeAlias = asyncio.Task


class ServerWrapper:
    def __init__(
        self,
        addr: Address,
        evaluator: Evaluator,
    ) -> None:
        self._clients: dict[ClientId, ClientTask] = {}
        self._addr = addr
        self._evaluator = evaluator

    async def on_client_connect(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        client_id = writer.get_extra_info("peername")

        def client_cleanup(future: asyncio.Future):
            try:
                future.result()
            except Exception as e:
                print(e)
            finally:
                del self._clients[client_id]

        client_task = asyncio.ensure_future(self.listen_task(reader, writer))
        client_task.add_done_callback(client_cleanup)

        self._clients[client_id] = client_task

    async def listen_task(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        parser = Parser(reader)

        while True:
            parsed_command = await parser.parse_statement()
            response = self._evaluator.eval(parsed_command)

            if response:
                writer.write(encode_resp2(response))
                await writer.drain()

    async def prepare(self):
        return await asyncio.start_server(
            self.on_client_connect,
            host=self._addr.host,
            port=self._addr.port,
            reuse_port=True,
            start_serving=False,
        )
