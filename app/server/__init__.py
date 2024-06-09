import asyncio
from typing import Literal, TypeAlias
from app.kv_store import KVStore
from app.server.info import ServerInfo
from app.resp2.encoder import encode_resp2
from app.resp2.parser import Parser
from app.resp2.evaluator import Evaluator

Address: TypeAlias = str
Port: TypeAlias = int

ClientId: TypeAlias = str
ClientTask: TypeAlias = asyncio.Task


class Server:
    def __init__(
        self,
        addr: tuple[Address, Port],
        kv_store: KVStore,
        role: Literal["master", "slave"] = "master",
    ) -> None:
        self._clients: dict[ClientId, ClientTask] = {}
        self._info = ServerInfo(role)
        self._evaluator = Evaluator(kv_store, self._info)
        self._host, self._port = addr

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
        while True:
            data = await reader.read(1024)

            if data == b"":
                break

            parsed_command = Parser(data).parse_statement()
            response = self._evaluator.eval(parsed_command)

            writer.write(encode_resp2(response))
            await writer.drain()

    async def run(self):
        server_socket = await asyncio.start_server(
            self.on_client_connect,
            host=self._host,
            port=self._port,
            reuse_port=True,
            start_serving=False,
        )

        await server_socket.serve_forever()

        server_socket.close()
        await server_socket.wait_closed()