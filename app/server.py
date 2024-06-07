import asyncio
from typing import TypeAlias
from app.kv_store import KVStore
from app.resp2.encoder import encode_resp2
from app.resp2.parser import Parser
from app.resp2.evaluator import Evaluator

Address: TypeAlias = str
Port: TypeAlias = int

ClientId: TypeAlias = str
ClientTask: TypeAlias = asyncio.Task

_clients: dict[ClientId, ClientTask] = {}
_kv_store = KVStore()


async def on_client_connect(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    client_id = writer.get_extra_info("peername")

    def client_cleanup(future: asyncio.Future):
        try:
            future.result()
        except Exception as e:
            print(e)
        finally:
            del _clients[client_id]

    client_task = asyncio.ensure_future(listen_task(reader, writer))
    client_task.add_done_callback(client_cleanup)

    _clients[client_id] = client_task


async def listen_task(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    evaluator = Evaluator(_kv_store)
    while True:
        data = await reader.read(1024)

        if data == b"":
            break

        parsed_command = Parser(data).parse_statement()
        response = evaluator.eval(parsed_command)

        writer.write(encode_resp2(response))
        await writer.drain()


async def run_server(host: str, port: int):
    server_socket = await asyncio.start_server(
        on_client_connect,
        host=host,
        port=port,
        reuse_port=True,
        start_serving=False,
    )

    await server_socket.serve_forever()

    server_socket.close()
    await server_socket.wait_closed()
