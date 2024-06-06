import asyncio
import datetime
from typing import TypeAlias
from app.resp2 import KVStore, parser, evaluator

Address: TypeAlias = str
Port: TypeAlias = int

ClientId: TypeAlias = str
ClientTask: TypeAlias = asyncio.Task

_clients: dict[ClientId, ClientTask] = {}
_kv_store: KVStore = {}

async def on_client_connect(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    client_id = writer.get_extra_info('peername')

    def client_cleanup(future: asyncio.Future):
        try:
            future.result()
        except Exception as e:
            print(e)
        finally:
            del _clients[client_id]

    client_task= asyncio.ensure_future(listen_task(reader, writer))
    client_task.add_done_callback(client_cleanup)

    _clients[client_id] = client_task


async def listen_task(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    while True:
        data = await reader.read(1024)

        if data == b"":
            break

        _, parsed = parser.parse_message(data)
        response = evaluator.exec(parsed, _kv_store)

        writer.write(response)
        await writer.drain()


async def expiry_task() -> None:
    now = datetime.datetime.now().timestamp() * 1000

    while True:
        for key, (_, expiry) in _kv_store.items():
            if expiry is None:
                continue
            if now >= expiry:
                del _kv_store[key]
        await asyncio.sleep(0.1)


async def main():
    server_socket = await asyncio.start_server(
        on_client_connect,
        host="127.0.0.1",
        port=6379,
        reuse_port=True,
        start_serving=False,
    )

    asyncio.create_task(expiry_task())
    await server_socket.serve_forever()

    server_socket.close()
    await server_socket.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
