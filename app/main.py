import socket


def main() -> None:
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    client, _addr = server_socket.accept() # wait for client

    raw_message = client.recv(1024)
    _message = raw_message.decode()

    resp = "+PONG\r\n"

    client.send(resp.encode())


if __name__ == "__main__":
    main()
