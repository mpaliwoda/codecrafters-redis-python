import socket


def main() -> None:
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    client, _addr = server_socket.accept() # wait for client

    while True:
        message = client.recv(1024)

        if not message:
            break

        client.send("+PONG\r\n".encode())


if __name__ == "__main__":
    main()
