from app.proto import ast


def exec(command_ast: ast.Ast) -> bytes:
    match command_ast:
        case ast.Arr(elements=[ast.String(command)]) | ast.Arr(elements=[ast.Bulk(command)]) if command.lower() == "ping":
            return ping_handler()
        case ast.Arr(elements=[ast.String(command), echo]) | ast.Arr(elements=[ast.Bulk(command), echo]) if command.lower() == "echo":
            return echo_handler(echo)
        case _:
            raise ValueError(f"Command not supported: {command_ast}")


def ping_handler() -> bytes:
    return ast.String("PONG").encode()


def echo_handler(echo: ast.Ast) -> bytes:
    return echo.encode()
