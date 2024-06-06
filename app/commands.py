from typing import cast
from app.proto import ast

_kv_store: dict = {}

def exec(command_ast: ast.Ast) -> bytes:
    match command_ast:
        # unary commands
        case ast.Arr(elements=[elem]):
            return exec_unary_command(elem)
        # binary commands
        case ast.Arr(elements=[command, arg]):
            return exec_binary_command(command, arg)
        case ast.Arr(elements=[command, arg1, arg2]):
            return exec_ternary_command(command, arg1, arg2)
        case _:
            raise ValueError(f"Command not supported: {command_ast}")


def exec_unary_command(command: ast.Ast) -> bytes:
    assert type(command) == ast.String

    match command.val.lower():
        case "ping":
            return ping_handler()
        case _:
            raise ValueError("command '{command}' not supported")


def exec_binary_command(command: ast.Ast, arg: ast.Ast) -> bytes:
    assert type(command) == ast.String

    match command.val.lower():
        case "echo":
            return echo_handler(arg)
        case "get":
            return get_handler(arg)
        case _:
            raise ValueError("command '{command}' not supported")


def exec_ternary_command(command: ast.Ast, arg1: ast.Ast, arg2: ast.Ast) -> bytes:
    assert type(command) == ast.String

    match command.val.lower():
        case "set":
            return set_handler(arg1, arg2)
        case _:
            return nil_handler()


def ping_handler() -> bytes:
    return ast.String("PONG").encode()


def echo_handler(echo: ast.Ast) -> bytes:
    return echo.encode()

def nil_handler() -> bytes:
    return ast.Null().encode()


def get_handler(key: ast.Ast) -> bytes:
    if type(key) not in (ast.String, ast.Integer):
        return nil_handler()

    key = cast(ast.String | ast.Integer, key)

    match _kv_store.get(key.val):
        case str(val):
            return ast.String(val).encode()
        case int(val):
            return ast.Integer(val).encode()
        case _:
            return nil_handler()


def set_handler(key: ast.Ast, val: ast.Ast) -> bytes:
    if type(key) not in (ast.String, ast.Integer):
        return nil_handler()

    if type(val) == ast.Arr:
        return nil_handler()

    key = cast(ast.String | ast.Integer, key)
    val = cast(ast.String | ast.Integer, val)
    _kv_store[key.val] = val.val

    return ast.String("OK").encode()
