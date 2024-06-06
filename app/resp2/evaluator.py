import datetime
from dataclasses import dataclass
from typing import Literal, TypeAlias, cast
from app.resp2 import KVStore, ast

def exec(command_ast: ast.Ast, kv_store: KVStore) -> bytes:
    match command_ast:
        case ast.Arr(elements=[command, *args]) if type(command) == ast.String:
            return exec_command(command, kv_store, *args)
        case _:
            raise ValueError(f"Malfromed message: {command_ast}")


def exec_command(command: ast.String, kv_store: KVStore, *args: ast.Ast) -> bytes:
    match command.val.lower():
        case "ping":
            if len(args) > 0:
                raise ValueError(f"Expected ping to have no args, got: {args}")
            return ping()
        case "echo":
            if len(args) != 1:
                raise ValueError(f"Expected ping to have exactly 1 arg, got: {args}")
            return echo(args[0])
        case "get":
            if len(args) != 1:
                raise ValueError(f"Expected get to have exactly 1 arg, got: {args}")
            return get_handler(kv_store, args[0])
        case "set":
            return set_handler(kv_store, *args)
        case val:
            print(f"got unsupported command: {val} {args}")
            return nil()


def ping() -> bytes:
    return ast.String("PONG").encode()


def echo(echo: ast.Ast) -> bytes:
    return echo.encode()

def nil() -> bytes:
    return ast.Null().encode()


def get_handler(kv_store: KVStore, key: ast.Ast) -> bytes:
    if type(key) not in (ast.String, ast.Integer):
        return nil()

    key = cast(ast.String | ast.Integer, key)
    res = kv_store.get(key.val)

    match res:
        case None:
            return nil()
        case (val, expiry):
            if expiry is not None and (datetime.datetime.now().timestamp() * 1000.) >= expiry:
                return nil()

            match val:
                case str():
                    return ast.String(val).encode()
                case int():
                    return ast.Integer(val).encode()

    return nil()


@dataclass
class Px:
    expiry_ms: float


@dataclass
class PxAt:
    expiry_at_ms: float


@dataclass
class Ex:
    expiry_s: float


@dataclass
class ExAt:
    expiry_at_s: float


Nx = Literal["nx"]
Xx = Literal["xx"]


Flag: TypeAlias = Nx | Xx | Ex | ExAt | Px | PxAt


def set_handler(kv_store: KVStore, *args: ast.Ast) -> bytes:
    key, val, *flags = args

    key = cast(ast.String | ast.Integer , key)
    val = cast(ast.String | ast.Integer , val)

    parsed_flags = _parse_set_flags(*flags)
    expiry: float | None = None

    for flag in parsed_flags:
        match flag:
            case "nx":
                if kv_store.get(key.val):
                    return nil()
            case "xx":
                match kv_store.get(key.val):
                    case None:
                        pass
                    case (_, None):
                        pass
                    case (_, float(expiry)):
                        if (datetime.datetime.now().timestamp() * 1000.) >= expiry:
                            pass
                        else:
                            return nil()
            case Px(exp):
                expiry = (datetime.datetime.now().timestamp() * 1000.) + exp
            case Ex(exp):
                expiry = (datetime.datetime.now().timestamp() * 1000.) + (exp * 1000.)
            case PxAt(exp):
                expiry = exp
            case ExAt(exp):
                expiry = exp * 1000.
    else:
        key = cast(ast.String | ast.Integer, key)
        val = cast(ast.String | ast.Integer, val)

        kv_store[key.val] = (val.val, expiry)

        return ast.String("OK").encode()

def _parse_set_flags(*flags: ast.Ast) -> list[Flag]:
    ix = 0
    parsed_flags: list[Flag] = []

    while ix < len(flags):
        flag = flags[ix] 
        assert type(flag) == ast.String

        match flag.val.lower():
            case "px":
                assert ix + 1 < len(flags)
                expiry_ast = flags[ix + 1]
                assert hasattr(expiry_ast, "val")
                parsed_flags.append(Px(float(expiry_ast.val)))
                ix += 2
            case "pxat":
                assert ix + 1 < len(flags)
                expiry_ast = flags[ix + 1]
                assert hasattr(expiry_ast, "val")
                parsed_flags.append(PxAt(float(expiry_ast.val)))
                ix += 2
            case "ex":
                assert ix + 1 < len(flags)
                expiry_ast = flags[ix + 1]
                assert hasattr(expiry_ast, "val")
                parsed_flags.append(ExAt(float(expiry_ast.val)))
                ix += 2
            case "exat":
                assert ix + 1 < len(flags)
                expiry_ast = flags[ix + 1]
                assert hasattr(expiry_ast, "val")
                parsed_flags.append(ExAt(float(expiry_ast.val)))
                ix += 2
            case "nx":
                parsed_flags.append("nx")
                ix += 1
            case "xx":
                parsed_flags.append("xx")
                ix += 1

    return parsed_flags
