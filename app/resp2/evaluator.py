import datetime
from typing import Any, Literal, TypeGuard, TypeVar, Type, Optional
from app.kv_store import KVStore, obj
from collections.abc import Iterable

from app.server.info import ServerInfo


T = TypeVar("T")


def ensure_iterable_type(v: Iterable[Any], typ: Type[T]) -> TypeGuard[Iterable[T]]:
    return all(isinstance(elem, typ) for elem in v)


Px = Literal["px"]
PxAt = Literal["pxat"]
Ex = Literal["ex"]
ExAt = Literal["exat"]

Nx = Literal["nx"]
Xx = Literal["xx"]

SetIf = Nx | Xx
Expiry = tuple[Px | PxAt | Ex | ExAt, float]


class Evaluator:
    def __init__(self, kv_store: KVStore, server_info: ServerInfo) -> None:
        self.kv_store = kv_store
        self._server_info = server_info

    def eval(self, command: obj.Obj) -> obj.Obj:
        match command:
            case obj.Arr(elements=elements):
                # heck yeah narrow those types brother
                if not ensure_iterable_type(elements, obj.String):
                    raise RuntimeError(f"malformed command: {elements}")
                func, *args = elements
                return self.exec_command(func, *args)
            case obj.String():
                return self.exec_command(command)
            case _:
                raise RuntimeError("malformed command")

    def exec_command(self, command: obj.String, *args: obj.String) -> obj.Obj:
        match command.val.lower():
            case "ping":
                if len(args) > 0:
                    raise ValueError(f"Expected ping to have no args, got: {args}")
                return obj.String("PONG")
            case "echo":
                if len(args) != 1:
                    raise ValueError(f"Expected ping to have exactly 1 arg, got: {args}")
                return args[0]
            case "get":
                if len(args) != 1:
                    raise ValueError(f"Expected get to have exactly 1 arg, got: {args}")
                key = args[0]
                return self.kv_store.get(key)
            case "set":
                key, val, *flags = args
                return self.set_handler(key, val, *flags)
            case "info":
                return self.info_handler(*args)
            case val:
                print(f"got unsupported command: {val} {args}")
                return obj.Null()

    def info_handler(self, *args) -> obj.Obj:
        match args:
            case [subcommand] if isinstance(subcommand, obj.String):
                match subcommand.val.lower():
                    case "replication":
                        info = self._server_info.to_string()
                        return obj.String(info)
                    case _:
                        raise RuntimeError("not supported yet")
            case _:
                raise RuntimeError("not supported yet")

    def set_handler(self, key: obj.String, val: obj.String, *flags: obj.String) -> obj.Obj:
        set_if, expiry = self._parse_set_flags(*flags)
        expires_at = self._calc_expiries_at(expiry)
        return self.kv_store.set(key, val, expires_at, set_if)

    @staticmethod
    def _parse_set_flags(
        *flags: obj.String,
    ) -> tuple[Optional[SetIf], Optional[Expiry]]:
        ix = 0

        expiry: Optional[Expiry] = None
        set_if: Optional[SetIf] = None

        while ix < len(flags):
            flag = flags[ix]

            match f := flag.val.lower():
                case "px" | "pxat" | "ex" | "exat":
                    if expiry:
                        raise RuntimeError("malformed set command")

                    expiry_val = flags[ix + 1]
                    expiry = f, float(expiry_val.val)
                    ix += 2
                case "nx" | "xx":
                    if set_if:
                        raise RuntimeError("malformed set command")

                    set_if = f
                    ix += 1

        return set_if, expiry

    @staticmethod
    def _calc_expiries_at(expiry: Optional[Expiry]) -> float | None:
        match expiry:
            case None:
                return None
            case ("px", exp):
                return (datetime.datetime.now().timestamp() * 1000.0) + exp
            case ("ex", exp):
                return (datetime.datetime.now().timestamp() * 1000.0) + (exp * 1000.0)
            case ("pxat", exp):
                return exp
            case ("exat", exp):
                return exp * 1000.0

        assert False, "unreachable"
