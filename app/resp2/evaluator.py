import datetime
from typing import Any, Literal, TypeGuard, TypeVar
from app import assert_that, die
from app.kv_store import KVStore, obj
from collections.abc import Iterable

from app.server.info import ServerInfo


T = TypeVar("T")


def ensure_iterable_type(v: Iterable[Any], typ: type[T]) -> TypeGuard[Iterable[T]]:
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
                if not ensure_iterable_type(elements, obj.String):
                    die(f"malformed command: {elements}")
                func, *args = elements
                return self.exec_command(func, *args)
            case obj.String():
                return self.exec_command(command)
            case _:
                die(f"malformed command: {command}")

    def exec_command(self, command: obj.String, *args: obj.String) -> obj.Obj:
        match command.val.lower():
            case "ping":
                assert_that(lambda: len(args) == 0, f"Expected ping to have no args, got: {args}")
                return obj.String("PONG", simple=True)
            case "echo":
                assert_that(lambda: len(args) == 1, f"Expected echo to have exactly 1 arg, got: {args}")
                return args[0]
            case "get":
                assert_that(lambda: len(args) == 1, f"Expected get to have exactly 1 arg, got: {args}")
                key = args[0]
                return self.kv_store.get(key)
            case "set":
                key, val, *flags = args
                return self.set_handler(key, val, *flags)
            case "info":
                return self.info_handler(*args)
            case "replconf":
                return self.replconf_handler(*args)
            case "psync":
                assert_that(lambda: len(args) == 2, "Invalid usage of psync")
                replid, offset = args
                return self.psync_handler(replid, offset)
            case val:
                die(f"got unsupported command: {val} {args}")

    def info_handler(self, *args: obj.String) -> obj.Obj:
        match args:
            case [subcommand] if isinstance(subcommand, obj.String):
                match subcommand.val.lower():
                    case "replication":
                        info = self._server_info.to_string()
                        return obj.String(info)
                    case _:
                        die("not supported yet")
            case _:
                return obj.String(val="OK", simple=True)

    def replconf_handler(self, *args: obj.String) -> obj.Obj:
        arg, *rest = args

        match arg.val.lower():
            case "listening-port":
                return obj.String(val="OK", simple=True)
            case "capa":
                return obj.String(val="OK", simple=True)
            case _:
                die(f"unsupported replconf: {args}")

    def psync_handler(self, replid: obj.String, offset: obj.String) -> obj.Obj:
        if replid.val == "?" and int(offset.val) == -1:
            return obj.String(f"FULLRESYNC {self._server_info.master_replid} 0", simple=True)
        die("we haven't gone that far yet")

    def set_handler(self, key: obj.String, val: obj.String, *flags: obj.String) -> obj.Obj:
        set_if, expiry = self._parse_set_flags(*flags)
        expires_at = self._calc_expiries_at(expiry)
        return self.kv_store.set(key, val, expires_at, set_if)

    @staticmethod
    def _parse_set_flags(
        *flags: obj.String,
    ) -> tuple[SetIf | None, Expiry | None]:
        ix = 0

        expiry: Expiry | None = None
        set_if: SetIf | None = None

        while ix < len(flags):
            flag = flags[ix]

            match f := flag.val.lower():
                case "px" | "pxat" | "ex" | "exat":
                    assert_that(lambda: expiry is None, "malformed set command")
                    expiry_val = flags[ix + 1]
                    expiry = f, float(expiry_val.val)
                    ix += 2
                case "nx" | "xx":
                    assert_that(lambda: set_if is None, "malformed set command")
                    set_if = f
                    ix += 1

        return set_if, expiry

    @staticmethod
    def _calc_expiries_at(expiry: Expiry | None) -> float | None:
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
