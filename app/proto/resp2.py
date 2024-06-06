from enum import Enum
from app.proto.ast import Ast, Arr, Err, Integer, Null, String, Bulk


class Token(Enum):
    ARRAY = ord("*")
    BULK = ord("$")
    ERROR = ord("-")
    INTEGER = ord(":")
    STRING = ord("+")


CARRIAGE_RET = ord("\r")
NEWLINE = ord("\n")


def parse_message(msg: bytes, ix: int = 0) -> tuple[int, Ast]:
    char = msg[ix]

    match Token(char):
        case Token.STRING:
            return read_string(msg, ix)
        case Token.INTEGER:
            return read_int(msg, ix)
        case Token.ERROR:
            return read_err(msg, ix)
        case Token.BULK:
            return read_bulk(msg, ix)
        case Token.ARRAY:
            return read_arr(msg, ix)


def read_string(msg: bytes, ix: int) -> tuple[int, String]:
    ix += 1
    val_end = _read_val_until_separator(msg, ix)
    ast_elem = String(val=msg[ix:val_end].decode())
    ix = _consume_separator(msg, val_end)
    return ix, ast_elem


def read_int(msg: bytes, ix: int) -> tuple[int, Integer]:
    ix += 1
    val_end = _read_val_until_separator(msg, ix)
    ast_elem = Integer(val=int(msg[ix:val_end].decode()))
    ix = _consume_separator(msg, val_end)
    return ix, ast_elem


def read_err(msg: bytes, ix: int) -> tuple[int, Err]:
    ix += 1
    val_end = _read_val_until_separator(msg, ix)
    ast_elem = Err(val=msg[ix:val_end].decode())
    ix = _consume_separator(msg, val_end)
    return ix, ast_elem


def read_bulk(msg: bytes, ix: int) -> tuple[int, Bulk | Null]:
    ix += 1
    # get bulk string size
    val_end =_read_val_until_separator(msg, ix)
    size = int(msg[ix:val_end].decode())
    ix = _consume_separator(msg, val_end)

    if size == -1:
        return ix, Null(tok="$")
    else:
        val_end = _read_val_until_separator(msg, ix)
        assert val_end - ix == size
        ast_elem = Bulk(val=msg[ix:val_end].decode(), size=size)
        ix = _consume_separator(msg, val_end)
        return ix, ast_elem


def read_arr(msg, ix) -> tuple[int, Arr]:
    ix += 1
    # get array string size
    val_end = _read_val_until_separator(msg, ix)
    size = int(msg[ix:val_end].decode())
    ix = _consume_separator(msg, val_end)

    elements: list[Ast] = []

    for _ in range(size):
        ix, ast_elem = parse_message(msg, ix)
        elements.append(ast_elem)

    return ix, Arr(elements, size)


def _read_val_until_separator(msg: bytes, ix: int) -> int:
    current = ix
    while msg[current] != CARRIAGE_RET:
        current += 1

    return current


def _consume_separator(msg: bytes, ix: int) -> int:
    assert msg[ix] == CARRIAGE_RET
    assert msg[ix + 1] == NEWLINE
    return ix + 2
