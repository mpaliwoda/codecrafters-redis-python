from app.resp2 import obj


class Token:
    ARRAY = ord("*")
    BULK = ord("$")
    ERROR = ord("-")
    INTEGER = ord(":")
    STRING = ord("+")
    CARRIAGE_RET = ord("\r")


class Parser:
    """Not exactly what you'd call a high performance parser"""

    def __init__(self, message: bytes) -> None:
        if message == b"":
            raise ValueError("Cannot parse empty message")

        self._message = message
        self._position = 0

    def parse_statement(self) -> obj.Obj:
        match self.current_char:
            case Token.BULK:
                return self.read_bulk()
            case Token.STRING:
                return self.read_string()
            case Token.ERROR:
                return self.read_err()
            case Token.INTEGER:
                return self.read_int()
            case Token.ARRAY:
                return self.read_arr()
            case _:
                raise RuntimeError("Failed to parse message")

    @property
    def current_char(self) -> int:
        return self._message[self._position]

    def advance_position(self, offset: int = 1) -> None:
        self._position += offset

    def read_bulk(self) -> obj.String:
        self.advance_position()
        start = self._position
        while self.current_char != Token.CARRIAGE_RET:
            self.advance_position()
        size = int(self._message[start : self._position].decode())
        self.advance_position(offset=2)
        raw_val = self._message[self._position : self._position + size]
        self.advance_position(offset=size + 2)
        return obj.String(raw_val.decode())

    def read_string(self) -> obj.String:
        self.advance_position()
        start = self._position
        while self.current_char != Token.CARRIAGE_RET:
            self.advance_position()
        raw_val = self._message[start : self._position]
        self.advance_position(offset=2)
        return obj.String(raw_val.decode())

    def read_int(self) -> obj.Integer:
        self.advance_position()
        start = self._position
        while self.current_char != Token.CARRIAGE_RET:
            self.advance_position()
        raw_val = self._message[start : self._position]
        self.advance_position(offset=2)
        return obj.Integer(int(raw_val.decode()))

    def read_err(self) -> obj.Err:
        self.advance_position()
        start = self._position
        while self.current_char != Token.CARRIAGE_RET:
            self.advance_position()
        raw_val = self._message[start : self._position]
        self.advance_position(offset=2)
        return obj.Err(raw_val.decode())

    def read_arr(self) -> obj.Arr:
        self.advance_position()
        start = self._position
        while self.current_char != Token.CARRIAGE_RET:
            self.advance_position()
        size = int(self._message[start : self._position].decode())
        self.advance_position(offset=2)
        return obj.Arr(elements=[self.parse_statement() for _ in range(size)])
