from app.resp2 import obj


def encode_resp2(o: obj.Obj) -> bytes:
    match o:
        case obj.String(val, simple=False):
            return f"${len(val)}\r\n{val}\r\n".encode()
        case obj.String(val, simple=True):
            return f"+{val}\r\n".encode()
        case obj.Integer(val):
            return f":{val}\r\n".encode()
        case obj.Err(val):
            return f"-{val}\r\n".encode()
        case obj.Null():
            return "$-1\r\n".encode()
        case obj.Arr(elements):
            s = f"*{len(elements)}\r\n".encode()
            el = b"".join(map(lambda elem: encode_resp2(elem), elements))
            return s + el
