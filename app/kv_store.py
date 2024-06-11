from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from app.resp2 import obj


@dataclass
class ValWrapper:
    val: obj.String
    expires_at: float | None

    def expired(self) -> bool:
        if not self.expires_at:
            return False

        ts = datetime.now().timestamp() * 1000.0
        return ts >= self.expires_at


class KVStore:
    def __init__(self) -> None:
        self._store: dict[obj.String, ValWrapper] = {}

    def get(self, key: obj.String) -> obj.String | obj.Null:
        result = self._store.get(key)

        if isinstance(result, ValWrapper) and not result.expired():
            return result.val

        return obj.Null()

    def set(
        self,
        key: obj.String,
        val: obj.String,
        expires_at: float | None,
        set_if: Literal["nx", "xx"] | None,
    ) -> obj.String | obj.Null:
        null = obj.Null()

        match set_if:
            case "nx":
                if self.get(key) != null:
                    return null
            case "xx":
                if self.get(key) == null:
                    return null

        self._store[key] = ValWrapper(val, expires_at)
        return obj.String("OK")
