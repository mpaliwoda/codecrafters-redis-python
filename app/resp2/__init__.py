from typing import Optional, TypeAlias


KVStore: TypeAlias = dict[str | int, tuple[str | int, Optional[float]]]
