# Adaptador del secuencial a interfaz Index (clave int).

from typing import Any, List, Optional
from ...model.base import RID, Index
from .lowlevel import LowLevelSequentialFile, SFEntry

class SequentialFileIndex(Index):
    name = "seqfile"
    def __init__(self, path: str, key_col: str):
        self.key_col = key_col
        self.sf = LowLevelSequentialFile(path)

    def insert(self, key: Any, rid: RID) -> None:
        self.sf.insert(SFEntry(int(key), rid))

    def search(self, key: Any) -> List[RID]:
        return [rid for rid in self.sf.search(int(key))]

    def range_search(self, lo: Any, hi: Any) -> List[RID]:
        return [e.rid for e in self.sf.range_search(int(lo), int(hi))]

    def delete(self, key: Any, rid: Optional[RID] = None) -> int:
        return self.sf.delete_key(int(key), rid)
