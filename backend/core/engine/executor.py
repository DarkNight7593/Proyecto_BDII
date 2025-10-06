from typing import Any, Dict, List
from ..model.base import RID

class Executor:
    def __init__(self, heap, indexes: List):
        self.heap = heap                         # heap de la tabla
        self.indexes = indexes or []             # índices registrados (por columna)

    def insert(self, row: Dict[str, Any]) -> RID:
        rid = self.heap.insert(row)              # escribe fila en heap
        for idx in self.indexes:                 # actualiza TODOS los índices
            try:
                idx.insert(row[idx.key_col], rid)
            except KeyError:
                raise ValueError(f"Falta columna indexada: {idx.key_col}")
        return rid

    def select_eq(self, col: str, key: Any) -> List[Dict[str, Any]]:
        idx = next((i for i in self.indexes if i.key_col == col), None)
        if idx:                                  # si hay índice, úsalo
            return [self.heap.read(r) for r in idx.search(key)]
        return list(self.heap.scan_eq(col, key)) # sin índice: full-scan

    def select_between(self, col: str, lo: Any, hi: Any) -> List[Dict[str, Any]]:
        idx = next((i for i in self.indexes if i.key_col == col), None)
        if idx:
            return [self.heap.read(r) for r in idx.range_search(lo, hi)]
        return list(self.heap.scan_range(col, lo, hi))  # sin índice: full-scan

    def delete(self, col: str, key: Any) -> int:
        idx = next((i for i in self.indexes if i.key_col == col), None)
        rows = ([self.heap.read(r) for r in idx.search(key)]  # localiza por índice
                if idx else list(self.heap.scan_eq(col, key)))# o full-scan
        cnt = 0
        for row in rows:
            rid = RID(row["_rid"]["page"], row["_rid"]["slot"])
            if self.heap.delete(rid):
                if idx: idx.delete(key, rid)
                cnt += 1
        return cnt
