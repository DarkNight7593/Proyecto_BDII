# HeapFile con slotted pages en binario.
# Página (4KB): [Header][Slots][...libre...][Datos]
# Header: [nslots:uint16][data_end:uint16]; Slot: [off:uint16][len:uint16], len==0 => libre

import os, struct
from typing import Dict, Any, Optional, Tuple, List, Iterator
from ...model.base import RID
from .rowfmt import pack_row, unpack_row

PAGE_SIZE = 4096
HDR_FMT = "<HH"; HDR_SIZE = struct.calcsize(HDR_FMT)     # nslots, data_end
SLOT_FMT = "<HH"; SLOT_SIZE = struct.calcsize(SLOT_FMT)  # off, len

class HeapFile:
    def __init__(self, path: str, schema: List[Tuple[str, str]]):
        self.path = path                                   # ruta de archivo
        self.schema = schema                               # schema de la tabla
        if not os.path.exists(path):
            with open(path, "wb"): pass                    # crea archivo vacío

    # ---------- utilidades de página ----------
    def _num_pages(self) -> int:                           # cantidad de páginas
        return os.path.getsize(self.path) // PAGE_SIZE

    def _base(self, p: int) -> int:                        # offset inicio de página p
        return p * PAGE_SIZE

    def _ensure_page(self, p: int) -> None:                # garantiza existencia de página p
        if p < self._num_pages(): return
        with open(self.path, "ab") as f:
            for _ in range(p - self._num_pages() + 1):
                f.write(b"\x00" * PAGE_SIZE)
        self._write_hdr(p, 0, PAGE_SIZE)                  # header inicial

    def _read_hdr(self, p: int) -> Tuple[int, int]:        # lee (nslots, data_end)
        with open(self.path, "rb") as f:
            f.seek(self._base(p))
            return struct.unpack(HDR_FMT, f.read(HDR_SIZE))

    def _write_hdr(self, p: int, nslots: int, data_end: int) -> None:  # escribe header
        with open(self.path, "r+b") as f:
            f.seek(self._base(p))
            f.write(struct.pack(HDR_FMT, nslots, data_end))

    def _read_slot(self, p: int, s: int) -> Tuple[int, int]:           # lee (off,len) slot s
        with open(self.path, "rb") as f:
            f.seek(self._base(p) + HDR_SIZE + s * SLOT_SIZE)
            return struct.unpack(SLOT_FMT, f.read(SLOT_SIZE))

    def _write_slot(self, p: int, s: int, off: int, ln: int) -> None:  # escribe (off,len) slot s
        with open(self.path, "r+b") as f:
            f.seek(self._base(p) + HDR_SIZE + s * SLOT_SIZE)
            f.write(struct.pack(SLOT_FMT, off, ln))

    def _find_free_slot(self, p: int, nslots: int) -> Optional[int]:   # busca slot con len==0
        for s in range(nslots):
            _, ln = self._read_slot(p, s)
            if ln == 0: return s
        return None

    def _free_bytes(self, nslots: int, data_end: int, reuse_slot: bool) -> int:
        dir_end = HDR_SIZE + (nslots + (0 if reuse_slot else 1)) * SLOT_SIZE  # fin del directorio
        return max(0, data_end - dir_end)                                     # bytes libres

    # ---------- API principal ----------
    def insert(self, row: Dict[str, Any]) -> RID:
        blob = pack_row(row, self.schema)                         # serializa en binario
        if len(blob) + HDR_SIZE + SLOT_SIZE > PAGE_SIZE:
            raise ValueError("Fila demasiado grande para una página")  # no partimos filas

        if self._num_pages() == 0: self._ensure_page(0)           # asegura primera página
        p = self._num_pages() - 1                                 # intenta en última página

        for _ in range(2):                                        # última o nueva
            nslots, data_end = self._read_hdr(p)
            s_free = self._find_free_slot(p, nslots)
            reuse = s_free is not None

            if self._free_bytes(nslots, data_end, reuse) >= len(blob):
                s = s_free if reuse else nslots                   # elige slot
                if not reuse:
                    nslots += 1
                    self._write_hdr(p, nslots, data_end)          # reserva slot nuevo
                data_end -= len(blob)
                with open(self.path, "r+b") as f:
                    f.seek(self._base(p) + data_end)
                    f.write(blob)                                 # escribe blob al final
                self._write_slot(p, s, data_end, len(blob))       # actualiza slot
                self._write_hdr(p, nslots, data_end)              # actualiza header
                return RID(p, s)

            p = self._num_pages()
            self._ensure_page(p)                                   # crea nueva página

        raise RuntimeError("No se pudo insertar")                  # no debería alcanzarse

    def read(self, rid: RID) -> Dict[str, Any]:
        nslots, _ = self._read_hdr(rid.page)
        if rid.slot < 0 or rid.slot >= nslots: raise KeyError("slot fuera de rango")
        off, ln = self._read_slot(rid.page, rid.slot)
        if ln == 0: raise KeyError("slot borrado")
        with open(self.path, "rb") as f:
            f.seek(self._base(rid.page) + off)
            buf = f.read(ln)                                       # lee bytes exactos
        row = unpack_row(buf, self.schema)                         # binario -> dict
        row["_rid"] = {"page": rid.page, "slot": rid.slot}         # adjunta RID
        return row

    def delete(self, rid: RID) -> bool:
        nslots, _ = self._read_hdr(rid.page)
        if rid.slot < 0 or rid.slot >= nslots: return False
        off, ln = self._read_slot(rid.page, rid.slot)
        if ln == 0: return False
        self._write_slot(rid.page, rid.slot, off, 0)               # marca slot libre
        return True

    # ---------- iteradores y scans (sin índice) ----------
    def iter_rids(self) -> Iterator[RID]:
        np = self._num_pages()
        for p in range(np):
            nslots, _ = self._read_hdr(p)
            for s in range(nslots):
                _, ln = self._read_slot(p, s)
                if ln != 0: yield RID(p, s)                        # solo vivos

    def iter_rows(self) -> Iterator[Dict[str, Any]]:
        for rid in self.iter_rids():
            yield self.read(rid)                                   # reusa read()

    def scan_eq(self, col: str, key: Any):
        for row in self.iter_rows():
            if row.get(col) == key:
                yield row

    def scan_range(self, col: str, lo: Any, hi: Any):
        if lo > hi: lo, hi = hi, lo
        for row in self.iter_rows():
            v = row.get(col)
            if v is not None and lo <= v <= hi:
                yield row
