# Índice secuencial D/A en disco:
#  - Entrada: [key:int][rid.page:uint16][rid.slot:uint16][next:int]
#  - Header : [main_count:int][aux_count:int][head_ptr:int]
#  - D se mantiene ordenado tras reorganize(); A recibe inserts.
#  - La lista lógica enlaza D<->A con punteros (enteros), saltando borrados.

import os, struct, math
from typing import List, Optional, Tuple
from ...model.base import RID

# ------------------ punteros y constantes ------------------

DELETED = -1  # tombstone para entradas borradas

def dptr(i: int) -> int:
    """Puntero a D (1-based)."""
    if i < 1:
        raise ValueError("dptr(i) requiere i>=1")
    return i

def aptr(i: int) -> int:
    """Puntero a A (1-based). a(1) = -2, a(2) = -3, ..."""
    if i < 1:
        raise ValueError("aptr(i) requiere i>=1")
    return -(i + 1)  # cuidado: NO pongas coma al final o devuelves una tupla

def is_end(p: int) -> bool:
    """True si p es fin de lista."""
    return p == 0

def loc(p: int) -> Tuple[bool, int]:
    """Convierte puntero entero a (is_aux, idx 1-based)."""
    if p == 0 or p == DELETED:
        raise ValueError("puntero fin o tombstone no tiene ubicación")
    return (False, p) if p > 0 else (True, -p - 1)

# ------------------ layout binario ------------------

ENTRY_FMT = "<iHHi"                     # key:int, page:uint16, slot:uint16, next:int
ENTRY_SIZE = struct.calcsize(ENTRY_FMT)
HDR_FMT   = "<iii"                      # main_count, aux_count, head_ptr
HDR_SIZE  = struct.calcsize(HDR_FMT)

class SFEntry:
    """Entrada del índice: clave, RID y puntero al siguiente."""
    __slots__ = ("key", "rid", "next_ptr")

    def __init__(self, key: int, rid: RID, next_ptr: int = 0):
        self.key = int(key)
        self.rid = rid
        self.next_ptr = int(next_ptr)

    def pack(self) -> bytes:
        """Empaqueta a binario."""
        return struct.pack(ENTRY_FMT, int(self.key), int(self.rid.page), int(self.rid.slot), int(self.next_ptr))

    @staticmethod
    def unpack(b: bytes) -> "SFEntry":
        """Desempaqueta desde binario."""
        k, p, s, n = struct.unpack(ENTRY_FMT, b)
        return SFEntry(int(k), RID(int(p), int(s)), int(n))

    def deleted(self) -> bool:
        """True si es tombstone."""
        return self.next_ptr == DELETED

# ------------------ archivo índice ------------------

class LowLevelSequentialFile:
    """
    Archivo de índice secuencial con:
      - Región principal D (ordenada físicamente tras reorganize)
      - Región auxiliar A (inserciones)
      - Cadena lógica ordenada por 'next_ptr' que salta D<->A
    Política: insertar en A, encadenar ordenado, y reorganizar cuando A supera umbral log2.
    """

    def __init__(self, path: str):
        self.path = path
        if not os.path.exists(path):
            # crea header limpio (0,0,0)
            with open(path, "wb") as f:
                f.write(struct.pack(HDR_FMT, 0, 0, 0))

    # ----- header -----

    def _hdr_get(self) -> Tuple[int, int, int]:
        """Lee (main_count, aux_count, head_ptr)."""
        with open(self.path, "rb") as f:
            f.seek(0)
            m, a, h = struct.unpack(HDR_FMT, f.read(HDR_SIZE))
            return int(m), int(a), int(h)

    def _hdr_set(self, m: int, a: int, h: int) -> None:
        """Escribe header (con cast defensivo a int)."""
        m = int(m); a = int(a); h = int(h)
        with open(self.path, "r+b") as f:
            f.seek(0)
            f.write(struct.pack(HDR_FMT, m, a, h))

    # ----- offsets -----

    def _off_d(self, i: int) -> int:
        """Offset byte de d(i) (1-based)."""
        return HDR_SIZE + (i - 1) * ENTRY_SIZE

    def _off_a(self, i: int, base: int = None) -> int:
        """Offset byte de a(i) (1-based), con base = main_count fija para lecturas estables."""
        if base is None:
            base, _, _ = self._hdr_get()
        return HDR_SIZE + base * ENTRY_SIZE + (i - 1) * ENTRY_SIZE

    # ----- I/O de entradas -----

    def _read(self, is_aux: bool, idx: int, base: int = None) -> SFEntry:
        """Lee entrada en D/A índice 1-based."""
        with open(self.path, "rb") as f:
            f.seek(self._off_a(idx, base) if is_aux else self._off_d(idx))
            return SFEntry.unpack(f.read(ENTRY_SIZE))

    def _write(self, is_aux: bool, idx: int, e: SFEntry, base: int = None) -> None:
        """Escribe entrada en D/A índice 1-based."""
        with open(self.path, "r+b") as f:
            f.seek(self._off_a(idx, base) if is_aux else self._off_d(idx))
            f.write(e.pack())

    # ----- búsqueda en D -----

    def _lb(self, key: int) -> int:
        """lower_bound en D: primer i con d(i).key >= key; m+1 si no hay."""
        m, _, _ = self._hdr_get()
        l, r, ans = 1, m, m + 1
        while l <= r:
            mid = (l + r) // 2
            e = self._read(False, mid, m)
            if e.key >= key:
                ans = mid
                r = mid - 1
            else:
                l = mid + 1
        return ans

    # ----- operaciones -----

    def insert(self, e: SFEntry) -> None:
        """Inserta en A y encadena en la lista lógica en orden por key."""
        m, a, h = self._hdr_get()

        # 1) guarda en A
        idx = a + 1
        e.next_ptr = 0
        self._write(True, idx, e, m)
        a += 1
        newp = aptr(idx)  # puntero lógico al nuevo nodo en A

        # 2) lista vacía: nuevo head
        if h == 0:
            self._hdr_set(m, a, newp)
            self._maybe_reorg()
            return

        # 3) predecesor en D (saltando borrados)
        lb = self._lb(e.key)
        j = min(lb - 1, m)
        while j >= 1:
            dj = self._read(False, j, m)
            if not dj.deleted():
                break
            j -= 1

        # 4) decidir punto de arranque cur/prev
        if j >= 1:
            prev_ptr = dptr(j)
            cur_ptr = self._read(False, j, m).next_ptr
        else:
            # OJO: no sobrescribas 'h'. Lee el head real en variable aparte.
            head_entry = self._read(*loc(h), m)
            if e.key <= head_entry.key:
                # insertar al inicio
                e.next_ptr = h
                self._write(True, idx, e, m)
                self._hdr_set(m, a, newp)
                self._maybe_reorg()
                return
            prev_ptr = 0
            cur_ptr = h

        # 5) avanzar por punteros hasta posición correcta
        while not is_end(cur_ptr):
            a1, i1 = loc(cur_ptr)
            node = self._read(a1, i1, m)
            if node.deleted():
                cur_ptr = node.next_ptr
                continue
            if node.key < e.key:
                prev_ptr = cur_ptr
                cur_ptr = node.next_ptr
            else:
                break

        # 6) enlazar: prev -> new -> cur
        e.next_ptr = cur_ptr
        self._write(True, idx, e, m)

        if prev_ptr == 0:
            h = newp
        else:
            pa, pi = loc(prev_ptr)
            prev = self._read(pa, pi, m)
            prev.next_ptr = newp
            self._write(pa, pi, prev, m)

        self._hdr_set(m, a, h)
        self._maybe_reorg()

    def search(self, key: int) -> List[RID]:
        """Búsqueda exacta: devuelve lista de RIDs con esa key."""
        m, _, h = self._hdr_get()
        if h == 0:
            return []

        # hit directo en D
        lb = self._lb(key)
        if 1 <= lb <= m:
            e = self._read(False, lb, m)
            if not e.deleted() and e.key == key:
                return [e.rid]

        # sucesor del predecesor vivo en D, o head si no hay
        j = min(lb - 1, m)
        while j >= 1:
            dj = self._read(False, j, m)
            if not dj.deleted():
                start = dj.next_ptr
                break
            j -= 1
        else:
            start = h

        out: List[RID] = []
        cur = start
        while not is_end(cur):
            a1, i1 = loc(cur)
            node = self._read(a1, i1, m)
            if node.deleted():
                cur = node.next_ptr
                continue
            if node.key > key:
                break
            if node.key == key:
                out.append(node.rid)
            cur = node.next_ptr
        return out

    def range_search(self, lo: int, hi: int) -> List[SFEntry]:
        """Búsqueda por rango [lo, hi] en orden lógico."""
        if lo > hi:
            lo, hi = hi, lo
        m, _, h = self._hdr_get()
        if h == 0:
            return []

        lb = self._lb(lo)
        j = min(lb - 1, m)
        while j >= 1:
            dj = self._read(False, j, m)
            if not dj.deleted():
                start = dj.next_ptr
                break
            j -= 1
        else:
            start = h

        out: List[SFEntry] = []
        cur = start
        while not is_end(cur):
            a1, i1 = loc(cur)
            node = self._read(a1, i1, m)
            if node.deleted():
                cur = node.next_ptr
                continue
            if node.key > hi:
                break
            if node.key >= lo:
                out.append(node)
            cur = node.next_ptr
        return out

    def delete_key(self, key: int, rid: Optional[RID] = None) -> int:
        """Borra nodos con 'key' (si 'rid' se pasa, borra solo ese). Devuelve cantidad borrada."""
        m, a, h = self._hdr_get()
        if h == 0:
            return 0

        lb = self._lb(key)
        j = min(lb - 1, m)
        while j >= 1:
            dj = self._read(False, j, m)
            if not dj.deleted():
                break
            j -= 1

        prev_ptr = dptr(j) if j >= 1 else 0
        cur_ptr = self._read(False, j, m).next_ptr if j >= 1 else h
        removed = 0

        while not is_end(cur_ptr):
            a1, i1 = loc(cur_ptr)
            node = self._read(a1, i1, m)

            if node.key > key:
                break

            if node.key == key and (rid is None or
                                    (node.rid.page, node.rid.slot) == (rid.page, rid.slot)):
                nxt = node.next_ptr
                # desconectar de la cadena lógica
                if prev_ptr == 0:
                    h = nxt
                else:
                    pa, pi = loc(prev_ptr)
                    prev = self._read(pa, pi, m)
                    prev.next_ptr = nxt
                    self._write(pa, pi, prev, m)
                # marcar tombstone
                node.next_ptr = DELETED
                self._write(a1, i1, node, m)
                removed += 1
                cur_ptr = nxt
                if rid is not None:
                    break
                continue

            prev_ptr = cur_ptr
            cur_ptr = node.next_ptr

        self._hdr_set(m, a, h)
        return removed

    # ----- mantenimiento -----

    def _maybe_reorg(self) -> None:
        """Dispara reorganize cuando A supera ~log2(|D|+1)."""
        m, a, _ = self._hdr_get()
        k = int(math.log2(max(1, m + 1)))
        if a > k:
            self.reorganize()

    def reorganize(self) -> None:
        """Reconstruye D siguiendo la lista lógica (ignorando tombstones)."""
        m, a, h = self._hdr_get()
        if h == 0:
            self._hdr_set(0, 0, 0)
            return

        base = m
        out: List[SFEntry] = []
        cur = h
        seen = 0
        cap = m + a + 8  # cut-off anti-bucle

        # recorre lista lógica
        while not is_end(cur) and seen < cap:
            a1, i1 = loc(cur)
            e = self._read(a1, i1, base)
            if not e.deleted():
                out.append(e)
            cur = e.next_ptr
            seen += 1

        # vuelca ordenado a D, reencadenando d(1)->d(2)->...->END
        newm = len(out)
        with open(self.path, "r+b") as f:
            for i, e in enumerate(out, start=1):
                e.next_ptr = dptr(i + 1) if i < newm else 0
                f.seek(self._off_d(i))
                f.write(e.pack())

        self._hdr_set(newm, 0, dptr(1) if newm >= 1 else 0)
