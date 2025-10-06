# Serializador binario por schema: INT, FLOAT, VARCHAR(n), DATE ("YYYY-MM-DD")
# Fila: [ncols:uint16][nullmap:ceil(n/8)][campos...]; varlen: [len:uint16][bytes]

import struct
from typing import Dict, Any, List, Tuple

def _nullmap_size(n: int) -> int:  # bytes de bitmap de NULLs
    return (n + 7) // 8

def _set_null(bm: bytearray, i: int) -> None:  # marca NULL en bit i
    bm[i // 8] |= (1 << (i % 8))

def _is_null(bm: bytes, i: int) -> bool:  # consulta bit i
    return (bm[i // 8] >> (i % 8)) & 1 == 1

def _varchar_max(typ: str) -> int:  # extrae N en VARCHAR(N)
    try:
        i, j = typ.index("("), typ.index(")")
        return max(0, min(65535, int(typ[i+1:j])))
    except:
        return 65535

def pack_row(row: Dict[str, Any], schema: List[Tuple[str, str]]) -> bytes:
    n = len(schema)                                 # número de columnas
    header = struct.pack("<H", n)                   # ncols
    nullmap = bytearray(_nullmap_size(n))           # mapa de NULLs
    parts: List[bytes] = []                         # payload por columna

    for i, (name, typ) in enumerate(schema):
        val = row.get(name, None)                   # toma valor o None
        if val is None:
            _set_null(nullmap, i)                   # marca NULL
            continue
        if typ == "INT":
            parts.append(struct.pack("<i", int(val)))
        elif typ == "FLOAT":
            parts.append(struct.pack("<d", float(val)))
        elif typ.startswith("VARCHAR"):
            b = str(val).encode("utf-8")
            mx = _varchar_max(typ)
            if len(b) > mx: b = b[:mx]
            parts.append(struct.pack("<H", len(b)) + b)
        elif typ == "DATE":
            b = str(val).encode("utf-8")           # "YYYY-MM-DD"
            if len(b) > 255: raise ValueError("DATE demasiado largo")
            parts.append(struct.pack("<H", len(b)) + b)
        else:
            raise ValueError(f"Tipo no soportado: {typ}")

    return header + bytes(nullmap) + b"".join(parts)

def unpack_row(buf: bytes, schema: List[Tuple[str, str]]) -> Dict[str, Any]:
    n = struct.unpack_from("<H", buf, 0)[0]         # lee ncols
    if n != len(schema): raise ValueError("Schema mismatch")
    off = 2
    bm_size = _nullmap_size(n)
    bm = buf[off:off+bm_size]; off += bm_size       # lee nullmap

    out: Dict[str, Any] = {}
    for i, (name, typ) in enumerate(schema):
        if _is_null(bm, i):
            out[name] = None
            continue
        if typ == "INT":
            out[name] = struct.unpack_from("<i", buf, off)[0]; off += 4
        elif typ == "FLOAT":
            out[name] = struct.unpack_from("<d", buf, off)[0]; off += 8
        elif typ.startswith("VARCHAR") or typ == "DATE":
            ln = struct.unpack_from("<H", buf, off)[0]; off += 2
            out[name] = buf[off:off+ln].decode("utf-8"); off += ln
        else:
            raise ValueError(f"Tipo no soportado: {typ}")
    return out
