# Demo E2E: crea heap binario, índice secuencial (por id) y corre operaciones.

import os
from core.storage.heap.heapfile import HeapFile
from core.storage.seqfile.index import SequentialFileIndex
from core.engine.executor import Executor

BASE = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(BASE, exist_ok=True)

HEAP_PATH = os.path.join(BASE, "empleados.heap")
IDX_PATH  = os.path.join(BASE, "empleados_id.sf")

for p in (HEAP_PATH, IDX_PATH):
    if os.path.exists(p): os.remove(p)  # limpia archivos previos

schema = [("id","INT"), ("nombre","VARCHAR(50)"), ("salario","FLOAT"), ("ingreso","DATE")]
heap = HeapFile(HEAP_PATH, schema)
idx  = SequentialFileIndex(IDX_PATH, key_col="id")
exe  = Executor(heap, [idx])

# INSERTS
exe.insert({"id": 10, "nombre": "Ana",  "salario": 1200.5, "ingreso": "2024-01-01"})
exe.insert({"id": 15, "nombre": "Luis", "salario": 2000.0, "ingreso": "2024-02-10"})
exe.insert({"id": 12, "nombre": "Zoe",  "salario": 1500.0, "ingreso": "2023-12-15"})
exe.insert({"id": 15, "nombre": "Luis2","salario": 2100.0, "ingreso": "2024-03-20"})

# SELECT =
print("id=15:", exe.select_eq("id", 15))
print("id=11:", exe.select_eq("id", 11))

# BETWEEN
print("id in [11..14]:", exe.select_between("id", 11, 14))

# DELETE
print("delete id=12:", exe.delete("id", 12))
print("after delete 10..20:", exe.select_between("id", 10, 20))

# Full-scan sin índice (prueba quitando idx de Executor):
# exe_noidx = Executor(heap, [])
# print(list(exe_noidx.heap.scan_eq("id", 10)))
