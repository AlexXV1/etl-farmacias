"""Exporta a mano la evidencia de UNA fecha a evidence/AAAA-MM-DD/ (CSV + resumen.md).

La misma exportación la hace sola, cada día, la tarea exportar_evidencia del DAG calcular_indicadores.
La lógica vive en src/pipelines/evidencia.py (este script solo la llama).

Uso:  .\\.venv\\Scripts\\python.exe scripts\\exportar_evidencia.py 2026-11-01
      .\\.venv\\Scripts\\python.exe scripts\\exportar_evidencia.py          (última fecha cargada)
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.database.connection import params_from_env  # noqa: E402
from src.pipelines import evidencia, indicadores  # noqa: E402


def main() -> None:
    with params_from_env().connect() as conn:
        fecha = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else indicadores.fecha_a_calcular(conn, None)
        r = evidencia.exportar(conn, fecha)
    for nombre, n in r["filas"].items():
        print(f"{nombre:26s} {n:>5} filas")
    print(f"\nEvidencia en {r['carpeta']}")


if __name__ == "__main__":
    main()
