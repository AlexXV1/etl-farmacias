"""Conteo de filas por tabla de cada capa (control de idempotencia y evidencia de carga).

Uso:  .\\.venv\\Scripts\\python.exe scripts\\conteos.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.database.connection import params_from_env  # noqa: E402

TABLAS = ("raw.api_respuesta", "staging.dpa_comuna", "staging.minsal_establecimiento", "staging.rechazo",
          "dw.dim_fecha", "dw.dim_territorio", "dw.dim_establecimiento", "dw.fact_establecimiento_diario",
          "mart.establecimientos_comuna", "mart.establecimientos_region", "mart.indicador",
          "mart.indicador_valor", "audit.etl_audit")


def main():
    with params_from_env().connect() as conn:
        for t in TABLAS:
            # f-string seguro: los nombres de tabla son constantes del código, no datos externos.
            n = conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            print(f"{t:38s} {n:>8}")


if __name__ == "__main__":
    main()
