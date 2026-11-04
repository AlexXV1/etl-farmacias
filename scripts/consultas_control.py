"""Ejecuta sql/08_consultas_control.sql y guarda los resultados como evidencia del informe.

Cubre lo que pide la pauta desde la tabla de auditoría: registros cargados por tabla,
% de registros con errores o nulos y tiempo de ejecución de cada DAG.
Uso:  .\\.venv\\Scripts\\python.exe scripts\\consultas_control.py   -> evidence/consultas_control_<ts>.txt
"""
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.database.connection import params_from_env  # noqa: E402


def bloques(sql: str):
    """Separa el archivo en (título, sentencia): los comentarios '-- N. …' son los títulos."""
    titulo, actual = "", []
    for linea in sql.splitlines():
        if linea.startswith("-- ") and linea[3:4].isdigit():
            titulo = linea[3:]
        elif linea.startswith("--"):
            continue
        else:
            actual.append(linea)
            if linea.rstrip().endswith(";"):
                yield titulo, "\n".join(actual).strip()
                actual = []


def tabla(cols, rows) -> str:
    """Tabla de texto con columnas alineadas (ancho máximo 60 por columna)."""
    textos = [[("" if v is None else str(v)) for v in r] for r in rows]
    anchos = [min(60, max([len(c)] + [len(r[i]) for r in textos])) for i, c in enumerate(cols)]
    fmt = " | ".join(f"{{:{a}.{a}}}" for a in anchos)
    return "\n".join([fmt.format(*cols), "-+-".join("-" * a for a in anchos)] + [fmt.format(*r) for r in textos])


def main():
    sql = (ROOT / "sql" / "08_consultas_control.sql").read_text(encoding="utf-8")
    salida = [f"Consultas de control — {datetime.now().isoformat(timespec='seconds')}\n"]
    with params_from_env().connect() as conn:
        for titulo, sentencia in bloques(sql):
            cur = conn.execute(sentencia)
            salida.append(f"\n### {titulo}\n{tabla([d.name for d in cur.description], cur.fetchall())}\n")
    texto = "\n".join(salida)
    out = ROOT / "evidence" / f"consultas_control_{datetime.now():%Y%m%d_%H%M%S}.txt"
    out.write_text(texto, encoding="utf-8")
    print(texto)
    print(f"\nGuardado en {out}")


if __name__ == "__main__":
    main()
