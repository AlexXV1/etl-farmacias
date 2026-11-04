"""Resultados de los indicadores para el informe (desde el esquema mart).

Uso:  .\\.venv\\Scripts\\python.exe scripts\\resultados.py [AAAA-MM-DD]   -> evidence/resultados_<fecha>_<ts>.txt
      Sin fecha usa la última con indicadores calculados.
"""
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.consultas_control import tabla  # noqa: E402
from src.database.connection import params_from_env  # noqa: E402

CONSULTAS = {
    "Indicadores": """
        SELECT codigo, nombre, numerador, denominador, valor, meta, cumple_meta
        FROM mart.v_indicadores WHERE fecha = %(f)s ORDER BY codigo""",
    "Distribución regional (IND03)": """
        SELECT cut_reg, region, n_establecimientos, pct_del_total_nacional AS pct_nacional, comunas_total,
               comunas_con_establecimiento AS comunas_cubiertas, pct_cobertura_comunal AS pct_cobertura,
               comunas_con_turno_vigente AS comunas_turno_vigente
        FROM mart.establecimientos_region r JOIN dw.dim_fecha d USING (fecha_id)
        WHERE d.fecha = %(f)s ORDER BY cut_reg""",
    "Top 10 comunas (IND02)": """
        SELECT ranking_nacional, comuna, region, n_establecimientos, n_turno_vigente
        FROM mart.establecimientos_comuna c JOIN dw.dim_fecha d USING (fecha_id)
        WHERE d.fecha = %(f)s ORDER BY n_establecimientos DESC, comuna LIMIT 10""",
    "Distribución de comunas según cantidad de establecimientos": """
        SELECT CASE WHEN n_establecimientos = 0 THEN '0' WHEN n_establecimientos = 1 THEN '1'
                    WHEN n_establecimientos <= 5 THEN '2-5' WHEN n_establecimientos <= 20 THEN '6-20'
                    ELSE '>20' END AS tramo, count(*) AS comunas, sum(n_establecimientos) AS establecimientos
        FROM mart.establecimientos_comuna c JOIN dw.dim_fecha d USING (fecha_id)
        WHERE d.fecha = %(f)s GROUP BY 1 ORDER BY min(n_establecimientos)""",
    "Comunas sin establecimientos registrados, por región (IND01)": """
        SELECT region, count(*) AS comunas_sin_registro, string_agg(comuna, ', ' ORDER BY comuna) AS comunas
        FROM mart.establecimientos_comuna c JOIN dw.dim_fecha d USING (fecha_id)
        WHERE d.fecha = %(f)s AND n_establecimientos = 0 GROUP BY region ORDER BY count(*) DESC""",
    "Tipo de establecimiento (inferido por nombre)": """
        SELECT e.tipo_establecimiento, count(*) FROM dw.fact_establecimiento_diario f
        JOIN dw.dim_establecimiento e USING (establecimiento_sk) JOIN dw.dim_fecha d USING (fecha_id)
        WHERE d.fecha = %(f)s GROUP BY 1 ORDER BY 2 DESC""",
    "Evolución de los indicadores (todas las fechas)": """
        SELECT fecha, codigo, valor, cumple_meta FROM mart.v_indicadores ORDER BY codigo, fecha""",
}


def main():
    with params_from_env().connect() as conn:
        f = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else conn.execute(
            "SELECT max(fecha) FROM mart.v_indicadores").fetchone()[0]
        dia = conn.execute("SELECT nombre_dia FROM dw.dim_fecha WHERE fecha = %s", (f,)).fetchone()[0]
        partes = [f"Resultados mart — fecha de observación {f} ({dia}) — generado {datetime.now():%Y-%m-%d %H:%M}"]
        for titulo, sql in CONSULTAS.items():
            cur = conn.execute(sql, {"f": f})
            partes.append(f"\n### {titulo}\n{tabla([d.name for d in cur.description], cur.fetchall())}")
    texto = "\n".join(partes)
    out = ROOT / "evidence" / f"resultados_{f}_{datetime.now():%Y%m%d_%H%M%S}.txt"
    out.write_text(texto, encoding="utf-8")
    print(texto, f"\n\nGuardado en {out}", sep="")


if __name__ == "__main__":
    main()
