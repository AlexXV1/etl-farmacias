"""Exporta la evidencia de UNA fecha de observación a evidence/AAAA-MM-DD/ (CSV + resumen.md).

La usan el DAG calcular_indicadores (tarea exportar_evidencia, automática cada día) y
scripts/exportar_evidencia.py (a mano). Los CSV usan ';' y UTF-8 con BOM para que Excel en
español los abra bien. Si la fecha se recalcula, sus archivos se sobrescriben con la versión nueva.
"""
from __future__ import annotations

import csv
from datetime import date, datetime
from pathlib import Path

import psycopg

from src.config.settings import PROJECT_ROOT, ZONA_HORARIA

# En el contenedor PROJECT_ROOT es /opt/airflow/project y ./evidence está montado ahí (docker-compose.yaml).
EVIDENCE_DIR = PROJECT_ROOT / "evidence"

# Nombre de archivo -> SQL. Cada consulta recibe %(fecha)s (date) y %(fid)s (AAAAMMDD).
CONSULTAS = {
    "indicadores": """
        SELECT codigo, nombre, numerador, denominador, valor, unidad, meta, cumple_meta
        FROM mart.v_indicadores WHERE fecha = %(fecha)s ORDER BY codigo""",
    "establecimientos_region": """
        SELECT cut_reg, region, n_establecimientos, pct_del_total_nacional, comunas_total,
               comunas_con_establecimiento, pct_cobertura_comunal, comunas_con_turno_vigente
        FROM mart.establecimientos_region WHERE fecha_id = %(fid)s ORDER BY cut_reg""",
    "establecimientos_comuna": """
        SELECT cut_com, comuna, cut_reg, region, n_establecimientos, n_listado_general,
               n_turno, n_turno_vigente, ranking_nacional
        FROM mart.establecimientos_comuna WHERE fecha_id = %(fid)s ORDER BY ranking_nacional, cut_com""",
    "farmacias_de_turno": """
        SELECT nombre, comuna_origen AS comuna, direccion, cut_com,
               CASE WHEN turno_vigente THEN 'vigente' ELSE 'fecha_antigua' END AS turno, fecha_fuente
        FROM staging.minsal_establecimiento
        WHERE fecha_observacion = %(fecha)s AND en_turno ORDER BY turno DESC, comuna, nombre""",
    "calidad": """
        SELECT 'correspondencia' AS dimension, estado_correspondencia AS estado, count(*) AS establecimientos
        FROM staging.minsal_establecimiento WHERE fecha_observacion = %(fecha)s GROUP BY 2
        UNION ALL
        SELECT 'coordenada', estado_coordenada, count(*)
        FROM staging.minsal_establecimiento WHERE fecha_observacion = %(fecha)s GROUP BY 2
        UNION ALL
        SELECT 'tipo', tipo_establecimiento, count(*)
        FROM staging.minsal_establecimiento WHERE fecha_observacion = %(fecha)s GROUP BY 2
        ORDER BY 1, 3 DESC""",
    "sin_correspondencia": """
        SELECT local_id, nombre, comuna_origen, fk_region_minsal, estado_correspondencia
        FROM staging.minsal_establecimiento
        WHERE fecha_observacion = %(fecha)s AND cut_com IS NULL ORDER BY comuna_origen, local_id""",
    "extracciones_raw": """
        SELECT fuente, to_char(extraido_en AT TIME ZONE 'America/Santiago', 'YYYY-MM-DD HH24:MI:SS') AS extraido_chile,
               count(*) AS paginas, sum(n_registros) AS registros, min(http_status) AS http_status,
               left(min(extraccion_sha256), 16) AS sha256, min(dag_id) AS dag_id
        FROM raw.api_respuesta WHERE fecha_observacion = %(fecha)s
        GROUP BY fuente, extraido_en ORDER BY fuente, extraido_en""",
    "auditoria": """
        SELECT dag_id, run_id, task_id, intento, etapa, fuente, estado, filas_leidas, filas_cargadas,
               filas_rechazadas, pct_nulos, duracion_s, left(mensaje, 200) AS mensaje,
               to_char(inicio AT TIME ZONE 'America/Santiago', 'YYYY-MM-DD HH24:MI:SS') AS inicio_chile
        FROM audit.etl_audit WHERE fecha_observacion = %(fecha)s ORDER BY inicio""",
}


def _escribir_csv(ruta: Path, columnas: list[str], filas: list[tuple]) -> None:
    with ruta.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(columnas)
        w.writerows(filas)


def exportar(conn: psycopg.Connection, fecha: date, base: Path = EVIDENCE_DIR) -> dict:
    """Escribe base/AAAA-MM-DD/*.csv y resumen.md. Devuelve la carpeta y las filas por archivo."""
    params = {"fecha": fecha, "fid": int(fecha.strftime("%Y%m%d"))}
    destino = base / fecha.isoformat()
    destino.mkdir(parents=True, exist_ok=True)

    resultados = {}
    for nombre, sql in CONSULTAS.items():
        cur = conn.execute(sql, params)
        columnas = [c.name for c in cur.description]
        filas = cur.fetchall()
        _escribir_csv(destino / f"{nombre}.csv", columnas, filas)
        resultados[nombre] = (columnas, filas)

    dia = conn.execute("SELECT nombre_dia FROM dw.dim_fecha WHERE fecha = %s", (fecha,)).fetchone()[0]
    hechos, turnos, vigentes, comunas = conn.execute("""
        SELECT count(*), count(*) FILTER (WHERE en_turno), count(*) FILTER (WHERE turno_vigente),
               count(DISTINCT territorio_sk) FILTER (WHERE territorio_sk <> 0)
        FROM dw.fact_establecimiento_diario WHERE fecha_id = %(fid)s""", params).fetchone()

    lineas = [
        f"# Evidencia del {fecha.isoformat()} ({dia})",
        "",
        f"Generado el {datetime.now(ZONA_HORARIA):%Y-%m-%d %H:%M} (hora de Chile) con `src/pipelines/evidencia.py`.",
        "",
        "## Carga del día",
        "",
        f"- Establecimientos observados (hechos): **{hechos}**",
        f"- Turnos publicados: **{turnos}**; turnos vigentes (fecha del día): **{vigentes}**",
        f"- Comunas con al menos un establecimiento: **{comunas}**",
        f"- Establecimientos sin comuna asignada: **{len(resultados['sin_correspondencia'][1])}**",
        "",
        "## Extracciones guardadas en raw",
        "",
        "| Fuente | Extraído (Chile) | Páginas | Registros |",
        "|---|---|---|---|",
        *[f"| {r[0]} | {r[1]} | {r[2]} | {r[3]} |" for r in resultados["extracciones_raw"][1]],
        "",
        "## Indicadores",
        "",
        "| Código | Indicador | Numerador | Denominador | Valor (%) | Meta | ¿Cumple? |",
        "|---|---|---|---|---|---|---|",
        *[f"| {r[0]} | {r[1]} | {r[2]:g} | {r[3]:g} | {r[4]} | {r[6]} | {'sí' if r[7] else 'no'} |"
          for r in resultados["indicadores"][1]],
        "",
        "## Archivos",
        "",
        *[f"- `{n}.csv` ({len(resultados[n][1])} filas)" for n in CONSULTAS],
        "",
    ]
    (destino / "resumen.md").write_text("\n".join(lineas), encoding="utf-8")
    return {"carpeta": str(destino), "filas": {n: len(r[1]) for n, r in resultados.items()}}
