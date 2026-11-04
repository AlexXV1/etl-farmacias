"""Verificación de dependencias, cálculo SQL de indicadores y controles de consistencia.

El cálculo vive en SQL (mart.calcular_indicadores, sql/06_indicadores.sql, paso 16); aquí solo se
decide si se puede calcular, se invoca la función y se validan los resultados.
"""
from __future__ import annotations

from datetime import date

import psycopg

from src.audit.auditoria import EtapaOmitida
from src.pipelines import minsal

# Vigencia máxima del catálogo DPA respecto de la fecha a calcular (schedule mensual + holgura).
MAX_DIAS_CATALOGO = 45


def fecha_a_calcular(conn: psycopg.Connection, fecha: date | None) -> date:
    """La fecha pedida o, si no se indica, la última fecha con hechos cargados."""
    if fecha is not None:
        return fecha
    row = conn.execute("""SELECT max(d.fecha) FROM dw.fact_establecimiento_diario f
                          JOIN dw.dim_fecha d USING (fecha_id)""").fetchone()
    if row[0] is None:
        raise EtapaOmitida("No hay hechos MINSAL cargados todavía")
    return row[0]


def verificar_dependencias(conn: psycopg.Connection, fecha: date) -> dict:
    """Comprueba que AMBAS fuentes estén cargadas y vigentes para `fecha`.

    Si la fotografía MINSAL se construyó antes de que existiera el catálogo DPA, se reprocesa
    desde raw (idempotente) para asignar las comunas antes de calcular.
    """
    fecha_id = int(fecha.strftime("%Y%m%d"))
    hechos = conn.execute("SELECT count(*) FROM dw.fact_establecimiento_diario WHERE fecha_id = %s",
                          (fecha_id,)).fetchone()[0]
    if hechos == 0:
        raise EtapaOmitida(f"Sin hechos MINSAL para {fecha}")
    cat = conn.execute("SELECT count(*), max(fecha_observacion) FROM staging.dpa_comuna WHERE vigente").fetchone()
    dim = conn.execute("SELECT count(*) FROM dw.dim_territorio WHERE vigente").fetchone()[0]
    if cat[0] == 0 or dim == 0:
        raise EtapaOmitida("Catálogo DPA no cargado: los indicadores territoriales no pueden calcularse")
    antiguedad = (fecha - cat[1]).days
    if antiguedad > MAX_DIAS_CATALOGO:
        raise EtapaOmitida(f"Catálogo DPA desactualizado ({antiguedad} días > {MAX_DIAS_CATALOGO})")

    sin_catalogo = conn.execute("""SELECT count(*) FROM staging.minsal_establecimiento
                                   WHERE fecha_observacion = %s AND estado_correspondencia = 'catalogo_no_disponible'""",
                                (fecha,)).fetchone()[0]
    reproceso = None
    if sin_catalogo:
        reproceso = {"staging": minsal.transformar_a_staging(conn, fecha)["carga"],
                     "dw": minsal.cargar_dw(conn, fecha)}
    return {"fecha": fecha.isoformat(), "hechos": hechos, "comunas_vigentes": dim,
            "antiguedad_catalogo_dias": antiguedad, "reprocesado_por_catalogo": reproceso}


def calcular(conn: psycopg.Connection, fecha: date) -> list[dict]:
    """Ejecuta mart.calcular_indicadores(fecha) en una transacción (idempotente: reemplaza la fecha)."""
    with conn.transaction():
        rows = conn.execute("SELECT * FROM mart.calcular_indicadores(%s)", (fecha,)).fetchall()
    return [{"codigo": r[0], "numerador": float(r[1]), "denominador": float(r[2]),
             "valor": float(r[3]), "cumple_meta": r[4]} for r in rows]


# Controles de consistencia: cada consulta devuelve 0 filas si todo está correcto.
CONTROLES = {
    "suma_comunas_mas_sin_correspondencia_igual_hechos": """
        SELECT 1 WHERE (SELECT coalesce(sum(n_establecimientos), 0) FROM mart.establecimientos_comuna WHERE fecha_id = %(f)s)
                     + (SELECT count(*) FROM dw.fact_establecimiento_diario WHERE fecha_id = %(f)s AND territorio_sk = 0)
                    <> (SELECT count(*) FROM dw.fact_establecimiento_diario WHERE fecha_id = %(f)s)""",
    "hechos_igual_staging": """
        SELECT 1 WHERE (SELECT count(*) FROM dw.fact_establecimiento_diario WHERE fecha_id = %(f)s)
                    <> (SELECT count(*) FROM staging.minsal_establecimiento WHERE fecha_observacion = to_date(%(f)s::text, 'YYYYMMDD'))""",
    "regiones_suman_total_comunas": """
        SELECT 1 WHERE (SELECT sum(comunas_total) FROM mart.establecimientos_region WHERE fecha_id = %(f)s)
                    <> (SELECT count(*) FROM dw.dim_territorio WHERE vigente)""",
    "cinco_indicadores_calculados": """
        SELECT 1 WHERE (SELECT count(*) FROM mart.indicador_valor WHERE fecha_id = %(f)s) <> 5""",
    "porcentajes_en_rango": """
        SELECT codigo FROM mart.indicador_valor WHERE fecha_id = %(f)s AND (valor < 0 OR valor > 100)""",
}


def validar_resultados(conn: psycopg.Connection, fecha: date) -> dict:
    f = int(fecha.strftime("%Y%m%d"))
    fallidos = [nombre for nombre, sql in CONTROLES.items() if conn.execute(sql, {"f": f}).fetchall()]
    if fallidos:
        raise ValueError(f"Controles de consistencia fallidos: {fallidos}")
    return {"controles_ok": list(CONTROLES)}
