"""Cargas a PostgreSQL. Todas las sentencias van parametrizadas (%s) y cada operación es idempotente:
ejecutarla dos veces con los mismos datos deja la base igual que ejecutarla una vez.

Estrategias:
- raw:                       INSERT ... ON CONFLICT DO NOTHING (fuente, fecha, hash de la extracción, página).
- staging MINSAL / dw hecho: reemplazo de la partición lógica de la fecha (DELETE + INSERT) en una transacción.
- staging DPA / dimensiones: UPSERT por clave natural (cut_com, local_id).

Nunca se arman valores con f-strings: con %s los valores viajan separados del SQL, así un texto
de la API con comillas no puede romper ni inyectar la consulta.
"""
from __future__ import annotations

import uuid
from datetime import date

import psycopg
from psycopg.types.json import Jsonb

from src.extractors.base import Extraccion


# ============================================================================= raw
SQL_INSERT_RAW = """
INSERT INTO raw.api_respuesta (fuente, extraccion_id, pagina, total_paginas, endpoint_url, parametros,
    http_status, content_type, extraido_en, fecha_observacion, dag_id, run_id, n_registros,
    pagina_sha256, extraccion_sha256, contenido)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT uq_raw_extraccion DO NOTHING
"""


def guardar_raw(conn: psycopg.Connection, ext: Extraccion, fecha_observacion: date,
                dag_id: str | None, run_id: str | None) -> dict:
    """Guarda cada página tal como llegó. Si la misma extracción ya existe, no duplica y
    devuelve el extraccion_id que ya estaba almacenado."""
    total = len(ext.paginas)
    with conn.transaction(), conn.cursor() as cur:
        insertadas = 0
        for p in ext.paginas:
            cur.execute(SQL_INSERT_RAW, (
                ext.fuente, ext.extraccion_id, p.numero, total, p.url,
                Jsonb(p.parametros) if p.parametros else None, p.http_status, p.content_type,
                ext.extraido_en, fecha_observacion, dag_id, run_id, p.n_registros,
                p.body_sha256, ext.sha256, Jsonb(p.contenido)))
            insertadas += cur.rowcount   # 0 si ON CONFLICT la descartó
        cur.execute("""SELECT extraccion_id FROM raw.api_respuesta
                       WHERE fuente = %s AND fecha_observacion = %s AND extraccion_sha256 = %s LIMIT 1""",
                    (ext.fuente, fecha_observacion, ext.sha256))
        extraccion_id = cur.fetchone()[0]
    return {"extraccion_id": str(extraccion_id), "paginas_insertadas": insertadas,
            "duplicada": insertadas == 0, "registros": ext.n_registros}


def leer_raw_ultima(conn: psycopg.Connection, fuente: str, fecha_observacion: date) -> dict | None:
    """Reconstruye la extracción COMPLETA más reciente de una fuente para una fecha.

    Es la base del reproceso: staging se arma desde raw, no desde la API, así una fecha
    pasada se puede volver a transformar aunque la API ya no tenga esos datos.
    """
    row = conn.execute("""
        SELECT extraccion_id, max(extraido_en) FROM raw.api_respuesta
        WHERE fuente = %s AND fecha_observacion = %s
        GROUP BY extraccion_id
        HAVING count(*) = max(total_paginas)          -- solo extracciones con todas sus páginas
        ORDER BY max(extraido_en) DESC LIMIT 1""", (fuente, fecha_observacion)).fetchone()
    if row is None:
        return None
    extraccion_id, extraido_en = row
    paginas = conn.execute("""SELECT contenido FROM raw.api_respuesta
                              WHERE extraccion_id = %s ORDER BY pagina""", (extraccion_id,)).fetchall()
    return {"extraccion_id": str(extraccion_id), "extraido_en": extraido_en, "paginas": [p[0] for p in paginas]}


def fechas_con_raw(conn: psycopg.Connection, fuente: str, desde: date, hasta: date) -> list[date]:
    """Fechas con datos en raw dentro de un rango (para recargar un rango de fechas pasado)."""
    rows = conn.execute("""SELECT DISTINCT fecha_observacion FROM raw.api_respuesta
                           WHERE fuente = %s AND fecha_observacion BETWEEN %s AND %s ORDER BY 1""",
                        (fuente, desde, hasta)).fetchall()
    return [r[0] for r in rows]


# ============================================================================= rechazos
def guardar_rechazos(cur: psycopg.Cursor, rechazos: list[dict], fecha_observacion: date,
                     extraccion_id: str | None, fuente_default: str) -> int:
    cur.executemany(
        """INSERT INTO staging.rechazo (fuente, fecha_observacion, clave_origen, motivo, registro, extraccion_id)
           VALUES (%s, %s, %s, %s, %s, %s)
           ON CONFLICT ON CONSTRAINT uq_rechazo DO NOTHING""",
        [(r.get("fuente", fuente_default), fecha_observacion, r["clave_origen"], r["motivo"],
          Jsonb(r["registro"]), extraccion_id) for r in rechazos])
    return len(rechazos)


# ============================================================================= staging DPA
def upsert_staging_dpa(conn: psycopg.Connection, filas: list[dict], rechazos: list[dict],
                       fecha_observacion: date, extraccion_id: str, fuente_url: str) -> dict:
    """UPSERT por cut_com: inserta comunas nuevas y actualiza las existentes."""
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO staging.dpa_comuna (cut_com, cut_prov, cut_reg, comuna, provincia, region,
                comuna_normalizada, vigente, fuente_url, extraccion_id, fecha_observacion, actualizado_en)
            VALUES (%(cut_com)s, %(cut_prov)s, %(cut_reg)s, %(comuna)s, %(provincia)s, %(region)s,
                %(comuna_normalizada)s, true, %(url)s, %(eid)s, %(fecha)s, now())
            ON CONFLICT (cut_com) DO UPDATE SET
                cut_prov = EXCLUDED.cut_prov, cut_reg = EXCLUDED.cut_reg, comuna = EXCLUDED.comuna,
                provincia = EXCLUDED.provincia, region = EXCLUDED.region,
                comuna_normalizada = EXCLUDED.comuna_normalizada, vigente = true,
                fuente_url = EXCLUDED.fuente_url, extraccion_id = EXCLUDED.extraccion_id,
                fecha_observacion = EXCLUDED.fecha_observacion, actualizado_en = now()""",
            [{**f, "url": fuente_url, "eid": extraccion_id, "fecha": fecha_observacion} for f in filas])
        # Comunas que la fuente ya no informa: se marcan no vigentes. No se borran (trazabilidad:
        # puede haber hechos históricos que las referencian).
        cur.execute("""UPDATE staging.dpa_comuna SET vigente = false, actualizado_en = now()
                       WHERE vigente AND NOT (cut_com = ANY(%s))""", ([f["cut_com"] for f in filas],))
        no_vigentes = cur.rowcount
        guardar_rechazos(cur, rechazos, fecha_observacion, extraccion_id, "dpa_comunas")
    return {"cargadas": len(filas), "marcadas_no_vigentes": no_vigentes, "rechazadas": len(rechazos)}


def catalogo_dpa(conn: psycopg.Connection) -> list[dict]:
    """Catálogo vigente para el cruce territorial de MINSAL."""
    rows = conn.execute("""SELECT cut_com, cut_reg, comuna_normalizada FROM staging.dpa_comuna
                           WHERE vigente""").fetchall()
    return [{"cut_com": r[0], "cut_reg": r[1], "comuna_normalizada": r[2]} for r in rows]


# ============================================================================= staging MINSAL
_COLS_EST = ("fecha_observacion", "local_id", "nombre", "tipo_establecimiento", "direccion", "telefono",
             "localidad", "comuna_origen", "comuna_normalizada", "fk_region_minsal", "fk_comuna_minsal",
             "latitud", "longitud", "estado_coordenada", "fecha_fuente", "en_listado_general", "en_turno",
             "turno_vigente", "cut_com", "estado_correspondencia", "extraccion_ids")


def reemplazar_staging_minsal(conn: psycopg.Connection, fecha_observacion: date, filas: list[dict],
                              rechazos: list[dict]) -> dict:
    """Reemplaza la fotografía completa de la fecha (DELETE + COPY) en UNA transacción.

    Si algo falla a mitad, el rollback deja la foto anterior intacta: nunca queda la fecha
    vacía ni a medias. Un establecimiento que dejó de aparecer simplemente no está en la foto nueva.
    """
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("DELETE FROM staging.minsal_establecimiento WHERE fecha_observacion = %s", (fecha_observacion,))
        eliminadas = cur.rowcount
        # '%%' es un '%' literal (en psycopg, % se usa para los parámetros).
        cur.execute("DELETE FROM staging.rechazo WHERE fecha_observacion = %s AND fuente LIKE 'minsal%%'",
                    (fecha_observacion,))
        # COPY es la forma más rápida de cargar muchas filas. Los nombres de columna son constantes
        # del código (no datos), por eso aquí sí se puede usar f-string.
        with cur.copy(f"COPY staging.minsal_establecimiento ({', '.join(_COLS_EST)}) FROM STDIN") as copy:
            for f in filas:
                copy.write_row([f[c] if c != "extraccion_ids" else [uuid.UUID(x) for x in f[c]] for c in _COLS_EST])
        guardar_rechazos(cur, rechazos, fecha_observacion, None, "minsal")
    return {"cargadas": len(filas), "reemplazadas": eliminadas, "rechazadas": len(rechazos)}


# ============================================================================= dw
SQL_DIM_TERRITORIO = """
INSERT INTO dw.dim_territorio (cut_com, comuna, cut_prov, provincia, cut_reg, region, vigente, actualizado_en)
SELECT cut_com, comuna, cut_prov, provincia, cut_reg, region, vigente, now() FROM staging.dpa_comuna
ON CONFLICT (cut_com) DO UPDATE SET
    comuna = EXCLUDED.comuna, cut_prov = EXCLUDED.cut_prov, provincia = EXCLUDED.provincia,
    cut_reg = EXCLUDED.cut_reg, region = EXCLUDED.region, vigente = EXCLUDED.vigente, actualizado_en = now()
-- Solo actualiza si algo cambió: una recarga sin cambios no toca filas.
WHERE (dw.dim_territorio.comuna, dw.dim_territorio.provincia, dw.dim_territorio.region, dw.dim_territorio.vigente)
      IS DISTINCT FROM (EXCLUDED.comuna, EXCLUDED.provincia, EXCLUDED.region, EXCLUDED.vigente)
"""

# SCD tipo 1 con guarda temporal: los atributos se sobrescriben solo si el dato es igual o más
# reciente. Así, reprocesar una fecha antigua no pisa una dirección más nueva.
SQL_DIM_ESTABLECIMIENTO = """
INSERT INTO dw.dim_establecimiento (local_id, nombre, tipo_establecimiento, direccion, telefono, localidad,
    latitud, longitud, primera_observacion, ultima_observacion, actualizado_en)
SELECT local_id, nombre, tipo_establecimiento, direccion, telefono, localidad, latitud, longitud,
       fecha_observacion, fecha_observacion, now()
FROM staging.minsal_establecimiento WHERE fecha_observacion = %(fecha)s
ON CONFLICT (local_id) DO UPDATE SET
    nombre = CASE WHEN EXCLUDED.ultima_observacion >= dw.dim_establecimiento.ultima_observacion
                  THEN EXCLUDED.nombre ELSE dw.dim_establecimiento.nombre END,
    tipo_establecimiento = CASE WHEN EXCLUDED.ultima_observacion >= dw.dim_establecimiento.ultima_observacion
                  THEN EXCLUDED.tipo_establecimiento ELSE dw.dim_establecimiento.tipo_establecimiento END,
    direccion = CASE WHEN EXCLUDED.ultima_observacion >= dw.dim_establecimiento.ultima_observacion
                  THEN EXCLUDED.direccion ELSE dw.dim_establecimiento.direccion END,
    telefono = CASE WHEN EXCLUDED.ultima_observacion >= dw.dim_establecimiento.ultima_observacion
                  THEN EXCLUDED.telefono ELSE dw.dim_establecimiento.telefono END,
    localidad = CASE WHEN EXCLUDED.ultima_observacion >= dw.dim_establecimiento.ultima_observacion
                  THEN EXCLUDED.localidad ELSE dw.dim_establecimiento.localidad END,
    latitud = CASE WHEN EXCLUDED.ultima_observacion >= dw.dim_establecimiento.ultima_observacion
                  THEN EXCLUDED.latitud ELSE dw.dim_establecimiento.latitud END,
    longitud = CASE WHEN EXCLUDED.ultima_observacion >= dw.dim_establecimiento.ultima_observacion
                  THEN EXCLUDED.longitud ELSE dw.dim_establecimiento.longitud END,
    primera_observacion = LEAST(dw.dim_establecimiento.primera_observacion, EXCLUDED.primera_observacion),
    ultima_observacion = GREATEST(dw.dim_establecimiento.ultima_observacion, EXCLUDED.ultima_observacion),
    actualizado_en = now()
"""

# LEFT JOIN + coalesce(…, 0): los establecimientos sin comuna van al miembro 0 en vez de perderse.
SQL_FACT = """
INSERT INTO dw.fact_establecimiento_diario (fecha_id, establecimiento_sk, territorio_sk, en_listado_general,
    en_turno, turno_vigente, coordenada_valida, estado_correspondencia)
SELECT df.fecha_id, de.establecimiento_sk, coalesce(dt.territorio_sk, 0), s.en_listado_general, s.en_turno,
       s.turno_vigente, s.estado_coordenada IN ('valida', 'corregida'), s.estado_correspondencia
FROM staging.minsal_establecimiento s
JOIN dw.dim_fecha df ON df.fecha = s.fecha_observacion
JOIN dw.dim_establecimiento de ON de.local_id = s.local_id
LEFT JOIN dw.dim_territorio dt ON dt.cut_com = s.cut_com
WHERE s.fecha_observacion = %(fecha)s
"""

# Control: establecimientos con CUT en staging que NO encontraron su comuna en dim_territorio
# (p. ej. staging.dpa_comuna se actualizó pero cargar_dim_territorio aún no corrió). Sin este
# control quedarían en el miembro 0 en silencio y los indicadores por comuna saldrían mal.
SQL_CUT_SIN_TERRITORIO = """
SELECT count(*), array_agg(DISTINCT s.cut_com)
FROM staging.minsal_establecimiento s
LEFT JOIN dw.dim_territorio dt ON dt.cut_com = s.cut_com
WHERE s.fecha_observacion = %(fecha)s AND s.cut_com IS NOT NULL AND dt.territorio_sk IS NULL
"""


def cargar_dim_territorio(conn: psycopg.Connection) -> dict:
    with conn.transaction():
        actualizadas = conn.execute(SQL_DIM_TERRITORIO).rowcount
        vigentes = conn.execute("SELECT count(*) FROM dw.dim_territorio WHERE vigente").fetchone()[0]
    return {"insertadas_o_actualizadas": actualizadas, "vigentes": vigentes}


def cargar_dw_minsal(conn: psycopg.Connection, fecha_observacion: date) -> dict:
    """Carga dim_establecimiento (UPSERT) y reemplaza los hechos de la fecha, con controles."""
    params = {"fecha": fecha_observacion}
    with conn.transaction():
        n_staging = conn.execute("SELECT count(*) FROM staging.minsal_establecimiento WHERE fecha_observacion = %(fecha)s",
                                 params).fetchone()[0]
        if n_staging == 0:
            raise ValueError(f"No hay registros en staging para {fecha_observacion}")
        n_sin_territorio, cuts = conn.execute(SQL_CUT_SIN_TERRITORIO, params).fetchone()
        if n_sin_territorio:
            raise ValueError(f"{n_sin_territorio} establecimientos tienen CUT sin fila en dw.dim_territorio "
                             f"({cuts[:10]}); ejecutar etl_comunas hasta cargar_dim_territorio")
        dim = conn.execute(SQL_DIM_ESTABLECIMIENTO, params).rowcount
        eliminadas = conn.execute("DELETE FROM dw.fact_establecimiento_diario WHERE fecha_id = %s",
                                  (int(fecha_observacion.strftime("%Y%m%d")),)).rowcount
        hechos = conn.execute(SQL_FACT, params).rowcount
        # Control: cada fila de staging debe producir exactamente un hecho (p. ej. falla si la
        # fecha queda fuera del rango de dim_fecha y el JOIN la descarta).
        if hechos != n_staging:
            raise ValueError(f"Hechos cargados ({hechos}) no coinciden con staging ({n_staging})")
    return {"staging": n_staging, "dim_establecimiento_upsert": dim, "hechos_reemplazados": eliminadas,
            "hechos_cargados": hechos}
