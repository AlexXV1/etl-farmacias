"""Pasos del ETL DPA SUBDERE (catálogo de comunas).

Cada función recibe una conexión y devuelve métricas. No conocen Airflow: las usan tanto los
DAGs (paso 15) como scripts/ejecutar_local.py.
"""
from __future__ import annotations

from datetime import date

import psycopg

from src.config.settings import Settings
from src.extractors import comunas_extractor
from src.extractors.http_client import ExtractionError
from src.loaders import postgres_loader as loader
from src.transformers import comunas_transformer
from src.utils.logging_config import get_logger

log = get_logger(__name__)

FUENTE = "dpa_comunas"


def urls(settings: Settings) -> list[str]:
    """Principal (MOP) y respaldo (Subpesca), en ese orden."""
    return [settings.dpa_comunas_url, settings.dpa_comunas_url_respaldo]


def validar_api(settings: Settings) -> dict:
    """Valida la URL principal; si falla, valida el respaldo y lo informa. Falla solo si ambas fallan."""
    try:
        return {**comunas_extractor.validar_disponibilidad(settings.dpa_comunas_url, settings.http_timeout_s),
                "usa_respaldo": False}
    except ExtractionError as exc:
        log.warning("Servicio DPA principal no disponible (%s). Se valida el respaldo.", exc)
        resultado = comunas_extractor.validar_disponibilidad(settings.dpa_comunas_url_respaldo, settings.http_timeout_s)
        return {**resultado, "usa_respaldo": True, "error_principal": str(exc)}


def extraer_a_raw(conn: psycopg.Connection, settings: Settings, fecha: date,
                  dag_id: str | None, run_id: str | None) -> dict:
    """API -> raw.api_respuesta (con respaldo automático)."""
    ext = comunas_extractor.extraer_con_respaldo(urls(settings), settings.dpa_page_size, settings.http_timeout_s)
    return {**loader.guardar_raw(conn, ext, fecha, dag_id, run_id), **ext.metricas()}


def transformar_a_staging(conn: psycopg.Connection, fecha: date) -> dict:
    """raw (última extracción completa de la fecha) -> transformación -> staging.dpa_comuna."""
    raw = loader.leer_raw_ultima(conn, FUENTE, fecha)
    if raw is None:
        raise ValueError(f"No hay extracción DPA en raw para {fecha}")
    registros = [comunas_extractor.normalizar_atributos(f.get("attributes") or {})
                 for pagina in raw["paginas"] for f in pagina.get("features", [])]
    url = conn.execute("SELECT endpoint_url FROM raw.api_respuesta WHERE extraccion_id = %s LIMIT 1",
                       (raw["extraccion_id"],)).fetchone()[0]
    res = comunas_transformer.transformar(registros)
    if len(res.filas) == 0:
        # Mejor conservar el catálogo anterior que dejar todas las comunas como no vigentes.
        raise ValueError("La transformación DPA no produjo comunas válidas; no se actualiza el catálogo")
    carga = loader.upsert_staging_dpa(conn, res.filas, res.rechazos, fecha, raw["extraccion_id"],
                                      url.removesuffix("/query"))
    return {"carga": carga, "metricas": res.metricas(len(registros)), "leidos": len(registros)}


def cargar_dim_territorio(conn: psycopg.Connection) -> dict:
    """staging.dpa_comuna -> dw.dim_territorio."""
    return loader.cargar_dim_territorio(conn)
