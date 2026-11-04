"""Pasos del ETL MINSAL Farmanet. Cada función recibe una conexión y devuelve métricas."""
from __future__ import annotations

from datetime import date

import psycopg

from src.audit.auditoria import EtapaOmitida
from src.config.settings import Settings
from src.extractors import minsal_extractor
from src.loaders import postgres_loader as loader
from src.transformers import minsal_transformer
from src.utils.validation import perfil_nulos

FUENTES = ("minsal_locales", "minsal_turnos")
# Campos opcionales cuya ausencia afecta el análisis: van al perfil de nulos de la auditoría.
CAMPOS_CALIDAD = ("direccion", "telefono", "latitud", "longitud", "cut_com", "fecha_fuente")


def url_de(settings: Settings, fuente: str) -> str:
    return {"minsal_locales": settings.minsal_locales_url, "minsal_turnos": settings.minsal_turnos_url}[fuente]


def validar_api(settings: Settings) -> list[dict]:
    """Validación previa de los dos endpoints."""
    return [minsal_extractor.validar_disponibilidad(url_de(settings, f), settings.http_timeout_s) for f in FUENTES]


def extraer_a_raw(conn: psycopg.Connection, settings: Settings, fuente: str, fecha: date,
                  dag_id: str | None, run_id: str | None) -> dict:
    """API -> raw.api_respuesta, para un endpoint."""
    ext = minsal_extractor.extraer(fuente, url_de(settings, fuente), settings.http_timeout_s)
    if fuente == "minsal_locales" and ext.n_registros == 0:
        # Respuesta válida pero vacía: se guarda en raw como evidencia y se detiene, porque un
        # listado nacional vacío no es plausible (no se sobrescribe la fotografía del día).
        loader.guardar_raw(conn, ext, fecha, dag_id, run_id)
        raise ValueError("getLocales respondió una lista vacía; se guardó en raw y no se continúa")
    return {**loader.guardar_raw(conn, ext, fecha, dag_id, run_id), **ext.metricas()}


def transformar_a_staging(conn: psycopg.Connection, fecha: date) -> dict:
    """Lee la última extracción raw de cada endpoint para `fecha`, transforma y reemplaza staging.

    Como lee desde raw (no desde la API), sirve igual para la carga del día y para reprocesar
    una fecha pasada.
    """
    raws = {f: loader.leer_raw_ultima(conn, f, fecha) for f in FUENTES}
    faltantes = [f for f, r in raws.items() if r is None]
    if faltantes:
        raise EtapaOmitida(f"No hay datos raw de {faltantes} para {fecha}; no se puede construir la fotografía")
    registros = {f: [reg for pagina in r["paginas"] for reg in pagina] for f, r in raws.items()}
    catalogo = loader.catalogo_dpa(conn)
    res = minsal_transformer.transformar(registros["minsal_locales"], registros["minsal_turnos"], catalogo,
                                         fecha, [raws[f]["extraccion_id"] for f in FUENTES])
    carga = loader.reemplazar_staging_minsal(conn, fecha, res.filas, res.rechazos)
    calidad = perfil_nulos(res.filas, CAMPOS_CALIDAD)
    return {"carga": carga, "metricas": res.metricas, "calidad": calidad,
            "leidos": len(registros["minsal_locales"]) + len(registros["minsal_turnos"]),
            "catalogo_dpa_disponible": bool(catalogo)}


def cargar_dw(conn: psycopg.Connection, fecha: date) -> dict:
    """staging -> dim_establecimiento + fact_establecimiento_diario de la fecha."""
    return loader.cargar_dw_minsal(conn, fecha)
