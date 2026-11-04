"""Extractor MINSAL Farmanet (getLocales.php y getLocalesTurnos.php).

La API devuelve el arreglo completo en una sola respuesta: no tiene paginación ni requiere
autenticación. Por eso cada extracción tiene exactamente una Pagina.
"""
from __future__ import annotations

import time

import requests

from src.extractors.base import Extraccion, Pagina
from src.extractors.http_client import InvalidResponseError, http_get, parse_json
from src.utils.logging_config import get_logger

log = get_logger(__name__)

# Campos sin los cuales un registro no sirve (identificador, nombre y ubicación territorial).
CAMPOS_REQUERIDOS = frozenset({"local_id", "local_nombre", "comuna_nombre", "fk_region"})
# Si MENOS de esta fracción de registros trae los campos requeridos, no son registros "malos":
# es la API la que cambió de formato, y hay que detener la carga.
MIN_FRACCION_ESQUEMA = 0.9


def validar_esquema(data, url: str) -> list[dict]:
    """Valida que la respuesta sea una lista de objetos con los campos mínimos.

    - Lista vacía: es VÁLIDA (puede no haber datos) y se devuelve; quien llama decide.
    - Algunos registros incompletos: válida; esos registros se rechazan en la transformación.
    - Mayoría de registros incompletos (< 90 % completos): cambio de esquema -> error.
    """
    if not isinstance(data, list):
        raise InvalidResponseError(f"Se esperaba una lista JSON desde {url}, llegó {type(data).__name__}")
    if not data:
        return data
    no_dict = sum(1 for r in data if not isinstance(r, dict))
    if no_dict:
        raise InvalidResponseError(f"{no_dict} elementos no son objetos JSON en {url}")
    completos = sum(1 for r in data if CAMPOS_REQUERIDOS <= r.keys())
    if completos / len(data) < MIN_FRACCION_ESQUEMA:
        faltantes = sorted(CAMPOS_REQUERIDOS - set().union(*(r.keys() for r in data)))
        raise InvalidResponseError(
            f"Esquema inesperado en {url}: solo {completos}/{len(data)} registros con campos requeridos; "
            f"campos ausentes en todos: {faltantes}")
    return data


def validar_disponibilidad(url: str, timeout_s: float, session: requests.Session | None = None) -> dict:
    """Tarea de VALIDACIÓN PREVIA (exigida por la pauta): la API responde 200 con JSON esperado.

    Usa solo 2 intentos: si la API está caída, conviene fallar rápido y dejar que Airflow
    reintente la tarea más tarde.
    """
    resp = http_get(url, timeout_s=timeout_s, max_attempts=2, session=session)
    data = validar_esquema(parse_json(resp), url)
    return {"url": url, "http_status": resp.status, "registros": len(data), "elapsed_s": resp.elapsed_s}


def extraer(fuente: str, url: str, timeout_s: float, session: requests.Session | None = None) -> Extraccion:
    inicio = time.monotonic()
    resp = http_get(url, timeout_s=timeout_s, session=session)
    data = validar_esquema(parse_json(resp), url)
    extraccion = Extraccion(
        fuente=fuente,
        url_base=url,
        extraido_en=resp.requested_at,
        paginas=[Pagina(1, url, None, resp.status, resp.content_type, data, resp.sha256, len(data))],
        registros=data,
        intentos_http=resp.attempts,
        duracion_s=time.monotonic() - inicio,
    )
    if not data:
        extraccion.advertencias.append("respuesta_valida_sin_registros")
    log.info("MINSAL %s: %d registros en %.2fs (%d intento/s)", fuente, len(data), extraccion.duracion_s, resp.attempts)
    return extraccion
