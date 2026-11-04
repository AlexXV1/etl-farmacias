"""Cliente HTTP común: timeouts, reintentos con espera exponencial y manejo de límites (429).

Hay DOS niveles de reintento, complementarios:
- Este módulo reintenta fallas transitorias dentro de una misma tarea (segundos).
- Airflow reintenta la tarea completa (minutos) si la API sigue caída.
"""
from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

from src.utils.logging_config import get_logger

log = get_logger(__name__)

USER_AGENT = "INF522-ETL-Farmacias/1.0 (proyecto academico UCM)"
# 429 = demasiadas solicitudes (límite de uso); 5xx = error temporal del servidor.
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
# Tope para Retry-After: un servidor podría pedir esperar horas y bloquear la tarea.
MAX_RETRY_AFTER_S = 60.0


# --- Excepciones: una jerarquía permite capturar "cualquier error de extracción" o uno específico.
class ExtractionError(Exception):
    """Error de extracción (base)."""


class HttpStatusError(ExtractionError):
    """La API respondió con un código HTTP distinto de 200."""

    def __init__(self, url: str, status: int, detail: str = ""):
        super().__init__(f"HTTP {status} en {url} {detail}".strip())
        self.url, self.status = url, status


class ConnectionFailedError(ExtractionError):
    """No fue posible conectarse (DNS, TLS, timeout) tras todos los intentos."""


class InvalidResponseError(ExtractionError):
    """La respuesta llegó, pero no tiene el formato o el esquema esperado."""


@dataclass(frozen=True)
class HttpResponse:
    url: str
    status: int
    content_type: str | None
    body: bytes               # bytes originales: se guardan tal cual en raw
    requested_at: datetime    # UTC
    elapsed_s: float
    attempts: int             # cuántos intentos hicieron falta (útil para auditoría)

    @property
    def sha256(self) -> str:
        """Huella del contenido: detecta si dos extracciones trajeron exactamente lo mismo."""
        return hashlib.sha256(self.body).hexdigest()


def _retry_after_seconds(resp: requests.Response, default: float) -> float:
    """Segundos que pide esperar el servidor en un 429 (cabecera Retry-After), con tope."""
    value = resp.headers.get("Retry-After")
    if value and value.strip().isdigit():
        return min(float(value), MAX_RETRY_AFTER_S)
    return default  # si no viene (o viene como fecha), se usa la espera exponencial


def http_get(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    timeout_s: float = 60,
    max_attempts: int = 3,
    backoff_s: float = 2.0,
    session: requests.Session | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> HttpResponse:
    """GET con reintentos.

    - 200                      -> devuelve HttpResponse.
    - 429 / 5xx / red / timeout -> reintenta, esperando backoff * 2^(intento-1): 2 s, 4 s, 8 s...
                                   (en 429 se respeta Retry-After).
    - otro 4xx (400, 404, 410)  -> falla de inmediato: reintentar no lo va a arreglar.

    `session` y `sleep` son parámetros para poder probar sin red y sin esperas reales.
    """
    http = session or requests.Session()
    requested_at = datetime.now(timezone.utc)
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        wait = backoff_s * 2 ** (attempt - 1)
        start = time.monotonic()
        try:
            resp = http.get(url, params=params, timeout=timeout_s,
                            headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_error = ConnectionFailedError(f"{type(exc).__name__} al consultar {url}: {exc}")
        else:
            if resp.status_code == 200:
                return HttpResponse(url, 200, resp.headers.get("Content-Type"), resp.content,
                                    requested_at, round(time.monotonic() - start, 3), attempt)
            if resp.status_code not in RETRYABLE_STATUS:
                raise HttpStatusError(url, resp.status_code)
            last_error = HttpStatusError(url, resp.status_code, "(reintentable)")
            if resp.status_code == 429:
                wait = _retry_after_seconds(resp, wait)
        if attempt < max_attempts:
            log.warning("Intento %d/%d falló (%s). Nuevo intento en %.1fs", attempt, max_attempts, last_error, wait)
            sleep(wait)
    assert last_error is not None
    raise last_error


def parse_json(response: HttpResponse) -> Any:
    """Decodifica el cuerpo como JSON SIN confiar en el Content-Type.

    MINSAL declara text/html aunque envía JSON; y apis.modernizacion.cl responde 200 con una
    página HTML. Lo que manda es el contenido. 'utf-8-sig' quita un posible BOM inicial.
    """
    try:
        return json.loads(response.body.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        preview = response.body[:120].decode("utf-8", "replace")
        raise InvalidResponseError(
            f"Respuesta no es JSON válido desde {response.url}: {exc}; inicio={preview!r}") from exc
