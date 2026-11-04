"""Estructura común de una extracción: páginas originales + metadatos + métricas.

Los extractores devuelven siempre un objeto `Extraccion`, sea cual sea la API. Así el loader
de raw (paso 11) guarda cualquier fuente con el mismo código.
"""
from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class Pagina:
    """Una respuesta HTTP (una página). Es lo que se guarda en una fila de raw.api_respuesta."""
    numero: int
    url: str
    parametros: dict[str, Any] | None   # p. ej. los objectIds pedidos a ArcGIS
    http_status: int
    content_type: str | None
    contenido: Any                      # JSON tal como llegó, sin modificar
    body_sha256: str                    # SHA-256 de los bytes recibidos
    n_registros: int


@dataclass
class Extraccion:
    fuente: str                         # minsal_locales / minsal_turnos / dpa_comunas
    url_base: str
    extraido_en: datetime               # UTC; raw guarda fecha y hora de extracción
    paginas: list[Pagina]
    registros: list[dict]               # registros de todas las páginas, para transformar
    intentos_http: int = 1
    duracion_s: float = 0.0
    advertencias: list[str] = field(default_factory=list)
    extraccion_id: uuid.UUID = field(default_factory=uuid.uuid4)  # agrupa las páginas en raw

    @property
    def n_registros(self) -> int:
        return len(self.registros)

    @property
    def sha256(self) -> str:
        """Huella de la extracción completa: SHA-256 de las huellas de cada página, en orden.

        Si cualquier byte de cualquier página cambia, la huella cambia. Es la base de la
        idempotencia de raw (UNIQUE fuente + fecha + extraccion_sha256 + pagina).
        """
        h = hashlib.sha256()
        for p in self.paginas:
            h.update(p.body_sha256.encode())
        return h.hexdigest()

    def metricas(self) -> dict:
        """Resumen para logs y auditoría."""
        return {
            "fuente": self.fuente,
            "url": self.url_base,
            "registros": self.n_registros,
            "paginas": len(self.paginas),
            "intentos_http": self.intentos_http,
            "duracion_s": round(self.duracion_s, 3),
            "advertencias": self.advertencias,
        }
