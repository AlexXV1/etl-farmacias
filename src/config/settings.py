"""Configuración del ETL, separada de la lógica.

- En Airflow, los valores vienen de las Variables (config/variables.json) y la base de datos
  de la Connection 'farmacias_pg'.
- Fuera de Airflow (scripts y pruebas locales), de variables de entorno o del archivo .env.

Este módulo NO importa Airflow: así todo src/ se puede probar sin el orquestador.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo

# Todas las fechas de observación se calculan en hora de Chile.
ZONA_HORARIA = ZoneInfo("America/Santiago")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
# En el contenedor el SQL está montado en /opt/airflow/project/sql (ver docker-compose.yaml).
SQL_DIR = Path(os.environ.get("PROJECT_SQL_DIR", PROJECT_ROOT / "sql"))

# Valores por defecto: los mismos de config/variables.json.
DEFAULTS = {
    "minsal_locales_url": "https://midas.minsal.cl/farmacia_v2/WS/getLocales.php",
    "minsal_turnos_url": "https://midas.minsal.cl/farmacia_v2/WS/getLocalesTurnos.php",
    "dpa_comunas_url": "https://rest-sit.mop.gob.cl/arcgis/rest/services/INTEROP/SERVICIO_DPA/MapServer/1",
    "dpa_comunas_url_respaldo": "https://geoportal.subpesca.cl/server/rest/services/Hosted/COMUNA/FeatureServer/0",
    "dpa_page_size": "100",
    "http_timeout_s": "60",
}


@dataclass(frozen=True)  # frozen: la configuración no se modifica a mitad de una ejecución
class Settings:
    minsal_locales_url: str
    minsal_turnos_url: str
    dpa_comunas_url: str
    dpa_comunas_url_respaldo: str
    dpa_page_size: int
    http_timeout_s: float

    @classmethod
    def from_mapping(cls, values: dict[str, str | None]) -> "Settings":
        """Crea Settings desde un diccionario (p. ej. las Variables de Airflow).

        Las claves que falten o vengan vacías toman el valor por defecto. Las Variables
        siempre son texto, por eso se convierten aquí a int / float.
        """
        merged = {k: (values.get(k) or DEFAULTS[k]) for k in DEFAULTS}
        return cls(
            minsal_locales_url=merged["minsal_locales_url"],
            minsal_turnos_url=merged["minsal_turnos_url"],
            dpa_comunas_url=merged["dpa_comunas_url"].rstrip("/"),
            dpa_comunas_url_respaldo=merged["dpa_comunas_url_respaldo"].rstrip("/"),
            dpa_page_size=int(merged["dpa_page_size"]),
            http_timeout_s=float(merged["http_timeout_s"]),
        )

    @classmethod
    def from_env(cls) -> "Settings":
        """Para uso local: MINSAL_LOCALES_URL, DPA_PAGE_SIZE, etc. (mayúsculas)."""
        return cls.from_mapping({k: os.environ.get(k.upper()) for k in DEFAULTS})


def read_dotenv(path: Path = PROJECT_ROOT / ".env") -> dict[str, str]:
    """Lee un archivo .env simple (CLAVE=valor). No imprime ni registra ningún valor."""
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values
