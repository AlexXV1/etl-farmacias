"""Conexiones a PostgreSQL (psycopg 3) y aplicación idempotente del DDL."""
from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

import psycopg

from src.config.settings import SQL_DIR, read_dotenv
from src.utils.logging_config import get_logger

log = get_logger(__name__)

# Archivos DDL en orden de aplicación (todos idempotentes: IF NOT EXISTS / ON CONFLICT / OR REPLACE).
# 08_consultas_control.sql NO va aquí: son consultas para el informe, no DDL.
DDL_FILES = (
    "01_schemas.sql", "02_raw.sql", "03_staging.sql", "04_dw.sql",
    "05_mart.sql", "06_indicadores.sql", "07_auditoria.sql",
)
# Número arbitrario que identifica el bloqueo consultivo del DDL (cualquier entero sirve,
# basta con que todos los procesos usen el mismo).
_DDL_LOCK_KEY = 522_2026


@dataclass(frozen=True)
class DbParams:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    def connect(self, autocommit: bool = False) -> psycopg.Connection:
        return psycopg.connect(
            host=self.host, port=self.port, dbname=self.dbname, user=self.user,
            password=self.password, connect_timeout=15, autocommit=autocommit,
            application_name="etl_farmacias",  # visible en pg_stat_activity
        )

    def __repr__(self) -> str:
        # El repr por defecto de un dataclass mostraría la contraseña en cualquier log o
        # traza de error que imprima el objeto. Este la omite.
        return f"DbParams(host={self.host!r}, port={self.port}, dbname={self.dbname!r}, user={self.user!r})"


def params_from_env() -> DbParams:
    """Parámetros para uso fuera de Airflow (desde Windows: 127.0.0.1:5433).

    Las variables de entorno tienen prioridad sobre el archivo .env.
    """
    env = {**read_dotenv(), **os.environ}
    return DbParams(
        host=env.get("FARMACIAS_DB_HOST", "127.0.0.1"),
        port=int(env.get("FARMACIAS_DB_PORT", "5433")),
        dbname=env["FARMACIAS_DB_NAME"],
        user=env["FARMACIAS_DB_USER"],
        password=env["FARMACIAS_DB_PASSWORD"],
    )


def huella_ddl(sql_dir: Path = SQL_DIR) -> str:
    """SHA-256 del contenido de todos los DDL, en orden: cambia si se edita cualquier archivo."""
    h = hashlib.sha256()
    for name in DDL_FILES:
        h.update((sql_dir / name).read_bytes())
    return h.hexdigest()


def aplicar_ddl(conn: psycopg.Connection, sql_dir: Path = SQL_DIR) -> list[str]:
    """Aplica los scripts DDL en orden, solo si cambió su huella. Devuelve los archivos aplicados.

    - Todo ocurre en UNA transacción: si un archivo falla, no queda nada a medias.
    - pg_advisory_xact_lock serializa la aplicación si dos DAGs arrancan a la vez: el segundo
      espera a que el primero termine y luego ve que la huella ya está registrada.
    - Si el esquema ya está en la versión actual no se ejecuta nada, así no se toman bloqueos
      sobre tablas que otros DAGs están usando (lección del deadlock de la referencia, DT-08).
    """
    huella = huella_ddl(sql_dir)
    aplicados: list[str] = []
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (_DDL_LOCK_KEY,))
        ya_existe = conn.execute("SELECT to_regclass('audit.ddl_version')").fetchone()[0]
        if ya_existe and conn.execute(
                "SELECT 1 FROM audit.ddl_version WHERE huella = %s", (huella,)).fetchone():
            log.info("Esquema ya en la versión %s; no se aplica DDL", huella[:12])
            return aplicados
        for name in DDL_FILES:
            # Sin parámetros, psycopg permite ejecutar varias sentencias en una sola llamada.
            conn.execute((sql_dir / name).read_text(encoding="utf-8"))
            aplicados.append(name)
        conn.execute("""CREATE TABLE IF NOT EXISTS audit.ddl_version (
                            huella      text PRIMARY KEY,
                            archivos    text[] NOT NULL,
                            aplicado_en timestamptz NOT NULL DEFAULT now())""")
        conn.execute("INSERT INTO audit.ddl_version (huella, archivos) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                     (huella, aplicados))
    log.info("DDL aplicado (versión %s): %s", huella[:12], ", ".join(aplicados))
    return aplicados
