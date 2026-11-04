"""Utilidades compartidas por los DAGs: configuración desde Airflow, auditoría, reintentos y Assets.

Es el ÚNICO módulo de src/ que importa Airflow. Traduce entre el mundo de Airflow (Variables,
Connection, contexto de la tarea, AirflowSkipException) y el código ETL, que no conoce Airflow.
"""
from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from datetime import date, datetime, timedelta

from airflow.sdk import Asset, BaseHook, Variable, get_current_context
from airflow.sdk.exceptions import AirflowSkipException

from src.audit.auditoria import ContextoEjecucion, EtapaAuditada, EtapaOmitida, registrar_falla
from src.config.settings import DEFAULTS, ZONA_HORARIA, Settings
from src.database.connection import DbParams
from src.utils.logging_config import get_logger

log = get_logger(__name__)

# Connection definida por variable de entorno en .env: AIRFLOW_CONN_FARMACIAS_PG=postgresql://...
CONN_ID = "farmacias_pg"

# Assets: los DAGs de fuente los "actualizan" al terminar (outlets) y el DAG de indicadores se
# programa con ellos (schedule = ASSET_MINSAL | ASSET_TERRITORIO). Así se dispara solo.
ASSET_MINSAL = Asset(name="farmacias_dw_establecimientos", uri="farmacias://dw/fact_establecimiento_diario")
ASSET_TERRITORIO = Asset(name="farmacias_dw_territorio", uri="farmacias://dw/dim_territorio")


# ================================================================== configuración
def settings() -> Settings:
    """URLs y parámetros desde las Variables de Airflow.

    Se llama DENTRO de las tareas, no al nivel del módulo del DAG: el dag-processor vuelve a leer
    los archivos de DAG cada 30 s, y leer Variables ahí consultaría la base en cada lectura.
    Además, así un cambio de Variable (p. ej. la demo de falla) aplica en la siguiente ejecución.
    """
    return Settings.from_mapping({k: Variable.get(k, default=None) for k in DEFAULTS})


def db() -> DbParams:
    """Parámetros de la base del proyecto desde la Connection 'farmacias_pg' (sin contraseñas en el código)."""
    c = BaseHook.get_connection(CONN_ID)
    return DbParams(host=c.host, port=int(c.port or 5432), dbname=c.schema, user=c.login, password=c.password)


def con_conexion(fn: Callable):
    """Ejecuta fn(conn) con una conexión de datos en autocommit.

    Igual que scripts/ejecutar_local.py: cada operación de los loaders confirma su propia
    transacción (conn.transaction()), así el mismo pipeline se comporta igual dentro y fuera de Airflow.
    """
    with db().connect(autocommit=True) as conn:
        return fn(conn)


# ================================================================== contexto de ejecución
def contexto_ejecucion(context=None) -> ContextoEjecucion:
    """dag_id, run_id, task_id e intento de la tarea en curso (clave de audit.etl_audit)."""
    ti = (context or get_current_context())["ti"]
    return ContextoEjecucion(ti.dag_id, ti.run_id, ti.task_id, int(ti.try_number or 1))


def hoy() -> date:
    return datetime.now(ZONA_HORARIA).date()


def fecha_observacion(context=None) -> date:
    """Fecha a procesar, en hora de Chile:
    1) el parámetro 'fecha_reproceso' si se indicó al disparar el DAG (recarga de una fecha pasada);
    2) si no, la fecha lógica del run (ejecuciones programadas);
    3) si no hay (runs manuales o disparados por Assets), el momento en que se ejecuta (run_after).
    """
    context = context or get_current_context()
    param = (context.get("params") or {}).get("fecha_reproceso")
    if param:
        return date.fromisoformat(param)
    base = context.get("logical_date") or context["dag_run"].run_after
    return base.astimezone(ZONA_HORARIA).date()


def es_reproceso(fecha: date, context=None) -> bool:
    """¿Hay que reconstruir la fecha desde raw, sin consultar la API?

    - Una fecha pasada: siempre (la API solo entrega el estado actual).
    - Si se indicó 'fecha_reproceso' explícitamente: también, aunque sea la fecha de hoy. Así se puede
      reprocesar el día en curso (p. ej. tras agregar un alias) sin cambiar la foto ya extraída.
    """
    context = context or get_current_context()
    pedido = bool((context.get("params") or {}).get("fecha_reproceso"))
    return pedido or fecha < hoy()


# ================================================================== auditoría y fallas
@contextmanager
def etapa(nombre: str, fuente: str | None = None, tabla: str | None = None, fecha: date | None = None):
    """Etapa auditada dentro de una tarea. EtapaOmitida se traduce a AirflowSkipException
    (la tarea queda 'skipped' en la UI, no 'failed')."""
    params = db()
    aud = EtapaAuditada(lambda: params.connect(autocommit=True), contexto_ejecucion(), nombre, fuente, fecha, tabla)
    try:
        with aud:
            yield aud
    except EtapaOmitida as exc:
        raise AirflowSkipException(str(exc)) from exc


def omitir(mensaje: str, nombre: str, fuente: str | None = None, fecha: date | None = None):
    """Registra una etapa como 'omitido' y marca la tarea como skipped."""
    with etapa(nombre, fuente, fecha=fecha):
        raise EtapaOmitida(mensaje)


def _callback_falla(context) -> None:
    """on_failure_callback y on_retry_callback: deja registro de CADA intento fallido en
    audit.etl_audit, incluso si la tarea murió fuera del bloque auditado (timeout, error al importar…)."""
    ti = context["ti"]
    exc = context.get("exception")
    mensaje = f"{type(exc).__name__}: {exc}" if exc else "Falla sin excepción asociada"
    try:
        params = db()
        registrar_falla(lambda: params.connect(autocommit=True), contexto_ejecucion(context),
                        etapa=f"tarea:{ti.task_id}", inicio=getattr(ti, "start_date", None), mensaje=mensaje)
    except Exception as cb_exc:  # noqa: BLE001 — un callback nunca debe lanzar; se deja en el log
        log.error("on_failure_callback no pudo registrar la falla de %s: %s", ti.task_id, cb_exc)
    log.error("FALLA registrada en etl_audit: dag=%s task=%s intento=%s: %s",
              ti.dag_id, ti.task_id, ti.try_number, mensaje)


# Política común de reintentos y tiempos (segundo nivel de reintento, después del de http_client).
DEFAULT_ARGS = {
    "owner": "equipo_inf522",
    "retries": 3,                                  # 1 intento + 3 reintentos = 4 intentos
    "retry_delay": timedelta(seconds=20),
    "retry_exponential_backoff": 2.0,              # 20 s -> 40 s -> 80 s
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),    # una tarea colgada no bloquea el DAG para siempre
    "on_failure_callback": _callback_falla,
    "on_retry_callback": _callback_falla,
}
