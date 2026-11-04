"""Registro de cada etapa del ETL en audit.etl_audit.

Cada etapa abre un registro 'en_ejecucion' y lo cierra como 'exito', 'omitido' o 'fallido'.

Usa una CONEXIÓN PROPIA en autocommit: la fila de auditoría persiste aunque la transacción de
datos haga rollback (si se registrara en la misma transacción, el rollback borraría también
la evidencia de la falla). El on_failure_callback de Airflow usa `registrar_falla` para cubrir
fallas que ocurren fuera del bloque, como un timeout que mata la tarea.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

import psycopg
from psycopg.types.json import Jsonb

from src.utils.logging_config import get_logger

log = get_logger(__name__)

_MAX_MENSAJE = 2000


@dataclass(frozen=True)
class ContextoEjecucion:
    """Identifica una fila de auditoría: (dag_id, run_id, task_id, intento) es UNIQUE en la tabla."""
    dag_id: str
    run_id: str
    task_id: str
    intento: int = 1


class EtapaOmitida(Exception):
    """Se lanza dentro de una etapa auditada para cerrarla como 'omitido' (no es un error).

    Ej.: no hay datos raw para la fecha pedida. En Airflow se convierte en AirflowSkipException.
    """


# UPSERT: la misma fila se escribe al iniciar (en_ejecucion) y al terminar (exito/fallido).
# coalesce conserva los valores ya escritos si la segunda escritura no los trae.
SQL_UPSERT = """
INSERT INTO audit.etl_audit (dag_id, run_id, task_id, intento, fuente, etapa, fecha_observacion, estado, inicio,
    fin, tabla_destino, filas_leidas, filas_cargadas, filas_rechazadas, n_errores, pct_nulos, detalle_calidad, mensaje)
VALUES (%(dag_id)s, %(run_id)s, %(task_id)s, %(intento)s, %(fuente)s, %(etapa)s, %(fecha)s, %(estado)s, %(inicio)s,
    %(fin)s, %(tabla)s, %(leidas)s, %(cargadas)s, %(rechazadas)s, %(errores)s, %(pct_nulos)s, %(detalle)s, %(mensaje)s)
ON CONFLICT ON CONSTRAINT uq_audit_intento DO UPDATE SET
    estado = EXCLUDED.estado,
    fin = coalesce(EXCLUDED.fin, audit.etl_audit.fin),
    fuente = coalesce(EXCLUDED.fuente, audit.etl_audit.fuente),
    fecha_observacion = coalesce(EXCLUDED.fecha_observacion, audit.etl_audit.fecha_observacion),
    tabla_destino = coalesce(EXCLUDED.tabla_destino, audit.etl_audit.tabla_destino),
    filas_leidas = coalesce(EXCLUDED.filas_leidas, audit.etl_audit.filas_leidas),
    filas_cargadas = coalesce(EXCLUDED.filas_cargadas, audit.etl_audit.filas_cargadas),
    filas_rechazadas = coalesce(EXCLUDED.filas_rechazadas, audit.etl_audit.filas_rechazadas),
    n_errores = greatest(EXCLUDED.n_errores, audit.etl_audit.n_errores),
    pct_nulos = coalesce(EXCLUDED.pct_nulos, audit.etl_audit.pct_nulos),
    detalle_calidad = coalesce(EXCLUDED.detalle_calidad, audit.etl_audit.detalle_calidad),
    mensaje = coalesce(audit.etl_audit.mensaje, EXCLUDED.mensaje)
"""


@dataclass
class EtapaAuditada:
    """Context manager de auditoría para una etapa:

        with EtapaAuditada(conectar, ctx, "extraccion", "minsal_locales") as aud:
            ...trabajo...
            aud.registrar(leidas=656, cargadas=1)
    """
    conectar: Callable[[], psycopg.Connection]   # crea la conexión propia de auditoría
    ctx: ContextoEjecucion
    etapa: str
    fuente: str | None = None
    fecha_observacion: date | None = None
    tabla_destino: str | None = None
    metricas: dict = field(default_factory=dict)
    inicio: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def registrar(self, *, leidas: int | None = None, cargadas: int | None = None,
                  rechazadas: int | None = None, pct_nulos: float | None = None,
                  detalle: dict | None = None, fecha_observacion: date | None = None) -> None:
        """Guarda métricas en memoria; se escriben en la base al cerrar la etapa."""
        for k, v in (("leidas", leidas), ("cargadas", cargadas), ("rechazadas", rechazadas),
                     ("pct_nulos", pct_nulos), ("detalle", detalle)):
            if v is not None:
                self.metricas[k] = v
        if fecha_observacion is not None:
            self.fecha_observacion = fecha_observacion

    def _escribir(self, estado: str, fin: datetime | None, mensaje: str | None = None, errores: int = 0) -> None:
        params = {
            **self.ctx.__dict__, "fuente": self.fuente, "etapa": self.etapa, "fecha": self.fecha_observacion,
            "estado": estado, "inicio": self.inicio, "fin": fin, "tabla": self.tabla_destino,
            "leidas": self.metricas.get("leidas"), "cargadas": self.metricas.get("cargadas"),
            "rechazadas": self.metricas.get("rechazadas"), "errores": errores,
            "pct_nulos": self.metricas.get("pct_nulos"),
            "detalle": Jsonb(self.metricas["detalle"]) if "detalle" in self.metricas else None,
            "mensaje": mensaje[:_MAX_MENSAJE] if mensaje else None,
        }
        try:
            with self.conectar() as conn:
                conn.execute(SQL_UPSERT, params)
        except psycopg.Error as exc:
            # Si la auditoría falla, NO debe ocultar el error original de la etapa: se registra en el log.
            log.error("No se pudo escribir auditoría (%s/%s): %s", self.ctx.task_id, estado, exc)

    def __enter__(self) -> "EtapaAuditada":
        self._escribir("en_ejecucion", None)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        fin = datetime.now(timezone.utc)
        if exc is None:
            self._escribir("exito", fin)
        elif isinstance(exc, EtapaOmitida):
            self._escribir("omitido", fin, str(exc))
        else:
            self._escribir("fallido", fin, f"{type(exc).__name__}: {exc}", errores=1)
        return False   # nunca suprime la excepción: Airflow debe enterarse de la falla


def registrar_falla(conectar: Callable[[], psycopg.Connection], ctx: ContextoEjecucion, etapa: str,
                    inicio: datetime | None, mensaje: str) -> None:
    """Usado por on_failure_callback: marca el intento como fallido (crea la fila si no existía)."""
    etapa_aud = EtapaAuditada(conectar, ctx, etapa, inicio=inicio or datetime.now(timezone.utc))
    etapa_aud._escribir("fallido", datetime.now(timezone.utc), mensaje, errores=1)
