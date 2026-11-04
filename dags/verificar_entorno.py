"""DAG manual de verificación del entorno: Connection 'farmacias_pg' y Variables de Airflow.

No forma parte del flujo ETL. Se usa al instalar y al inicio de la demostración. Usa las mismas
funciones que los DAGs del ETL (af.db, af.settings): si este DAG funciona, la configuración que
ellos necesitan está bien.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from src.config.settings import DEFAULTS, ZONA_HORARIA
from src.orquestacion import airflow_utils as af


@dag(
    dag_id="verificar_entorno",
    schedule=None,                     # solo manual
    start_date=pendulum.datetime(2026, 10, 1, tz=str(ZONA_HORARIA)),
    catchup=False,
    tags=["infraestructura"],
    doc_md=__doc__,
)
def verificar_entorno():

    @task
    def verificar_postgres() -> dict:
        def consultar(conn):
            db, user, superuser = conn.execute(
                "SELECT current_database(), current_user, current_setting('is_superuser')").fetchone()
            esquemas = [r[0] for r in conn.execute(
                "SELECT nspname FROM pg_namespace WHERE nspname IN ('raw','staging','dw','mart','audit') ORDER BY 1")]
            return {"db": db, "user": user, "superusuario": superuser, "esquemas": esquemas}
        r = af.con_conexion(consultar)
        print(f"Connection {af.CONN_ID} OK: {r}")
        return r

    @task
    def verificar_variables() -> dict:
        s = af.settings()   # Variable.get dentro de una tarea real
        faltantes = [k for k in DEFAULTS if not getattr(s, k)]
        if faltantes:
            raise ValueError(f"Variables de Airflow faltantes: {faltantes}")
        print("Variables OK:", s)
        return {"variables": list(DEFAULTS), "dpa_page_size": s.dpa_page_size, "http_timeout_s": s.http_timeout_s}

    verificar_postgres()
    verificar_variables()


verificar_entorno()
