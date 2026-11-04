"""DAG etl_comunas — División Político-Administrativa SUBDERE (códigos CUT) -> raw -> staging -> dw.dim_territorio.

Fuente principal: servicio DPA del MOP (fuente SUBDERE). Respaldo: Subpesca (DPA 2020).
Ambas URLs son Variables de Airflow.

Schedule mensual (día 1, 06:00 Chile): la DPA solo cambia por ley (creación o fusión de comunas),
con muy baja frecuencia. El DAG de indicadores exige que el catálogo tenga a lo más 45 días.

Es independiente de etl_minsal: no comparten tareas ni se esperan entre sí. Si uno falla, el
otro sigue funcionando.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from src.config.settings import ZONA_HORARIA
from src.database.connection import aplicar_ddl
from src.orquestacion import airflow_utils as af
from src.pipelines import comunas


@dag(
    dag_id="etl_comunas",
    schedule="0 6 1 * *",                    # minuto 0, hora 6, día 1 de cada mes
    start_date=pendulum.datetime(2026, 10, 1, tz=str(ZONA_HORARIA)),
    catchup=False,                           # no recrear meses pasados al activarlo
    max_active_runs=1,                       # nunca dos cargas del catálogo a la vez
    default_args=af.DEFAULT_ARGS,
    dagrun_timeout=pendulum.duration(minutes=30),
    tags=["etl", "subdere", "dpa", "fuente"],
    doc_md=__doc__,
)
def etl_comunas():

    @task
    def asegurar_esquema() -> list[str]:
        return af.con_conexion(aplicar_ddl)

    @task
    def validar_api() -> dict:
        # Validación previa: si la API no responde, no se intenta extraer.
        with af.etapa("validacion", "dpa_comunas", fecha=af.hoy()) as aud:
            r = comunas.validar_api(af.settings())
            aud.registrar(leidas=r["registros"], detalle=r)
        return r

    @task
    def extraer_raw() -> dict:
        fecha, ctx = af.hoy(), af.contexto_ejecucion()
        with af.etapa("extraccion", "dpa_comunas", "raw.api_respuesta", fecha) as aud:
            r = af.con_conexion(lambda c: comunas.extraer_a_raw(c, af.settings(), fecha, ctx.dag_id, ctx.run_id))
            aud.registrar(leidas=r["registros"], cargadas=r["paginas_insertadas"], detalle=r)
        return r

    @task
    def transformar_staging() -> dict:
        fecha = af.hoy()
        with af.etapa("transformacion", "dpa_comunas", "staging.dpa_comuna", fecha) as aud:
            t = af.con_conexion(lambda c: comunas.transformar_a_staging(c, fecha))
            aud.registrar(leidas=t["leidos"], cargadas=t["carga"]["cargadas"],
                          rechazadas=t["carga"]["rechazadas"], pct_nulos=0.0, detalle=t)
        return t

    @task(outlets=[af.ASSET_TERRITORIO])     # al terminar, avisa que el catálogo cambió
    def cargar_dim_territorio() -> dict:
        with af.etapa("carga_dw", "dpa_comunas", "dw.dim_territorio", af.hoy()) as aud:
            d = af.con_conexion(comunas.cargar_dim_territorio)
            aud.registrar(cargadas=d["vigentes"], detalle=d)
        return d

    asegurar_esquema() >> validar_api() >> extraer_raw() >> transformar_staging() >> cargar_dim_territorio()


etl_comunas()
