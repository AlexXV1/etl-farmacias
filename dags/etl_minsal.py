"""DAG etl_minsal — Farmacias MINSAL Farmanet -> raw -> staging -> dw.

Schedule diario 08:00 (America/Santiago): los turnos cambian cada día y la API entrega solo el
estado del momento, así que hay que tomar una fotografía diaria.

Reproceso: la API no tiene historial, por eso NO se vuelve a consultar. Con el parámetro
'fecha_reproceso' (o un backfill) se reconstruye staging y dw desde raw. Funciona también con la
fecha de hoy, por ejemplo después de agregar un alias de comuna:

    airflow dags trigger etl_minsal --conf '{"fecha_reproceso": "2026-11-01"}'
    airflow backfill create --dag-id etl_minsal --from-date 2026-11-01 --to-date 2026-11-03
"""
from __future__ import annotations

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

from src.config.settings import ZONA_HORARIA
from src.database.connection import aplicar_ddl
from src.orquestacion import airflow_utils as af
from src.pipelines import minsal


@dag(
    dag_id="etl_minsal",
    schedule="0 8 * * *",                    # todos los días a las 08:00
    start_date=pendulum.datetime(2026, 11, 1, tz=str(ZONA_HORARIA)),
    catchup=False,
    max_active_runs=1,
    default_args=af.DEFAULT_ARGS,
    dagrun_timeout=pendulum.duration(minutes=45),
    params={"fecha_reproceso": Param(None, type=["null", "string"], format="date",
                                     description="Fecha pasada a reprocesar desde raw (AAAA-MM-DD). Vacío = hoy.")},
    tags=["etl", "minsal", "fuente"],
    doc_md=__doc__,
)
def etl_minsal():

    @task
    def asegurar_esquema() -> list[str]:
        return af.con_conexion(aplicar_ddl)

    @task
    def validar_api() -> list[dict]:
        fecha = af.fecha_observacion()
        if af.es_reproceso(fecha):
            # Skip: las extracciones de abajo también quedan skipped (no se llama a la API).
            af.omitir(f"Reproceso de {fecha}: no se consulta la API", "validacion", "minsal", fecha)
        with af.etapa("validacion", "minsal", fecha=fecha) as aud:
            resultado = minsal.validar_api(af.settings())
            aud.registrar(leidas=sum(r["registros"] for r in resultado), detalle={"endpoints": resultado})
        return resultado

    @task
    def extraer_raw(fuente: str) -> dict:
        fecha = af.fecha_observacion()
        ctx = af.contexto_ejecucion()
        with af.etapa("extraccion", fuente, "raw.api_respuesta", fecha) as aud:
            r = af.con_conexion(lambda c: minsal.extraer_a_raw(c, af.settings(), fuente, fecha, ctx.dag_id, ctx.run_id))
            aud.registrar(leidas=r["registros"], cargadas=r["paginas_insertadas"], detalle=r)
        return r

    # none_failed: corre si nada falló arriba, aunque las extracciones hayan quedado SKIPPED
    # (reproceso). Con la regla por defecto (all_success) el reproceso se saltaría entero.
    @task(trigger_rule="none_failed")
    def transformar_staging() -> dict:
        fecha = af.fecha_observacion()
        with af.etapa("transformacion", "minsal", "staging.minsal_establecimiento", fecha) as aud:
            t = af.con_conexion(lambda c: minsal.transformar_a_staging(c, fecha))
            aud.registrar(leidas=t["leidos"], cargadas=t["carga"]["cargadas"], rechazadas=t["carga"]["rechazadas"],
                          pct_nulos=t["calidad"]["pct_filas_con_nulos"],
                          detalle={**t["metricas"], "nulos": t["calidad"]})
        return {"fecha": fecha.isoformat(), **t["carga"], "estado_correspondencia": t["metricas"]["estado_correspondencia"]}

    @task(outlets=[af.ASSET_MINSAL])
    def cargar_dw() -> dict:
        fecha = af.fecha_observacion()
        with af.etapa("carga_dw", "minsal", "dw.fact_establecimiento_diario", fecha) as aud:
            d = af.con_conexion(lambda c: minsal.cargar_dw(c, fecha))
            aud.registrar(leidas=d["staging"], cargadas=d["hechos_cargados"], detalle=d)
        # La fecha cargada viaja en el evento del Asset: calcular_indicadores sabe qué fecha calcular.
        get_current_context()["outlet_events"][af.ASSET_MINSAL].extra = {"fecha_observacion": fecha.isoformat()}
        return d

    esquema = asegurar_esquema()
    validacion = validar_api()
    # La misma tarea, dos veces: los dos endpoints se extraen en PARALELO.
    locales = extraer_raw.override(task_id="extraer_locales_raw")("minsal_locales")
    turnos = extraer_raw.override(task_id="extraer_turnos_raw")("minsal_turnos")
    staging = transformar_staging()
    esquema >> validacion >> [locales, turnos] >> staging >> cargar_dw()


etl_minsal()
