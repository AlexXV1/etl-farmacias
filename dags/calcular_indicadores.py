"""DAG calcular_indicadores — KPIs en SQL (mart.calcular_indicadores) sobre el modelo dimensional.

Disparo AUTOMÁTICO por Assets: corre cuando etl_minsal o etl_comunas actualizan el dw
(schedule = ASSET_MINSAL | ASSET_TERRITORIO). Es OR y no AND: MINSAL carga a diario y la DPA
una vez al mes; con AND los indicadores se calcularían solo una vez al mes.

Antes de calcular verifica que AMBAS fuentes estén cargadas y vigentes; si no, la ejecución queda
'omitido' en etl_audit y los indicadores de esa fecha no se actualizan.

Al final exporta la evidencia del día a evidence/AAAA-MM-DD/ (carpeta montada desde el host):
cada carga diaria deja su foto en archivos sin intervención manual.
"""
from __future__ import annotations

from datetime import date

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

from src.config.settings import ZONA_HORARIA
from src.orquestacion import airflow_utils as af
from src.pipelines import evidencia, indicadores


def _fecha_objetivo(conn) -> date:
    """Parámetro manual > fecha informada por el evento de etl_minsal > última fecha con hechos."""
    context = get_current_context()
    if (context.get("params") or {}).get("fecha"):
        return date.fromisoformat(context["params"]["fecha"])
    fechas = [ev.extra["fecha_observacion"]
              for eventos in (context.get("triggering_asset_events") or {}).values()
              for ev in eventos if (ev.extra or {}).get("fecha_observacion")]
    return date.fromisoformat(max(fechas)) if fechas else indicadores.fecha_a_calcular(conn, None)


@dag(
    dag_id="calcular_indicadores",
    schedule=(af.ASSET_MINSAL | af.ASSET_TERRITORIO),
    start_date=pendulum.datetime(2026, 10, 1, tz=str(ZONA_HORARIA)),
    catchup=False,
    max_active_runs=1,
    default_args=af.DEFAULT_ARGS,
    dagrun_timeout=pendulum.duration(minutes=20),
    params={"fecha": Param(None, type=["null", "string"], format="date",
                           description="Fecha a recalcular (AAAA-MM-DD). Vacío = la del evento o la última cargada.")},
    tags=["indicadores", "mart"],
    doc_md=__doc__,
)
def calcular_indicadores():

    @task
    def verificar_dependencias() -> str:
        with af.etapa("verificacion_dependencias", "dw") as aud:
            def _run(conn):
                fecha = _fecha_objetivo(conn)
                aud.registrar(fecha_observacion=fecha)
                return fecha, indicadores.verificar_dependencias(conn, fecha)
            fecha, dep = af.con_conexion(_run)
            aud.registrar(leidas=dep["hechos"], detalle=dep)
        return fecha.isoformat()

    @task
    def calcular_kpis(fecha_iso: str) -> list[dict]:
        fecha = date.fromisoformat(fecha_iso)
        with af.etapa("calculo_kpi", "mart", "mart.indicador_valor", fecha) as aud:
            kpis = af.con_conexion(lambda c: indicadores.calcular(c, fecha))
            aud.registrar(cargadas=len(kpis), detalle={"indicadores": kpis})
        for k in kpis:
            print(f"{k['codigo']}: {k['valor']:.2f}% ({k['numerador']:.0f}/{k['denominador']:.0f}) "
                  f"cumple_meta={k['cumple_meta']}")
        return kpis

    @task
    def validar_resultados(fecha_iso: str) -> dict:
        fecha = date.fromisoformat(fecha_iso)
        with af.etapa("validacion_resultados", "mart", fecha=fecha) as aud:
            r = af.con_conexion(lambda c: indicadores.validar_resultados(c, fecha))
            aud.registrar(detalle=r)
        return r

    @task
    def exportar_evidencia(fecha_iso: str) -> dict:
        fecha = date.fromisoformat(fecha_iso)
        with af.etapa("exportacion_evidencia", "mart", f"evidence/{fecha_iso}", fecha) as aud:
            r = af.con_conexion(lambda c: evidencia.exportar(c, fecha))
            aud.registrar(cargadas=len(r["filas"]), detalle=r)
        print(f"Evidencia exportada en {r['carpeta']}: {r['filas']}")
        return r

    fecha = verificar_dependencias()
    # Solo se exporta si los controles de consistencia pasaron.
    calcular_kpis(fecha) >> validar_resultados(fecha) >> exportar_evidencia(fecha)


calcular_indicadores()
