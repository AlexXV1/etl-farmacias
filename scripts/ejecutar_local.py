"""Ejecuta el ETL completo FUERA de Airflow (prueba de integración desde Windows).

Usa exactamente las mismas funciones que los DAGs (src/pipelines) y audita cada etapa con
dag_id='ejecucion_local'.
Uso:  .\\.venv\\Scripts\\python.exe scripts\\ejecutar_local.py [comunas|minsal|indicadores|todo]
      (indicadores requiere sql/06_indicadores.sql, paso 16)
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.audit.auditoria import ContextoEjecucion, EtapaAuditada  # noqa: E402
from src.config.settings import ZONA_HORARIA, Settings  # noqa: E402
from src.database.connection import aplicar_ddl, params_from_env  # noqa: E402
from src.pipelines import comunas, indicadores, minsal  # noqa: E402

DB = params_from_env()
SETTINGS = Settings.from_env()
RUN_ID = "local__" + datetime.now(ZONA_HORARIA).strftime("%Y%m%dT%H%M%S")
FECHA = datetime.now(ZONA_HORARIA).date()   # fecha de observación = hoy en Chile


def etapa(task_id, nombre, fuente=None, tabla=None):
    """Etapa auditada con su propia conexión (autocommit) para la auditoría."""
    ctx = ContextoEjecucion("ejecucion_local", RUN_ID, task_id)
    return EtapaAuditada(lambda: DB.connect(autocommit=True), ctx, nombre, fuente, FECHA, tabla)


def mostrar(titulo, datos):
    print(f"\n== {titulo}\n{json.dumps(datos, ensure_ascii=False, indent=1, default=str)[:1500]}")


def run_comunas(conn):
    with etapa("validar_api", "validacion", "dpa_comunas") as a:
        v = comunas.validar_api(SETTINGS); a.registrar(leidas=v["registros"]); mostrar("DPA validar", v)
    with etapa("extraer_raw", "extraccion", "dpa_comunas", "raw.api_respuesta") as a:
        r = comunas.extraer_a_raw(conn, SETTINGS, FECHA, "ejecucion_local", RUN_ID)
        a.registrar(leidas=r["registros"], cargadas=r["paginas_insertadas"]); mostrar("DPA raw", r)
    with etapa("transformar_staging", "transformacion", "dpa_comunas", "staging.dpa_comuna") as a:
        t = comunas.transformar_a_staging(conn, FECHA)
        a.registrar(leidas=t["leidos"], cargadas=t["carga"]["cargadas"], rechazadas=t["carga"]["rechazadas"],
                    pct_nulos=0.0, detalle=t["metricas"]); mostrar("DPA staging", t)
    with etapa("cargar_dim_territorio", "carga_dw", "dpa_comunas", "dw.dim_territorio") as a:
        d = comunas.cargar_dim_territorio(conn); a.registrar(cargadas=d["vigentes"]); mostrar("DPA dim", d)


def run_minsal(conn):
    with etapa("validar_api", "validacion", "minsal") as a:
        v = minsal.validar_api(SETTINGS); a.registrar(leidas=sum(x["registros"] for x in v)); mostrar("MINSAL validar", v)
    for fuente in minsal.FUENTES:
        with etapa(f"extraer_{fuente}", "extraccion", fuente, "raw.api_respuesta") as a:
            r = minsal.extraer_a_raw(conn, SETTINGS, fuente, FECHA, "ejecucion_local", RUN_ID)
            a.registrar(leidas=r["registros"], cargadas=r["paginas_insertadas"]); mostrar(f"MINSAL raw {fuente}", r)
    with etapa("transformar_staging", "transformacion", "minsal", "staging.minsal_establecimiento") as a:
        t = minsal.transformar_a_staging(conn, FECHA)
        a.registrar(leidas=t["leidos"], cargadas=t["carga"]["cargadas"], rechazadas=t["carga"]["rechazadas"],
                    pct_nulos=t["calidad"]["pct_filas_con_nulos"], detalle={**t["metricas"], "nulos": t["calidad"]})
        mostrar("MINSAL staging", t["metricas"]); mostrar("MINSAL calidad", t["calidad"])
    with etapa("cargar_dw", "carga_dw", "minsal", "dw.fact_establecimiento_diario") as a:
        d = minsal.cargar_dw(conn, FECHA); a.registrar(leidas=d["staging"], cargadas=d["hechos_cargados"])
        mostrar("MINSAL dw", d)


def run_indicadores(conn):
    with etapa("verificar_dependencias", "validacion", "dw"):
        dep = indicadores.verificar_dependencias(conn, FECHA); mostrar("Dependencias", dep)
    with etapa("calcular_kpis", "calculo", "mart", "mart.indicador_valor") as a:
        kpis = indicadores.calcular(conn, FECHA); a.registrar(cargadas=len(kpis)); mostrar("Indicadores", kpis)
    with etapa("validar_resultados", "validacion", "mart"):
        mostrar("Controles", indicadores.validar_resultados(conn, FECHA))


def main():
    objetivo = sys.argv[1] if len(sys.argv) > 1 else "todo"
    # autocommit=True: cada loader confirma SU transacción (conn.transaction()). Sin autocommit, la
    # primera consulta abriría una transacción implícita que envolvería todo, y una falla al final
    # desharía etapas que la auditoría ya registró como 'exito'.
    with DB.connect(autocommit=True) as conn:
        aplicar_ddl(conn)
        if objetivo in ("comunas", "todo"):
            run_comunas(conn)
        if objetivo in ("minsal", "todo"):
            run_minsal(conn)
        if objetivo in ("indicadores", "todo"):
            run_indicadores(conn)
    print(f"\nOK run_id={RUN_ID} fecha_observacion={FECHA}")


if __name__ == "__main__":
    main()
