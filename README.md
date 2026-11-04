# ETL de cobertura farmacéutica en Chile

Procesos ETL en **Python**, orquestados con **Apache Airflow 3.3.2**, que consumen dos **APIs públicas de Chile**
(MINSAL Farmanet y División Político-Administrativa) y cargan **PostgreSQL 16** para calcular **5 indicadores de gestión**.

Evaluación 2 de INF-522, Ingeniería Civil Informática, Universidad Católica del Maule.

**Pregunta de gestión:** ¿cómo se distribuyen entre las 345 comunas de Chile los establecimientos farmacéuticos
registrados en Farmanet, qué comunas no tienen ninguno registrado y cuántas tienen una farmacia de turno publicada cada día?

## Contenido

- [Fuentes de datos](#fuentes-de-datos)
- [Arquitectura](#arquitectura)
- [DAGs](#dags)
- [Instalación](#instalación)
- [Ejecución](#ejecución)
- [Indicadores](#indicadores)
- [Estructura del repositorio](#estructura-del-repositorio)
- [Seguridad](#seguridad)
- [Problemas frecuentes](#problemas-frecuentes)

## Fuentes de datos

Todos los datos se obtienen por API, sin descargas manuales.

| Fuente | Endpoint | Formato | DAG |
|---|---|---|---|
| MINSAL Farmanet, locales | `https://midas.minsal.cl/farmacia_v2/WS/getLocales.php` | JSON, una respuesta | `etl_minsal` |
| MINSAL Farmanet, turnos | `https://midas.minsal.cl/farmacia_v2/WS/getLocalesTurnos.php` | JSON, una respuesta | `etl_minsal` |
| DPA SUBDERE (códigos CUT), servicio MOP | `https://rest-sit.mop.gob.cl/arcgis/rest/services/INTEROP/SERVICIO_DPA/MapServer/1` | ArcGIS REST, paginado por OBJECTID | `etl_comunas` |
| DPA 2020, respaldo Subpesca | `https://geoportal.subpesca.cl/server/rest/services/Hosted/COMUNA/FeatureServer/0` | ArcGIS REST | `etl_comunas` (si falla el MOP) |

**Campo de cruce:** nombre de la comuna normalizado (sin tildes y en mayúsculas), más 10 alias verificados.
`fk_comuna` y `fk_region` de MINSAL son códigos internos, no el CUT; la región se valida con un mapa
`fk_region → CUT_REG` inferido de los propios datos.

## Arquitectura

```text
 APIs ──► etl_minsal  (diario 08:00)  ──┐                     ┌──► mart: 5 indicadores
          etl_comunas (mensual, día 1) ─┼─► raw ► staging ► dw ┤
                                        │   (PostgreSQL)       └──► audit.etl_audit
                                        └── Assets ──► calcular_indicadores (se dispara solo)
```

| Capa | Contenido |
|---|---|
| `raw` | Respuesta JSON de cada API, con fecha y hora de extracción |
| `staging` | Datos limpios, con estados de calidad y rechazos con motivo |
| `dw` | Esquema estrella: `fact_establecimiento_diario`, `dim_fecha`, `dim_territorio`, `dim_establecimiento` |
| `mart` | Agregados e indicadores |
| `audit` | Una fila por tarea e intento (`etl_audit`) |

## DAGs

| DAG | Schedule | Función |
|---|---|---|
| `etl_minsal` | Diario, 08:00 | Extrae locales y turnos, y carga `raw`, `staging` y `dw` |
| `etl_comunas` | Día 1 de cada mes, 06:00 | Carga el catálogo de comunas, que solo cambia por ley |
| `calcular_indicadores` | Por Assets | Calcula los KPIs cuando alguna fuente actualiza el `dw` |
| `verificar_entorno` | Manual | Comprueba la Connection y las Variables |

Los DAGs de fuente son independientes: si uno falla, el otro sigue funcionando. Las cargas son **idempotentes**
(`ON CONFLICT` y reemplazo por fecha), hay reintentos en dos niveles (HTTP y Airflow), validación previa de cada API,
y cada intento fallido queda registrado en `audit.etl_audit` mediante `on_failure_callback`.

## Instalación

**Requisitos:** Windows con PowerShell, Docker Desktop iniciado, Python 3.13 y los puertos `8080` y `5433` libres.

**1. Preparar el entorno de Python**

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

**2. Crear el archivo `.env`** con contraseñas y claves aleatorias:

```powershell
.\.venv\Scripts\python.exe scripts\generar_env.py
```

**3. Levantar los servicios**

```powershell
docker compose up -d postgres     # crea el rol etl_farmacias y la base farmacias
docker compose up airflow-init    # esperar "exited with code 0"
docker compose up -d
docker compose ps                 # los servicios deben quedar "healthy"
```

**4. Verificar**

```powershell
.\.venv\Scripts\python.exe scripts\verificar_postgres.py
docker compose exec airflow-scheduler airflow dags list-import-errors
```

La interfaz de Airflow queda en <http://127.0.0.1:8080>. El usuario y la contraseña están en `.env` (`AIRFLOW_ADMIN_*`).

## Ejecución

**Primera carga.** Los DAGs se crean en pausa, así que hay que activarlos y lanzar las dos fuentes:

```powershell
foreach ($d in "etl_comunas","etl_minsal","calcular_indicadores") { docker compose exec airflow-scheduler airflow dags unpause $d }
docker compose exec airflow-scheduler airflow dags trigger etl_comunas
docker compose exec airflow-scheduler airflow dags trigger etl_minsal
```

`calcular_indicadores` se ejecuta solo al terminar las fuentes y exporta la evidencia del día a `evidence\AAAA-MM-DD\`.

**Revisar resultados**

```powershell
.\.venv\Scripts\python.exe scripts\conteos.py            # filas por tabla
.\.venv\Scripts\python.exe scripts\resultados.py         # indicadores
.\.venv\Scripts\python.exe scripts\consultas_control.py  # carga, % de nulos y errores, y duración por DAG
```

**Demostración de falla controlada**

```powershell
.\scripts\demo_falla.ps1 -Paso provocar    # luego: -Paso estado y -Paso recuperar
```

**Reprocesar una fecha** desde `raw`, sin consultar la API (desde Git Bash, o en la interfaz con *Trigger → Advanced*):

```bash
docker compose exec airflow-scheduler airflow dags trigger etl_minsal --conf '{"fecha_reproceso": "2026-11-02"}'
```

**Detener y reanudar**

```powershell
docker compose stop      # detiene y conserva los datos
docker compose start     # reanuda
docker compose down -v   # ⚠ borra los datos; las APIs no tienen historial, así que no se recuperan
```

## Indicadores

| Código | Indicador | Meta |
|---|---|---|
| IND01 | Cobertura comunal: comunas con ≥ 1 establecimiento / 345 comunas | ≥ 90 % |
| IND02 | Concentración: establecimientos en el 10 % de comunas con más establecimientos / total con comuna | ≤ 50 % |
| IND03 | Cobertura mínima regional: menor % de comunas cubiertas entre las 16 regiones | ≥ 50 % |
| IND04 | Cobertura de turnos: comunas con turno vigente / comunas con ≥ 1 establecimiento | ≥ 80 % |
| IND05 | Calidad del cruce: establecimientos con comuna asignada / establecimientos observados | ≥ 98 % |

Las fichas completas (objetivo, fórmula, fuentes, periodicidad y fundamento de la meta) están en la tabla
`mart.indicador`, y los valores diarios en `mart.v_indicadores`.

## Estructura del repositorio

```text
dags/                 etl_minsal · etl_comunas · calcular_indicadores · verificar_entorno
src/
  extractors/         cliente HTTP (reintentos, 429, timeouts) y extractores de MINSAL y DPA
  transformers/       normalización, coordenadas, unión de endpoints y cruce con CUT
  loaders/            carga idempotente a raw, staging y dw
  pipelines/          pasos del ETL, independientes de Airflow
  audit/              auditoría por etapa
  orquestacion/       Variables, Connection, Assets, reintentos y callbacks de Airflow
  config/ database/ utils/
sql/                  01–07: DDL por capas e indicadores · 08: consultas de control
scripts/              instalación, ejecución local, consultas, demo de falla y empaquetado
config/               variables.json (Variables de Airflow)
docker/               script de creación de la base del proyecto
docker-compose.yaml   PostgreSQL y servicios de Airflow
```

## Seguridad

- Sin credenciales en el código: se leen de `.env` (excluido de Git), de la Connection `farmacias_pg` y de las Variables.
- PostgreSQL y Airflow publican sus puertos solo en `127.0.0.1`, y el ETL usa un rol sin superusuario.
- Las consultas son siempre parametrizadas.

## Problemas frecuentes

| Síntoma | Causa y solución |
|---|---|
| `port is already allocated` | Otro proyecto usa el puerto `8080` o `5433`: detenlo con `docker compose stop` en su carpeta |
| Un servicio queda `unhealthy` o `restarting` | Revisa `docker compose logs <servicio>`; suele ser poca memoria para Docker (≥ 4 GB) |
| `required variable … is missing a value` | Falta `.env`: ejecuta `scripts\generar_env.py` |
| `password authentication failed` | Se cambió `.env` después del primer arranque: restaura el original o usa `docker compose down -v` |
| `$'\r': command not found` en postgres | El `.sh` quedó con CRLF: vuelve a clonar el repositorio |
| `--conf '{...}'` falla desde PowerShell | PowerShell altera las comillas del JSON: usa Git Bash o la interfaz de Airflow |
| Turnos vigentes casi en 0 | La carga se hizo de madrugada, antes de que MINSAL publique los turnos: se corrige con la carga de las 08:00 |
