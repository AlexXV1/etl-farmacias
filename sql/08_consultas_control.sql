-- 08_consultas_control.sql — Consultas para el informe y la demostración.
-- NO es DDL: no se aplica automáticamente. Se ejecuta a mano o con scripts/consultas_control.py.

-- 1. Registros cargados por tabla (última ejecución exitosa de cada tarea con tabla destino).
SELECT DISTINCT ON (dag_id, tabla_destino)
       dag_id, tabla_destino, fecha_observacion, filas_leidas, filas_cargadas, filas_rechazadas, inicio
FROM audit.etl_audit
WHERE estado = 'exito' AND tabla_destino IS NOT NULL AND dag_id <> 'ejecucion_local'
ORDER BY dag_id, tabla_destino, inicio DESC;

-- 2. Porcentaje de registros con errores (rechazados) o nulos, por etapa de transformación.
--    pct_rechazados: denominador = filas_leidas de la etapa; pct_nulos: denominador = filas_cargadas.
SELECT dag_id, fuente, fecha_observacion, filas_leidas, filas_cargadas, filas_rechazadas,
       round(100.0 * filas_rechazadas / NULLIF(filas_leidas, 0), 2) AS pct_rechazados,
       pct_nulos AS pct_filas_con_nulos,
       detalle_calidad -> 'nulos' -> 'por_campo' AS nulos_por_campo
FROM audit.etl_audit
WHERE etapa = 'transformacion' AND estado = 'exito'
ORDER BY inicio DESC;

-- 3. Duración de cada ejecución de DAG (desde el inicio de la primera tarea al fin de la última).
SELECT dag_id, run_id, min(inicio) AS inicio, max(fin) AS fin,
       round(extract(epoch FROM max(fin) - min(inicio))::numeric, 1) AS duracion_s,
       count(*) AS tareas_auditadas
FROM audit.etl_audit
GROUP BY dag_id, run_id
ORDER BY min(inicio) DESC;

-- 4. Estado de las ejecuciones (incluye intentos fallidos y reintentos).
SELECT dag_id, run_id, task_id, intento, estado, round(duracion_s, 2) AS duracion_s, left(mensaje, 120) AS mensaje
FROM audit.etl_audit
ORDER BY inicio DESC
LIMIT 50;

-- 5. Registros rechazados por fuente y motivo.
SELECT fuente, motivo, count(*) AS registros, min(fecha_observacion) AS desde, max(fecha_observacion) AS hasta
FROM staging.rechazo
GROUP BY fuente, motivo
ORDER BY registros DESC;

-- 6. Correspondencia farmacia <-> comuna (cuántas por nombre exacto, alias o sin comuna).
SELECT fecha_observacion, estado_correspondencia, count(*) AS establecimientos,
       string_agg(DISTINCT comuna_origen, ', ') FILTER (WHERE estado_correspondencia <> 'exacto') AS comunas
FROM staging.minsal_establecimiento
GROUP BY fecha_observacion, estado_correspondencia
ORDER BY fecha_observacion DESC, establecimientos DESC;

-- 7. Calidad de coordenadas.
SELECT fecha_observacion, estado_coordenada, count(*),
       round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY fecha_observacion), 2) AS pct
FROM staging.minsal_establecimiento
GROUP BY fecha_observacion, estado_coordenada
ORDER BY fecha_observacion DESC, count(*) DESC;

-- 8. Contraste auditoría vs. base real (deben coincidir).
SELECT a.fecha_observacion, a.filas_cargadas AS segun_auditoria,
       (SELECT count(*) FROM dw.fact_establecimiento_diario f
        WHERE f.fecha_id = to_char(a.fecha_observacion, 'YYYYMMDD')::int) AS en_dw
FROM audit.etl_audit a
WHERE a.dag_id = 'etl_minsal' AND a.etapa = 'carga_dw' AND a.estado = 'exito'
ORDER BY a.inicio DESC LIMIT 1;

-- 9. Evolución diaria (una fila por fecha observada).
SELECT d.fecha, d.nombre_dia, count(*) AS establecimientos,
       count(*) FILTER (WHERE f.en_turno)      AS turnos_publicados,
       count(*) FILTER (WHERE f.turno_vigente) AS turnos_vigentes,
       count(DISTINCT f.territorio_sk) FILTER (WHERE f.territorio_sk <> 0) AS comunas_con_oferta
FROM dw.fact_establecimiento_diario f
JOIN dw.dim_fecha d USING (fecha_id)
GROUP BY d.fecha, d.nombre_dia
ORDER BY d.fecha;

-- 10. Indicadores y estado de actualización.
SELECT * FROM mart.v_indicadores ORDER BY fecha DESC, codigo;
SELECT * FROM mart.v_estado_indicadores;
