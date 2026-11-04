-- 07_auditoria.sql — Auditoría de ejecuciones. Una fila por (DAG, run, tarea, intento).
-- La escriben las tareas (al iniciar y al terminar) y el on_failure_callback (fallas, incluidos timeouts).
-- Cubre lo que pide la pauta: DAG, run_id, filas leídas y cargadas, % de nulos, duración y estado.
CREATE TABLE IF NOT EXISTS audit.etl_audit (
    audit_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dag_id            text        NOT NULL,
    run_id            text        NOT NULL,
    task_id           text        NOT NULL,
    intento           smallint    NOT NULL DEFAULT 1,      -- cada reintento de Airflow es una fila nueva
    fuente            text,
    etapa             text        NOT NULL,                 -- validacion / extraccion / transformacion / carga / indicadores
    fecha_observacion date,
    estado            text        NOT NULL CHECK (estado IN ('en_ejecucion', 'exito', 'fallido', 'omitido')),
    inicio            timestamptz NOT NULL,
    fin               timestamptz,
    -- Columna generada: la calcula PostgreSQL, nunca queda inconsistente con inicio/fin.
    duracion_s        numeric(10, 3) GENERATED ALWAYS AS (extract(epoch FROM (fin - inicio))) STORED,
    tabla_destino     text,
    filas_leidas      integer,
    filas_cargadas    integer,
    filas_rechazadas  integer,
    n_errores         integer     NOT NULL DEFAULT 0,
    pct_nulos         numeric(6, 2),   -- % de filas cargadas con >= 1 nulo en campos relevantes (denominador: filas cargadas)
    detalle_calidad   jsonb,           -- {campo: {nulos, total, pct}} y métricas propias de la etapa
    mensaje           text,            -- mensaje de error si falló
    CONSTRAINT uq_audit_intento UNIQUE (dag_id, run_id, task_id, intento)
);
CREATE INDEX IF NOT EXISTS ix_audit_dag_inicio ON audit.etl_audit (dag_id, inicio DESC);
