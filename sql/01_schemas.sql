-- 01_schemas.sql — Capas de la base 'farmacias'. Idempotente (IF NOT EXISTS).
-- Flujo de los datos:  raw -> staging -> dw -> mart   (+ audit transversal)
CREATE SCHEMA IF NOT EXISTS raw;      -- respuestas originales de las APIs + metadatos de extracción
CREATE SCHEMA IF NOT EXISTS staging;  -- registros limpios, normalizados y con estado de calidad
CREATE SCHEMA IF NOT EXISTS dw;       -- modelo dimensional (hechos + dimensiones)
CREATE SCHEMA IF NOT EXISTS mart;     -- indicadores de gestión calculados en SQL
CREATE SCHEMA IF NOT EXISTS audit;    -- auditoría de ejecuciones de los DAGs

COMMENT ON SCHEMA raw     IS 'Respuestas de API tal como llegan (JSONB) con fecha/hora de extracción';
COMMENT ON SCHEMA staging IS 'Datos limpios y normalizados; rechazos con motivo';
COMMENT ON SCHEMA dw      IS 'Modelo dimensional: dim_fecha, dim_territorio, dim_establecimiento, fact_establecimiento_diario';
COMMENT ON SCHEMA mart    IS 'Agregados e indicadores de gestión';
COMMENT ON SCHEMA audit   IS 'Registro de ejecuciones de los DAGs (etl_audit)';
