-- 02_raw.sql — Capa raw. Una fila por PÁGINA de respuesta HTTP, con el JSON tal como llegó.
-- Una sola tabla genérica para todas las fuentes: raw no interpreta los datos, solo los guarda
-- junto con cuándo, de dónde y cómo se obtuvieron.
--
-- Idempotencia: la misma extracción (misma fuente, fecha de observación y contenido COMPLETO)
-- no se vuelve a insertar (uq_raw_extraccion). Si el contenido cambió durante el día se guarda
-- como una extracción nueva; staging usa la más reciente y raw conserva ambas para auditoría.
CREATE TABLE IF NOT EXISTS raw.api_respuesta (
    respuesta_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fuente             text        NOT NULL CHECK (fuente IN ('minsal_locales', 'minsal_turnos', 'dpa_comunas')),
    extraccion_id      uuid        NOT NULL,              -- agrupa las páginas de una misma extracción
    pagina             integer     NOT NULL DEFAULT 1 CHECK (pagina >= 1),
    total_paginas      integer     NOT NULL DEFAULT 1 CHECK (total_paginas >= pagina),
    endpoint_url       text        NOT NULL,
    parametros         jsonb,                              -- p. ej. los objectIds pedidos a ArcGIS
    http_status        smallint    NOT NULL,
    content_type       text,
    extraido_en        timestamptz NOT NULL,               -- fecha y hora de la extracción (exigido por la pauta)
    fecha_observacion  date        NOT NULL,               -- día (hora de Chile) al que corresponde la foto
    dag_id             text,
    run_id             text,
    n_registros        integer     NOT NULL CHECK (n_registros >= 0),
    pagina_sha256      char(64)    NOT NULL,
    extraccion_sha256  char(64)    NOT NULL,
    contenido          jsonb       NOT NULL,
    CONSTRAINT uq_raw_extraccion UNIQUE (fuente, fecha_observacion, extraccion_sha256, pagina)
);

CREATE INDEX IF NOT EXISTS ix_raw_fuente_fecha
    ON raw.api_respuesta (fuente, fecha_observacion, extraido_en DESC);
CREATE INDEX IF NOT EXISTS ix_raw_extraccion
    ON raw.api_respuesta (extraccion_id, pagina);

COMMENT ON TABLE  raw.api_respuesta IS 'Respuesta original de cada API (una fila por página), sin transformar';
COMMENT ON COLUMN raw.api_respuesta.contenido IS 'Cuerpo JSON recibido, sin modificaciones de contenido';
COMMENT ON COLUMN raw.api_respuesta.fecha_observacion IS 'Fecha (America/Santiago) a la que corresponde la fotografía extraída';
COMMENT ON COLUMN raw.api_respuesta.extraccion_sha256 IS 'SHA-256 de los bytes de todas las páginas de la extracción, en orden';
