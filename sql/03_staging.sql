-- 03_staging.sql — Capa staging: datos limpios y normalizados. Las reglas de calidad
-- críticas se declaran como CHECK para que la base rechace lo que el código dejara pasar.

-- Catálogo territorial vigente (DPA SUBDERE). Clave natural: código CUT de comuna.
CREATE TABLE IF NOT EXISTS staging.dpa_comuna (
    cut_com             char(5)     PRIMARY KEY CHECK (cut_com ~ '^[0-9]{5}$'),
    cut_prov            char(3)     NOT NULL,
    cut_reg             char(2)     NOT NULL,
    comuna              text        NOT NULL,
    provincia           text        NOT NULL,
    region              text        NOT NULL,
    comuna_normalizada  text        NOT NULL,              -- sin tildes, mayúsculas: llave del cruce con MINSAL
    vigente             boolean     NOT NULL DEFAULT true, -- false si la comuna deja de venir en la API
    fuente_url          text        NOT NULL,
    extraccion_id       uuid        NOT NULL,
    fecha_observacion   date        NOT NULL,
    actualizado_en      timestamptz NOT NULL DEFAULT now(),
    -- Jerarquía CUT: 05801 -> provincia 058 -> región 05
    CONSTRAINT ck_dpa_jerarquia CHECK (cut_prov = left(cut_com, 3) AND cut_reg = left(cut_com, 2))
);
-- Un nombre normalizado identifica una única comuna vigente (si no, el cruce sería ambiguo).
CREATE UNIQUE INDEX IF NOT EXISTS uq_dpa_nombre_vigente
    ON staging.dpa_comuna (comuna_normalizada) WHERE vigente;

-- Fotografía diaria de establecimientos MINSAL (unión de listado general y turnos).
CREATE TABLE IF NOT EXISTS staging.minsal_establecimiento (
    fecha_observacion       date        NOT NULL,
    local_id                integer     NOT NULL CHECK (local_id > 0),
    nombre                  text        NOT NULL,
    tipo_establecimiento    text        NOT NULL CHECK (tipo_establecimiento IN
                                ('farmacia', 'botiquin', 'almacen_farmaceutico', 'vacunatorio')),
    direccion               text,
    telefono                text,
    localidad               text,
    comuna_origen           text        NOT NULL,          -- tal como venía de MINSAL
    comuna_normalizada      text        NOT NULL,
    fk_region_minsal        smallint    NOT NULL,          -- código INTERNO de MINSAL (no es el CUT)
    fk_comuna_minsal        integer,
    latitud                 numeric(10, 7),
    longitud                numeric(10, 7),
    estado_coordenada       text        NOT NULL CHECK (estado_coordenada IN
                                ('valida', 'corregida', 'vacia', 'formato_invalido', 'fuera_de_rango')),
    fecha_fuente            date,
    en_listado_general      boolean     NOT NULL,          -- vino en getLocales
    en_turno                boolean     NOT NULL,          -- vino en getLocalesTurnos
    turno_vigente           boolean     NOT NULL DEFAULT false,
    cut_com                 char(5),
    estado_correspondencia  text        NOT NULL CHECK (estado_correspondencia IN
                                ('exacto', 'alias', 'sin_correspondencia', 'region_inconsistente', 'catalogo_no_disponible')),
    extraccion_ids          uuid[]      NOT NULL,          -- trazabilidad hacia raw
    cargado_en              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (fecha_observacion, local_id),
    -- Todo establecimiento viene de al menos un endpoint.
    CONSTRAINT ck_establecimiento_origen CHECK (en_listado_general OR en_turno),
    -- Lat y lng van juntas, y solo existen si la coordenada es válida o fue corregida.
    CONSTRAINT ck_coordenadas CHECK ((latitud IS NULL) = (longitud IS NULL)
                                     AND ((latitud IS NOT NULL) = (estado_coordenada IN ('valida', 'corregida')))),
    -- Hay CUT si y solo si el cruce territorial fue confiable.
    CONSTRAINT ck_correspondencia CHECK ((cut_com IS NOT NULL) = (estado_correspondencia IN ('exacto', 'alias'))),
    -- Un turno solo puede estar vigente si el local vino en el endpoint de turnos.
    CONSTRAINT ck_turno_vigente CHECK (NOT turno_vigente OR en_turno)
);
CREATE INDEX IF NOT EXISTS ix_staging_est_estado
    ON staging.minsal_establecimiento (fecha_observacion, estado_correspondencia);

-- Registros que no pudieron pasar a staging, con su motivo. Nunca se descartan en silencio.
CREATE TABLE IF NOT EXISTS staging.rechazo (
    rechazo_id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fuente             text        NOT NULL,
    fecha_observacion  date        NOT NULL,
    clave_origen       text        NOT NULL,   -- local_id / CUT tal como venía ('' si no venía)
    motivo             text        NOT NULL,
    registro           jsonb       NOT NULL,   -- el registro original completo
    extraccion_id      uuid,
    detectado_en       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_rechazo UNIQUE (fuente, fecha_observacion, clave_origen, motivo)
);

COMMENT ON COLUMN staging.minsal_establecimiento.turno_vigente IS 'en_turno y fecha_fuente = fecha_observacion (excluye turnos publicados con fecha antigua)';
COMMENT ON COLUMN staging.minsal_establecimiento.tipo_establecimiento IS 'Inferido del nombre comercial (no viene en la API)';
COMMENT ON COLUMN staging.minsal_establecimiento.estado_coordenada IS 'corregida = valor obtenido tras limpiar caracteres espurios del original';
COMMENT ON COLUMN staging.minsal_establecimiento.fecha_fuente IS 'Campo "fecha" de la API (formatos dd-mm-yy o yyyy-mm-dd unificados)';
