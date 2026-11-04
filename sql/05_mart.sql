-- 05_mart.sql — Capa mart: agregados e indicadores. Se recalculan por fecha
-- (DELETE + INSERT de la fecha dentro de una transacción) => idempotente.

-- Conteo por comuna. Incluye TODAS las comunas vigentes de la DPA, también las que tienen 0
-- (se calcula con LEFT JOIN desde dim_territorio; ver 06_indicadores.sql).
CREATE TABLE IF NOT EXISTS mart.establecimientos_comuna (
    fecha_id             integer  NOT NULL REFERENCES dw.dim_fecha (fecha_id),
    territorio_sk        smallint NOT NULL REFERENCES dw.dim_territorio (territorio_sk),
    cut_com              char(5)  NOT NULL,
    comuna               text     NOT NULL,
    cut_reg              char(2)  NOT NULL,
    region               text     NOT NULL,
    n_establecimientos   integer  NOT NULL CHECK (n_establecimientos >= 0),
    n_listado_general    integer  NOT NULL CHECK (n_listado_general >= 0),
    n_turno              integer  NOT NULL CHECK (n_turno >= 0),           -- publicados en turnos
    n_turno_vigente      integer  NOT NULL DEFAULT 0 CHECK (n_turno_vigente >= 0), -- y con fecha del día
    ranking_nacional     integer  NOT NULL,
    PRIMARY KEY (fecha_id, territorio_sk)
);

CREATE TABLE IF NOT EXISTS mart.establecimientos_region (
    fecha_id                     integer      NOT NULL REFERENCES dw.dim_fecha (fecha_id),
    cut_reg                      char(2)      NOT NULL,
    region                       text         NOT NULL,
    n_establecimientos           integer      NOT NULL,
    pct_del_total_nacional       numeric(6,2) NOT NULL,
    comunas_total                integer      NOT NULL,
    comunas_con_establecimiento  integer      NOT NULL,
    pct_cobertura_comunal        numeric(6,2) NOT NULL,
    comunas_con_turno            integer      NOT NULL,
    comunas_con_turno_vigente    integer      NOT NULL DEFAULT 0,
    PRIMARY KEY (fecha_id, cut_reg)
);

-- Ficha de cada indicador (lo que pide la pauta: nombre, objetivo, fórmula, fuente, periodicidad, meta).
CREATE TABLE IF NOT EXISTS mart.indicador (
    codigo            text PRIMARY KEY,
    nombre            text    NOT NULL,
    objetivo          text    NOT NULL,
    formula           text    NOT NULL,
    fuentes           text    NOT NULL,
    periodicidad      text    NOT NULL,
    unidad            text    NOT NULL,
    meta_valor        numeric NOT NULL,
    sentido_meta      text    NOT NULL CHECK (sentido_meta IN ('mayor_o_igual', 'menor_o_igual')),
    meta_fundamento   text    NOT NULL
);

-- Valor diario de cada indicador, con numerador y denominador para poder auditar el cálculo.
CREATE TABLE IF NOT EXISTS mart.indicador_valor (
    fecha_id      integer NOT NULL REFERENCES dw.dim_fecha (fecha_id),
    codigo        text    NOT NULL REFERENCES mart.indicador (codigo),
    numerador     numeric NOT NULL,
    denominador   numeric NOT NULL CHECK (denominador > 0),
    valor         numeric(8,2) NOT NULL,
    cumple_meta   boolean NOT NULL,
    calculado_en  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (fecha_id, codigo)
);

-- Metas: no existe norma oficial que fije estos valores; son UMBRALES ANALÍTICOS PROPUESTOS por el
-- equipo, definidos ANTES de cargar los datos y justificados en meta_fundamento.
INSERT INTO mart.indicador VALUES
('IND01', 'Cobertura comunal de establecimientos registrados',
 'Identificar comunas sin ningún establecimiento farmacéutico registrado en Farmanet para priorizar su revisión (registro incompleto o ausencia real de oferta).',
 'comunas vigentes DPA con >= 1 establecimiento / comunas vigentes DPA * 100',
 'MINSAL Farmanet (getLocales + getLocalesTurnos) + DPA SUBDERE', 'Diaria', '%', 90, 'mayor_o_igual',
 'Umbral analítico propuesto: el ideal es 100 %; 90 % deja un margen de 10 % (≈35 comunas) para comunas rurales o insulares de baja población. Bajo ese margen la falta de registros se considera una alerta territorial.'),
('IND02', 'Concentración en el 10 % de comunas con más establecimientos',
 'Medir si la oferta registrada se concentra en pocas comunas, como insumo para políticas de distribución territorial.',
 'establecimientos en las ceil(10 % de comunas vigentes) comunas con más establecimientos / establecimientos con comuna asignada * 100',
 'MINSAL Farmanet + DPA SUBDERE', 'Diaria', '%', 50, 'menor_o_igual',
 'Umbral analítico propuesto: que un décimo de las comunas concentre más de la mitad de los establecimientos se interpreta como concentración alta.'),
('IND03', 'Cobertura comunal mínima entre regiones',
 'Detectar la región con menor proporción de comunas cubiertas, para focalizar acciones regionales.',
 'min sobre regiones de (comunas de la región con >= 1 establecimiento / comunas de la región * 100)',
 'MINSAL Farmanet + DPA SUBDERE', 'Diaria', '%', 50, 'mayor_o_igual',
 'Umbral analítico propuesto: ninguna región debería tener menos de la mitad de sus comunas sin establecimientos registrados.'),
('IND04', 'Cobertura diaria de turnos',
 'Verificar en cuántas comunas con oferta registrada existe al menos una farmacia de turno publicada para el día.',
 'comunas con >= 1 establecimiento de turno vigente (fecha del turno = fecha de observación) / comunas con >= 1 establecimiento * 100',
 'MINSAL Farmanet (getLocalesTurnos) + DPA SUBDERE', 'Diaria', '%', 80, 'mayor_o_igual',
 'Umbral analítico propuesto: los turnos rotan entre comunas cercanas, por lo que no se exige 100 %; bajo 80 % se considera baja disponibilidad publicada.'),
('IND05', 'Correspondencia territorial válida',
 'Asegurar que los indicadores territoriales se basen en datos confiables; un valor bajo invalida la lectura de IND01–IND04.',
 'establecimientos con CUT asignado (exacto o alias) / establecimientos observados * 100',
 'MINSAL Farmanet + DPA SUBDERE', 'Diaria', '%', 98, 'mayor_o_igual',
 'Umbral de calidad de datos propuesto: hasta 2 % de registros sin comuna confiable no altera materialmente los conteos comunales.')
ON CONFLICT (codigo) DO UPDATE SET
    nombre = EXCLUDED.nombre, objetivo = EXCLUDED.objetivo, formula = EXCLUDED.formula,
    fuentes = EXCLUDED.fuentes, periodicidad = EXCLUDED.periodicidad, unidad = EXCLUDED.unidad,
    meta_valor = EXCLUDED.meta_valor, sentido_meta = EXCLUDED.sentido_meta,
    meta_fundamento = EXCLUDED.meta_fundamento;
