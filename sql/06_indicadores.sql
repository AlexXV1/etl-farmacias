-- 06_indicadores.sql — Agregados e indicadores calculados EN SQL para una fecha de observación.
-- Uso: SELECT * FROM mart.calcular_indicadores(DATE '2026-11-01');
--
-- Idempotente: borra y vuelve a insertar la fecha (fecha_id) en las tablas mart. Se ejecuta dentro
-- de la transacción de quien la llama (pipelines/indicadores.calcular): todo o nada.
--
-- Nota PL/pgSQL: las columnas de RETURNS TABLE (codigo, numerador, ...) son VARIABLES dentro de la
-- función. Por eso toda referencia a columnas con esos nombres va calificada (calc.codigo, iv.codigo);
-- sin prefijo PostgreSQL no sabría si es la columna o la variable ("column reference is ambiguous").

CREATE OR REPLACE FUNCTION mart.calcular_indicadores(p_fecha date)
RETURNS TABLE (codigo text, numerador numeric, denominador numeric, valor numeric, cumple_meta boolean)
LANGUAGE plpgsql AS $$
DECLARE
    v_fecha_id integer := to_char(p_fecha, 'YYYYMMDD')::integer;
BEGIN
    -- 1) No se calcula sobre datos que no existen.
    IF NOT EXISTS (SELECT 1 FROM dw.fact_establecimiento_diario f WHERE f.fecha_id = v_fecha_id) THEN
        RAISE EXCEPTION 'No hay hechos cargados para la fecha %', p_fecha;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM dw.dim_territorio t WHERE t.vigente) THEN
        RAISE EXCEPTION 'dim_territorio no tiene comunas vigentes';
    END IF;

    -- 2) Reemplazo de la fecha (idempotencia).
    DELETE FROM mart.indicador_valor        iv WHERE iv.fecha_id = v_fecha_id;
    DELETE FROM mart.establecimientos_region er WHERE er.fecha_id = v_fecha_id;
    DELETE FROM mart.establecimientos_comuna ec WHERE ec.fecha_id = v_fecha_id;

    -- 3) Agregado por comuna. LEFT JOIN DESDE dim_territorio: las comunas sin establecimientos
    --    aparecen con 0 (con INNER JOIN desaparecerían y IND01 sería siempre 100 %).
    INSERT INTO mart.establecimientos_comuna (fecha_id, territorio_sk, cut_com, comuna, cut_reg, region,
        n_establecimientos, n_listado_general, n_turno, n_turno_vigente, ranking_nacional)
    SELECT v_fecha_id, t.territorio_sk, t.cut_com, t.comuna, t.cut_reg, t.region,
           count(f.establecimiento_sk),
           count(f.establecimiento_sk) FILTER (WHERE f.en_listado_general),
           count(f.establecimiento_sk) FILTER (WHERE f.en_turno),
           count(f.establecimiento_sk) FILTER (WHERE f.turno_vigente),
           rank() OVER (ORDER BY count(f.establecimiento_sk) DESC)
    FROM dw.dim_territorio t
    LEFT JOIN dw.fact_establecimiento_diario f
           ON f.territorio_sk = t.territorio_sk AND f.fecha_id = v_fecha_id   -- la fecha va en el ON, no en el WHERE
    WHERE t.vigente
    GROUP BY t.territorio_sk, t.cut_com, t.comuna, t.cut_reg, t.region;

    -- 4) Agregado por región (a partir del agregado por comuna).
    INSERT INTO mart.establecimientos_region (fecha_id, cut_reg, region, n_establecimientos,
        pct_del_total_nacional, comunas_total, comunas_con_establecimiento, pct_cobertura_comunal,
        comunas_con_turno, comunas_con_turno_vigente)
    SELECT v_fecha_id, c.cut_reg, min(c.region),
           sum(c.n_establecimientos),
           round(100.0 * sum(c.n_establecimientos) / NULLIF(sum(sum(c.n_establecimientos)) OVER (), 0), 2),
           count(*),
           count(*) FILTER (WHERE c.n_establecimientos > 0),
           round(100.0 * count(*) FILTER (WHERE c.n_establecimientos > 0) / count(*), 2),
           count(*) FILTER (WHERE c.n_turno > 0),
           count(*) FILTER (WHERE c.n_turno_vigente > 0)
    FROM mart.establecimientos_comuna c
    WHERE c.fecha_id = v_fecha_id
    GROUP BY c.cut_reg;

    -- 5) Indicadores: se guardan numerador y denominador para poder auditar cada valor.
    WITH c AS (SELECT * FROM mart.establecimientos_comuna WHERE fecha_id = v_fecha_id),
    top AS (   -- el 10 % de comunas (redondeado hacia arriba) con más establecimientos
        SELECT n_establecimientos FROM c
        ORDER BY n_establecimientos DESC
        LIMIT (SELECT ceil(0.10 * count(*))::int FROM c)
    ),
    f AS (SELECT * FROM dw.fact_establecimiento_diario WHERE fecha_id = v_fecha_id),
    r AS (SELECT * FROM mart.establecimientos_region WHERE fecha_id = v_fecha_id),
    calc(codigo, numerador, denominador) AS (
        -- IND01 Cobertura comunal: comunas con >= 1 establecimiento / comunas vigentes
        SELECT 'IND01', (SELECT count(*) FROM c WHERE n_establecimientos > 0)::numeric,
                        (SELECT count(*) FROM c)::numeric
        UNION ALL
        -- IND02 Concentración: establecimientos en el top 10 % de comunas / establecimientos con comuna
        SELECT 'IND02', (SELECT coalesce(sum(n_establecimientos), 0) FROM top)::numeric,
                        (SELECT sum(n_establecimientos) FROM c)::numeric
        UNION ALL
        -- IND03 Cobertura mínima regional: la región con menor % de comunas cubiertas
        SELECT 'IND03', (SELECT comunas_con_establecimiento FROM r ORDER BY pct_cobertura_comunal, cut_reg LIMIT 1)::numeric,
                        (SELECT comunas_total FROM r ORDER BY pct_cobertura_comunal, cut_reg LIMIT 1)::numeric
        UNION ALL
        -- IND04 Cobertura de turnos: comunas con turno VIGENTE / comunas con >= 1 establecimiento
        SELECT 'IND04', (SELECT count(*) FROM c WHERE n_turno_vigente > 0)::numeric,
                        (SELECT count(*) FROM c WHERE n_establecimientos > 0)::numeric
        UNION ALL
        -- IND05 Calidad del cruce: establecimientos con comuna asignada / establecimientos observados
        SELECT 'IND05', (SELECT count(*) FROM f WHERE territorio_sk <> 0)::numeric,
                        (SELECT count(*) FROM f)::numeric
    )
    INSERT INTO mart.indicador_valor (fecha_id, codigo, numerador, denominador, valor, cumple_meta)
    SELECT v_fecha_id, calc.codigo, calc.numerador, calc.denominador,
           round(100.0 * calc.numerador / calc.denominador, 2),
           CASE i.sentido_meta
               WHEN 'mayor_o_igual' THEN round(100.0 * calc.numerador / calc.denominador, 2) >= i.meta_valor
               ELSE round(100.0 * calc.numerador / calc.denominador, 2) <= i.meta_valor
           END
    FROM calc JOIN mart.indicador i ON i.codigo = calc.codigo
    WHERE calc.denominador > 0;   -- sin denominador el indicador no se define (el control lo detecta)

    RETURN QUERY
        SELECT iv.codigo, iv.numerador, iv.denominador, iv.valor, iv.cumple_meta
        FROM mart.indicador_valor iv WHERE iv.fecha_id = v_fecha_id ORDER BY iv.codigo;
END;
$$;

-- Vista para el informe: valor, meta y si se cumple, por fecha.
CREATE OR REPLACE VIEW mart.v_indicadores AS
SELECT d.fecha, i.codigo, i.nombre, v.numerador, v.denominador, v.valor, i.unidad,
       CASE i.sentido_meta WHEN 'mayor_o_igual' THEN '>= ' ELSE '<= ' END || i.meta_valor AS meta,
       v.cumple_meta, v.calculado_en
FROM mart.indicador_valor v
JOIN mart.indicador i USING (codigo)
JOIN dw.dim_fecha d USING (fecha_id);

-- Estado de actualización: si la última fecha con hechos es posterior a la última con indicadores,
-- el DAG de indicadores no corrió (o se omitió) para esa fecha.
CREATE OR REPLACE VIEW mart.v_estado_indicadores AS
SELECT (SELECT max(d.fecha) FROM dw.fact_establecimiento_diario f JOIN dw.dim_fecha d USING (fecha_id)) AS ultima_fecha_hechos,
       (SELECT max(d.fecha) FROM mart.indicador_valor v JOIN dw.dim_fecha d USING (fecha_id))           AS ultima_fecha_indicadores,
       (SELECT max(fecha_observacion) FROM staging.dpa_comuna WHERE vigente)                             AS ultima_fecha_catalogo_dpa;
