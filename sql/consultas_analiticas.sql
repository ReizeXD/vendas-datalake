-- =============================================================================
-- Consultas analíticas sobre a Camada Ouro (Star Schema no PostgreSQL dw_vendas)
--   fato_vendas  (grão: uma linha de item de venda)
--   dim_tempo · dim_loja · dim_produto
-- =============================================================================


-- 1. Cubo desnormalizado (dataset "cubo_vendas_ouro" usado no Superset)
SELECT
    f.id_fato,
    f.cd_venda,
    t.dt_data,
    t.nr_ano,
    t.nr_mes,
    t.nm_mes,
    t.ds_ano_mes,
    t.ds_trimestre,
    t.ds_dia_semana,
    t.fl_fim_semana,
    l.nm_loja,
    l.nm_cidade,
    l.sg_estado,
    l.nm_regiao,
    l.vl_meta_mensal,
    p.nm_produto,
    p.nm_categoria,
    p.ds_estilo,
    f.vl_quantidade,
    f.vl_preco_unit,
    f.vl_final
FROM fato_vendas f
JOIN dim_tempo t   ON f.sk_data = t.sk_data
JOIN dim_loja l    ON f.sk_loja = l.sk_loja
JOIN dim_produto p ON f.sk_produto = p.sk_produto;


-- 2. Indicadores gerais do período
SELECT
    SUM(f.vl_final)                                    AS faturamento_total,
    COUNT(DISTINCT f.cd_venda)                         AS qtd_vendas,
    SUM(f.vl_quantidade)                               AS itens_vendidos,
    ROUND(SUM(f.vl_final) / COUNT(DISTINCT f.cd_venda), 2) AS ticket_medio
FROM fato_vendas f;


-- 3. Faturamento mensal e variação em relação ao mês anterior
WITH mensal AS (
    SELECT t.ds_ano_mes, SUM(f.vl_final) AS faturamento
    FROM fato_vendas f
    JOIN dim_tempo t ON f.sk_data = t.sk_data
    GROUP BY t.ds_ano_mes
)
SELECT
    ds_ano_mes,
    faturamento,
    ROUND(100.0 * (faturamento / LAG(faturamento) OVER (ORDER BY ds_ano_mes) - 1), 1) AS variacao_pct
FROM mensal
ORDER BY ds_ano_mes;


-- 4. Top 5 produtos por faturamento
SELECT
    p.nm_produto,
    p.nm_categoria,
    SUM(f.vl_quantidade) AS itens,
    SUM(f.vl_final)      AS faturamento
FROM fato_vendas f
JOIN dim_produto p ON f.sk_produto = p.sk_produto
GROUP BY p.nm_produto, p.nm_categoria
ORDER BY faturamento DESC
LIMIT 5;


-- 5. Faturamento por região e categoria, com participação dentro da região
SELECT
    l.nm_regiao,
    p.nm_categoria,
    SUM(f.vl_final) AS faturamento,
    ROUND(100.0 * SUM(f.vl_final) / SUM(SUM(f.vl_final)) OVER (PARTITION BY l.nm_regiao), 1) AS participacao_regiao_pct
FROM fato_vendas f
JOIN dim_loja l    ON f.sk_loja = l.sk_loja
JOIN dim_produto p ON f.sk_produto = p.sk_produto
GROUP BY l.nm_regiao, p.nm_categoria
ORDER BY l.nm_regiao, faturamento DESC;


-- 6. Atingimento da meta mensal por loja e mês
--    (a meta é mensal, então a comparação precisa ser feita mês a mês)
SELECT
    l.nm_loja,
    t.ds_ano_mes,
    SUM(f.vl_final)                                 AS faturamento_mes,
    l.vl_meta_mensal,
    ROUND(100.0 * SUM(f.vl_final) / l.vl_meta_mensal, 1) AS atingimento_pct,
    SUM(f.vl_final) >= l.vl_meta_mensal             AS bateu_meta
FROM fato_vendas f
JOIN dim_loja l  ON f.sk_loja = l.sk_loja
JOIN dim_tempo t ON f.sk_data = t.sk_data
GROUP BY l.nm_loja, l.vl_meta_mensal, t.ds_ano_mes
ORDER BY l.nm_loja, t.ds_ano_mes;


-- 7. Ranking de lojas: atingimento médio e quantidade de meses com meta batida
WITH loja_mes AS (
    SELECT
        f.sk_loja,
        t.ds_ano_mes,
        SUM(f.vl_final) AS faturamento_mes
    FROM fato_vendas f
    JOIN dim_tempo t ON f.sk_data = t.sk_data
    GROUP BY f.sk_loja, t.ds_ano_mes
)
SELECT
    l.nm_loja,
    l.nm_regiao,
    ROUND(AVG(lm.faturamento_mes), 2)                                   AS faturamento_medio_mensal,
    l.vl_meta_mensal,
    ROUND(100.0 * AVG(lm.faturamento_mes) / l.vl_meta_mensal, 1)        AS atingimento_medio_pct,
    COUNT(*) FILTER (WHERE lm.faturamento_mes >= l.vl_meta_mensal)      AS meses_meta_batida,
    COUNT(*)                                                            AS meses_com_venda
FROM loja_mes lm
JOIN dim_loja l ON lm.sk_loja = l.sk_loja
GROUP BY l.nm_loja, l.nm_regiao, l.vl_meta_mensal
ORDER BY atingimento_medio_pct DESC;


-- 8. Faturamento médio diário por dia da semana
WITH diario AS (
    SELECT t.sk_data, t.nr_dia_semana, t.ds_dia_semana, COALESCE(SUM(f.vl_final), 0) AS faturamento
    FROM dim_tempo t
    LEFT JOIN fato_vendas f ON f.sk_data = t.sk_data
    GROUP BY t.sk_data, t.nr_dia_semana, t.ds_dia_semana
)
SELECT
    ds_dia_semana,
    ROUND(AVG(faturamento), 2) AS faturamento_medio_dia,
    COUNT(*)                   AS dias
FROM diario
GROUP BY nr_dia_semana, ds_dia_semana
ORDER BY nr_dia_semana;


-- 9. Ticket médio por loja
SELECT
    l.nm_loja,
    l.nm_cidade,
    l.sg_estado,
    COUNT(DISTINCT f.cd_venda)                              AS qtd_vendas,
    ROUND(SUM(f.vl_final) / COUNT(DISTINCT f.cd_venda), 2)  AS ticket_medio
FROM fato_vendas f
JOIN dim_loja l ON f.sk_loja = l.sk_loja
GROUP BY l.nm_loja, l.nm_cidade, l.sg_estado
ORDER BY ticket_medio DESC;
