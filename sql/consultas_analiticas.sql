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
JOIN dim_produto p ON f.sk_produto = p.sk_produto