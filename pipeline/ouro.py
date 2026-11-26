import logging

import pandas as pd
import sqlalchemy
from sqlalchemy import BigInteger, Boolean, Date, Integer, Numeric, SmallInteger, Text

from config import BUCKET_PRATA, BUCKET_OURO, POSTGRES_URI
from storage import chave_particao, get_s3, gravar_parquet, ler_parquet, resolver_particao

log = logging.getLogger(__name__)

MESES_PT = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}

DIAS_PT = {
    0: "Segunda", 1: "Terça", 2: "Quarta",
    3: "Quinta", 4: "Sexta", 5: "Sábado", 6: "Domingo",
}

DINHEIRO = Numeric(12, 2)

# Tipos explícitos no PostgreSQL (valores monetários em NUMERIC, não em ponto flutuante)
TIPOS_SQL = {
    "dim_tempo": {
        "sk_data": Integer(), "dt_data": Date(), "nr_ano": SmallInteger(),
        "nr_mes": SmallInteger(), "nr_dia": SmallInteger(), "nr_trimestre": SmallInteger(),
        "ds_trimestre": Text(), "nm_mes": Text(), "ds_ano_mes": Text(),
        "nr_dia_semana": SmallInteger(), "ds_dia_semana": Text(), "fl_fim_semana": Boolean(),
    },
    "dim_loja": {
        "sk_loja": Integer(), "nm_loja": Text(), "nm_cidade": Text(),
        "sg_estado": Text(), "nm_regiao": Text(), "vl_meta_mensal": DINHEIRO,
    },
    "dim_produto": {
        "sk_produto": Integer(), "nm_produto": Text(), "nm_categoria": Text(), "ds_estilo": Text(),
    },
    "fato_vendas": {
        "id_fato": BigInteger(), "sk_data": Integer(), "sk_loja": Integer(),
        "sk_produto": Integer(), "cd_venda": BigInteger(), "vl_quantidade": Integer(),
        "vl_preco_unit": DINHEIRO, "vl_final": DINHEIRO,
    },
}

# Restrições do Star Schema, aplicadas após a carga
DDL_RESTRICOES = [
    "ALTER TABLE dim_tempo   ADD CONSTRAINT pk_dim_tempo   PRIMARY KEY (sk_data)",
    "ALTER TABLE dim_loja    ADD CONSTRAINT pk_dim_loja    PRIMARY KEY (sk_loja)",
    "ALTER TABLE dim_produto ADD CONSTRAINT pk_dim_produto PRIMARY KEY (sk_produto)",
    "ALTER TABLE fato_vendas ADD CONSTRAINT pk_fato_vendas PRIMARY KEY (id_fato)",
    "ALTER TABLE fato_vendas ADD CONSTRAINT fk_fato_tempo   FOREIGN KEY (sk_data)    REFERENCES dim_tempo (sk_data)",
    "ALTER TABLE fato_vendas ADD CONSTRAINT fk_fato_loja    FOREIGN KEY (sk_loja)    REFERENCES dim_loja (sk_loja)",
    "ALTER TABLE fato_vendas ADD CONSTRAINT fk_fato_produto FOREIGN KEY (sk_produto) REFERENCES dim_produto (sk_produto)",
    "CREATE INDEX ix_fato_vendas_sk_data    ON fato_vendas (sk_data)",
    "CREATE INDEX ix_fato_vendas_sk_loja    ON fato_vendas (sk_loja)",
    "CREATE INDEX ix_fato_vendas_sk_produto ON fato_vendas (sk_produto)",
]


def get_engine():
    return sqlalchemy.create_engine(POSTGRES_URI)


def ler_prata(s3, tabela: str, dt_ingestao: str) -> pd.DataFrame:
    return ler_parquet(s3, BUCKET_PRATA, chave_particao(tabela, dt_ingestao))


def carregar_postgres(engine, tabelas: dict[str, pd.DataFrame]) -> None:
    """Recarrega o Star Schema no PostgreSQL em uma única transação.

    Se qualquer etapa falhar, nada é alterado no banco (o Superset continua
    enxergando a carga anterior completa).
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "DROP TABLE IF EXISTS fato_vendas, dim_tempo, dim_loja, dim_produto CASCADE"
        )
        for nome, df in tabelas.items():
            df.to_sql(nome, conn, if_exists="fail", index=False, dtype=TIPOS_SQL[nome])
            log.info(f"    ✓ {len(df):,} linhas → postgres.{nome}")
        for ddl in DDL_RESTRICOES:
            conn.exec_driver_sql(ddl)
        log.info("    ✓ Chaves primárias, estrangeiras e índices criados")


def construir_dim_tempo(datas: pd.Series) -> pd.DataFrame:
    datas_dt = pd.to_datetime(datas)
    todas = pd.date_range(start=datas_dt.min(), end=datas_dt.max(), freq="D")

    registros = []
    for dt in todas:
        trimestre = (dt.month - 1) // 3 + 1
        registros.append({
            "sk_data": int(dt.strftime("%Y%m%d")),
            "dt_data": dt.date(),
            "nr_ano": dt.year,
            "nr_mes": dt.month,
            "nr_dia": dt.day,
            "nr_trimestre": trimestre,
            "ds_trimestre": f"T{trimestre}/{dt.year}",
            "nm_mes": MESES_PT[dt.month],
            "ds_ano_mes": dt.strftime("%Y-%m"),
            "nr_dia_semana": dt.weekday(),
            "ds_dia_semana": DIAS_PT[dt.weekday()],
            "fl_fim_semana": dt.weekday() >= 5,
        })
    return pd.DataFrame(registros)


def construir_dim_loja(lojas_prata: pd.DataFrame) -> pd.DataFrame:
    df = lojas_prata.sort_values("id_loja").reset_index(drop=True)
    return pd.DataFrame({
        "sk_loja": df.index + 1,
        "nm_loja": df["id_loja"],
        "nm_cidade": df["cidade"],
        "sg_estado": df["estado"],
        "nm_regiao": df["regiao"],
        "vl_meta_mensal": df["meta_mensal"],
    })


def construir_dim_produto(vendas_prata: pd.DataFrame) -> pd.DataFrame:
    produtos = (
        vendas_prata[["produto", "categoria", "estilo"]]
        .drop_duplicates(subset=["produto"])
        .sort_values("produto")
        .reset_index(drop=True)
    )
    return pd.DataFrame({
        "sk_produto": produtos.index + 1,
        "nm_produto": produtos["produto"],
        "nm_categoria": produtos["categoria"],
        "ds_estilo": produtos["estilo"],
    })


def construir_fato_vendas(
    vendas_prata: pd.DataFrame,
    dim_loja: pd.DataFrame,
    dim_produto: pd.DataFrame,
) -> pd.DataFrame:
    mapa_loja = dim_loja.set_index("nm_loja")["sk_loja"].to_dict()
    mapa_produto = dim_produto.set_index("nm_produto")["sk_produto"].to_dict()

    df = vendas_prata.copy()
    df["sk_data"] = pd.to_datetime(df["data"]).dt.strftime("%Y%m%d").astype(int)
    df["sk_loja"] = df["id_loja"].map(mapa_loja)
    df["sk_produto"] = df["produto"].map(mapa_produto)
    df = df.reset_index(drop=True)

    sem_chave = df[["sk_loja", "sk_produto"]].isna().any(axis=1)
    if sem_chave.any():
        raise ValueError(f"{int(sem_chave.sum())} linhas da fato sem chave de dimensão (loja/produto)")

    return pd.DataFrame({
        "id_fato": df.index + 1,
        "sk_data": df["sk_data"],
        "sk_loja": df["sk_loja"].astype(int),
        "sk_produto": df["sk_produto"].astype(int),
        "cd_venda": df["codigo_venda"],
        "vl_quantidade": df["quantidade"],
        "vl_preco_unit": df["valor_unitario"],
        "vl_final": df["valor_final"],
    })


def executar_ouro(dt_ingestao: str | None = None) -> list[dict]:
    log.info("=" * 60)
    log.info(" OURO — Modelagem Dimensional (Star Schema)")
    log.info("=" * 60)

    s3 = get_s3()
    metricas = []

    try:
        dt_ingestao = resolver_particao(s3, BUCKET_PRATA, "vendas_integradas", dt_ingestao)
        vendas_prata = ler_prata(s3, "vendas_integradas", dt_ingestao)
        lojas_prata = ler_prata(s3, "lojas_integradas", dt_ingestao)

        dim_tempo = construir_dim_tempo(vendas_prata["data"])
        dim_loja = construir_dim_loja(lojas_prata)
        dim_produto = construir_dim_produto(vendas_prata)
        fato_vendas = construir_fato_vendas(vendas_prata, dim_loja, dim_produto)

        tabelas = {
            "dim_tempo": dim_tempo,
            "dim_loja": dim_loja,
            "dim_produto": dim_produto,
            "fato_vendas": fato_vendas,
        }

        # 1. Serving (PostgreSQL) em transação única; 2. Parquet no bucket ouro.
        # O Parquet só é atualizado depois que o banco foi carregado com sucesso.
        log.info("  ▸ Carregando Star Schema no PostgreSQL (transação única)...")
        carregar_postgres(get_engine(), tabelas)

        log.info("  ▸ Gravando Parquet no bucket ouro...")
        for nome, df in tabelas.items():
            key = f"{nome}/{nome}.parquet"
            linhas = gravar_parquet(s3, df, BUCKET_OURO, key)
            log.info(f"    ✓ {linhas:,} linhas → s3://{BUCKET_OURO}/{key}")
            metricas.append({"tabela": nome, "linhas": linhas, "status": "ok"})

        log.info("\n  Resumo Fato Vendas (Camada Ouro):")
        log.info(f"    Registros:     {len(fato_vendas):,}")
        log.info(f"    Faturamento:   R$ {fato_vendas['vl_final'].sum():,.2f}")
        log.info(f"    Itens totais:  {fato_vendas['vl_quantidade'].sum():,}")

    except Exception as exc:
        log.error(f"  ✗ Erro na camada Ouro: {exc}")
        metricas.append({"tabela": "ouro", "linhas": 0, "status": "erro", "erro": str(exc)})

    return metricas
