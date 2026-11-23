import io
import logging
from datetime import date

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy
from botocore.config import Config

from config import (
    S3_ENDPOINT,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    BUCKET_PRATA,
    BUCKET_OURO,
    POSTGRES_URI,
)

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


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def get_engine():
    return sqlalchemy.create_engine(POSTGRES_URI)


def ler_prata(s3, tabela: str, dt_ingestao: str) -> pd.DataFrame:
    key = f"{tabela}/dt_ingestao={dt_ingestao}/{tabela}.parquet"
    resp = s3.get_object(Bucket=BUCKET_PRATA, Key=key)
    return pd.read_parquet(io.BytesIO(resp["Body"].read()))


def gravar_ouro(s3, engine, df: pd.DataFrame, tabela: str) -> int:
    # 1. Grava Parquet no Object Storage (bucket ouro)
    key = f"{tabela}/{tabela}.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    s3.put_object(Bucket=BUCKET_OURO, Key=key, Body=buf.getvalue())

    # 2. Carrega no PostgreSQL (dw_vendas) para consumo do Superset
    df.to_sql(tabela, engine, if_exists="replace", index=False)

    log.info(f"    ✓ {len(df):,} linhas → s3://{BUCKET_OURO}/{key} & postgres.{tabela}")
    return len(df)


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

    return pd.DataFrame({
        "id_fato": df.index + 1,
        "sk_data": df["sk_data"],
        "sk_loja": df["sk_loja"],
        "sk_produto": df["sk_produto"],
        "cd_venda": df["codigo_venda"],
        "vl_quantidade": df["quantidade"],
        "vl_preco_unit": df["valor_unitario"],
        "vl_final": df["valor_final"],
    })


def executar_ouro(dt_ingestao: str | None = None) -> list[dict]:
    if dt_ingestao is None:
        dt_ingestao = date.today().isoformat()

    log.info("=" * 60)
    log.info(" OURO — Modelagem Dimensional (Star Schema)")
    log.info("=" * 60)

    s3 = get_s3()
    engine = get_engine()
    metricas = []

    try:
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

        for nome, df in tabelas.items():
            log.info(f"  ▸ Persistindo {nome}...")
            linhas = gravar_ouro(s3, engine, df, nome)
            metricas.append({"tabela": nome, "linhas": linhas, "status": "ok"})

        log.info("\n  Resumo Fato Vendas (Camada Ouro):")
        log.info(f"    Registros:     {len(fato_vendas):,}")
        log.info(f"    Faturamento:   R$ {fato_vendas['vl_final'].sum():,.2f}")
        log.info(f"    Itens totais:  {fato_vendas['vl_quantidade'].sum():,}")

    except Exception as exc:
        log.error(f"  ✗ Erro na camada Ouro: {exc}")
        metricas.append({"tabela": "ouro", "linhas": 0, "status": "erro", "erro": str(exc)})

    return metricas