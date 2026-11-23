import io
import logging
from datetime import date

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config

from config import (
    S3_ENDPOINT,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    BUCKET_BRONZE,
    BUCKET_PRATA,
)

log = logging.getLogger(__name__)

MAPA_CATEGORIA = {
    "Camisa": "Vestuário Superior",
    "Camiseta": "Vestuário Superior",
    "Polo": "Vestuário Superior",
    "Casaco": "Vestuário Superior",
    "Terno": "Vestuário Superior",
    "Calça": "Vestuário Inferior",
    "Bermuda": "Vestuário Inferior",
    "Short": "Vestuário Inferior",
    "Sapato": "Calçados",
    "Tênis": "Calçados",
    "Chinelo": "Calçados",
    "Relógio": "Acessórios",
    "Cinto": "Acessórios",
    "Mochila": "Acessórios",
    "Gorro": "Acessórios",
    "Pulseira": "Acessórios",
    "Cueca": "Moda Íntima & Praia",
    "Meia": "Moda Íntima & Praia",
    "Sunga": "Moda Íntima & Praia",
}

ESTILOS = ("Estampa", "Linho", "Liso", "Listrado", "Xadrez")


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def ler_bronze(s3, tabela: str, dt_ingestao: str) -> pd.DataFrame:
    s3_key = f"{tabela}/dt_ingestao={dt_ingestao}/{tabela}.parquet"
    resp = s3.get_object(Bucket=BUCKET_BRONZE, Key=s3_key)
    return pd.read_parquet(io.BytesIO(resp["Body"].read()))


def gravar_prata(s3, df: pd.DataFrame, tabela: str, dt_ingestao: str) -> int:
    s3_key = f"{tabela}/dt_ingestao={dt_ingestao}/{tabela}.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    s3.put_object(Bucket=BUCKET_PRATA, Key=s3_key, Body=buf.getvalue())
    log.info(f"    ✓ {len(df):,} linhas → s3://{BUCKET_PRATA}/{s3_key}")
    return len(df)


def classificar_produto(nome: str) -> tuple[str, str]:
    primeira_palavra = nome.split()[0]
    categoria = MAPA_CATEGORIA.get(primeira_palavra, "Outros")
    estilo = next((e for e in ESTILOS if nome.endswith(e)), "Padrão")
    return categoria, estilo


def executar_prata(dt_ingestao: str | None = None) -> list[dict]:
    if dt_ingestao is None:
        dt_ingestao = date.today().isoformat()

    log.info("=" * 60)
    log.info(" PRATA — Saneamento, deduplicação e integração")
    log.info(f" Partição: dt_ingestao={dt_ingestao}")
    log.info("=" * 60)

    s3 = get_s3()
    metricas = []

    try:
        # 1. Leitura da Bronze
        df_vendas = ler_bronze(s3, "vendas", dt_ingestao)
        df_lojas = ler_bronze(s3, "lojas", dt_ingestao)
        df_metas = ler_bronze(s3, "metas_lojas", dt_ingestao)

        # 2. Saneamento de Lojas e Metas (Integração de cadastro)
        for df in (df_lojas, df_metas):
            df["id_loja"] = df["id_loja"].astype(str).str.strip()

        df_lojas["cidade"] = df_lojas["cidade"].str.strip()
        df_lojas["estado"] = df_lojas["estado"].str.strip().str.upper()
        df_lojas["regiao"] = df_lojas["regiao"].str.strip()
        df_metas["meta_mensal"] = df_metas["meta_mensal"].astype(float).round(2)

        lojas_integradas = (
            df_lojas[["id_loja", "cidade", "estado", "regiao"]]
            .drop_duplicates(subset=["id_loja"])
            .merge(
                df_metas[["id_loja", "meta_mensal"]].drop_duplicates(subset=["id_loja"]),
                on="id_loja",
                how="left",
            )
        )
        linhas_lojas = gravar_prata(s3, lojas_integradas, "lojas_integradas", dt_ingestao)
        metricas.append({"tabela": "lojas_integradas", "linhas": linhas_lojas, "status": "ok"})

        # 3. Saneamento de Vendas
        colunas_negocio = [
            "codigo_venda",
            "data",
            "id_loja",
            "produto",
            "quantidade",
            "valor_unitario",
            "valor_final",
        ]
        vendas = df_vendas[colunas_negocio].copy()
        vendas["id_loja"] = vendas["id_loja"].astype(str).str.strip()
        vendas["produto"] = vendas["produto"].astype(str).str.strip()
        vendas["data"] = pd.to_datetime(vendas["data"]).dt.date
        vendas["codigo_venda"] = vendas["codigo_venda"].astype(int)
        vendas["quantidade"] = vendas["quantidade"].astype(int)
        vendas["valor_unitario"] = vendas["valor_unitario"].astype(float).round(2)
        vendas["valor_final"] = (vendas["quantidade"] * vendas["valor_unitario"]).round(2)

        antes = len(vendas)
        vendas = vendas.drop_duplicates().reset_index(drop=True)
        removidas = antes - len(vendas)
        log.info(f"  ▸ Duplicatas exatas removidas em vendas: {removidas} ({antes:,} → {len(vendas):,})")

        # 4. Enriquecimento de Produto + Integração com Lojas
        classificacao = vendas["produto"].apply(classificar_produto)
        vendas["categoria"] = [c[0] for c in classificacao]
        vendas["estilo"] = [c[1] for c in classificacao]

        vendas_integradas = vendas.merge(lojas_integradas, on="id_loja", how="left")
        linhas_vendas = gravar_prata(s3, vendas_integradas, "vendas_integradas", dt_ingestao)
        metricas.append({"tabela": "vendas_integradas", "linhas": linhas_vendas, "status": "ok"})

    except Exception as exc:
        log.error(f"  ✗ Erro na camada Prata: {exc}")
        metricas.append({"tabela": "prata", "linhas": 0, "status": "erro", "erro": str(exc)})

    return metricas