"""Acesso ao Object Storage (RustFS / S3) compartilhado pelas camadas."""

import io
import logging
import re

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError

from config import S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY

log = logging.getLogger(__name__)

_RE_PARTICAO = re.compile(r"dt_ingestao=(\d{4}-\d{2}-\d{2})/")


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def garantir_buckets(s3, buckets) -> None:
    for bucket in buckets:
        try:
            s3.head_bucket(Bucket=bucket)
        except ClientError:
            s3.create_bucket(Bucket=bucket)
            log.info(f"  ✓ Bucket criado: {bucket}")


def chave_particao(tabela: str, dt_ingestao: str) -> str:
    return f"{tabela}/dt_ingestao={dt_ingestao}/{tabela}.parquet"


def gravar_parquet(s3, df: pd.DataFrame, bucket: str, key: str) -> int:
    table = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    return len(df)


def ler_parquet(s3, bucket: str, key: str) -> pd.DataFrame:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            raise FileNotFoundError(f"Objeto não encontrado: s3://{bucket}/{key}") from exc
        raise
    return pd.read_parquet(io.BytesIO(resp["Body"].read()))


def listar_particoes(s3, bucket: str, tabela: str) -> list[str]:
    """Datas de ingestão disponíveis para uma tabela, em ordem crescente."""
    datas = set()
    paginator = s3.get_paginator("list_objects_v2")
    for pagina in paginator.paginate(Bucket=bucket, Prefix=f"{tabela}/"):
        for obj in pagina.get("Contents", []):
            m = _RE_PARTICAO.search(obj["Key"])
            if m:
                datas.add(m.group(1))
    return sorted(datas)


def resolver_particao(s3, bucket: str, tabela: str, dt_ingestao: str | None) -> str:
    """Usa a data informada ou, se nenhuma for informada, a partição mais recente."""
    if dt_ingestao:
        return dt_ingestao
    particoes = listar_particoes(s3, bucket, tabela)
    if not particoes:
        raise FileNotFoundError(
            f"Nenhuma partição encontrada em s3://{bucket}/{tabela}/ — execute a camada anterior primeiro."
        )
    log.info(f"  ▸ Usando a partição mais recente de {bucket}/{tabela}: dt_ingestao={particoes[-1]}")
    return particoes[-1]
