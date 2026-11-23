import io
import logging
from datetime import date

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError

from config import (
    S3_ENDPOINT,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    BUCKET_BRONZE,
    BUCKET_PRATA,
    BUCKET_OURO,
    FONTES_CSV,
)

log = logging.getLogger(__name__)


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def garantir_buckets(s3):
    for bucket in (BUCKET_BRONZE, BUCKET_PRATA, BUCKET_OURO):
        try:
            s3.head_bucket(Bucket=bucket)
        except ClientError:
            s3.create_bucket(Bucket=bucket)
            log.info(f"  ✓ Bucket criado: {bucket}")


def gravar_parquet(s3, df: pd.DataFrame, bucket: str, s3_key: str) -> int:
    table = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)
    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    return len(df)


def executar_bronze(dt_ingestao: str | None = None) -> list[dict]:
    if dt_ingestao is None:
        dt_ingestao = date.today().isoformat()

    log.info("=" * 60)
    log.info(f" BRONZE — Ingestão bruta ({len(FONTES_CSV)} fontes CSV)")
    log.info(f" Partição: dt_ingestao={dt_ingestao}")
    log.info("=" * 60)

    s3 = get_s3()
    garantir_buckets(s3)
    metricas = []

    for tabela, caminho in FONTES_CSV.items():
        log.info(f"  ▸ Ingerindo {tabela} ({caminho.name})...")
        try:
            if not caminho.exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")

            df = pd.read_csv(caminho)
            df["_dt_ingestao"] = pd.Timestamp.utcnow().isoformat()
            df["_fonte"] = f"csv/{caminho.name}"

            s3_key = f"{tabela}/dt_ingestao={dt_ingestao}/{tabela}.parquet"
            linhas = gravar_parquet(s3, df, BUCKET_BRONZE, s3_key)
            log.info(f"    ✓ {linhas:,} linhas → s3://{BUCKET_BRONZE}/{s3_key}")
            metricas.append({"tabela": tabela, "linhas": linhas, "status": "ok"})
        except Exception as exc:
            log.error(f"  ✗ Erro em {tabela}: {exc}")
            metricas.append({"tabela": tabela, "linhas": 0, "status": "erro", "erro": str(exc)})

    return metricas