import logging
from datetime import date

import pandas as pd

from config import BUCKET_BRONZE, BUCKET_PRATA, BUCKET_OURO, FONTES_CSV
from storage import chave_particao, garantir_buckets, get_s3, gravar_parquet

log = logging.getLogger(__name__)


def executar_bronze(dt_ingestao: str | None = None) -> list[dict]:
    if dt_ingestao is None:
        dt_ingestao = date.today().isoformat()

    log.info("=" * 60)
    log.info(f" BRONZE — Ingestão bruta ({len(FONTES_CSV)} fontes CSV)")
    log.info(f" Partição: dt_ingestao={dt_ingestao}")
    log.info("=" * 60)

    s3 = get_s3()
    garantir_buckets(s3, (BUCKET_BRONZE, BUCKET_PRATA, BUCKET_OURO))
    metricas = []
    ts_ingestao = pd.Timestamp.now(tz="UTC")

    for tabela, caminho in FONTES_CSV.items():
        log.info(f"  ▸ Ingerindo {tabela} ({caminho.name})...")
        try:
            if not caminho.exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")

            # Bronze guarda o dado como chegou: tudo como texto, sem inferência de tipos.
            # A tipagem é responsabilidade da camada Prata.
            df = pd.read_csv(caminho, dtype=str, encoding="utf-8")
            df["_dt_ingestao"] = ts_ingestao
            df["_fonte"] = f"csv/{caminho.name}"

            s3_key = chave_particao(tabela, dt_ingestao)
            linhas = gravar_parquet(s3, df, BUCKET_BRONZE, s3_key)
            log.info(f"    ✓ {linhas:,} linhas → s3://{BUCKET_BRONZE}/{s3_key}")
            metricas.append({"tabela": tabela, "linhas": linhas, "status": "ok"})
        except Exception as exc:
            log.error(f"  ✗ Erro em {tabela}: {exc}")
            metricas.append({"tabela": tabela, "linhas": 0, "status": "erro", "erro": str(exc)})

    return metricas
