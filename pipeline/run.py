import argparse
import logging
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from bronze import executar_bronze
from prata import executar_prata
from ouro import executar_ouro

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def imprimir_resumo(metricas: list[dict], camada: str, elapsed: float):
    ok = sum(1 for m in metricas if m["status"] == "ok")
    total = sum(m["linhas"] for m in metricas)
    log.info(f"\n{'─' * 52}")
    log.info(f"  {camada.upper()} concluído em {elapsed:.2f}s | Tabelas OK: {ok}/{len(metricas)} | Linhas: {total:,}")
    log.info(f"{'─' * 52}\n")


def executar_pipeline(camada: str = "todas", dt_ingestao: str | None = None):
    if dt_ingestao is None:
        dt_ingestao = date.today().isoformat()

    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║   Pipeline Vendas Data Lake — Arquitetura Medalhão   ║")
    log.info(f"║   Data de ingestão: {dt_ingestao:<33}║")
    log.info("╚══════════════════════════════════════════════════════╝\n")

    inicio = time.time()

    if camada in ("todas", "bronze"):
        t = time.time()
        imprimir_resumo(executar_bronze(dt_ingestao), "bronze", time.time() - t)

    if camada in ("todas", "prata"):
        t = time.time()
        imprimir_resumo(executar_prata(dt_ingestao), "prata", time.time() - t)

    if camada in ("todas", "ouro"):
        t = time.time()
        imprimir_resumo(executar_ouro(dt_ingestao), "ouro", time.time() - t)

    log.info(f"✓ Pipeline finalizado em {time.time() - inicio:.2f}s")
    log.info("  • RustFS Console: http://localhost:9001 (admin / admin123)")
    log.info("  • Superset BI:    http://localhost:8088 (admin / admin)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline Vendas Data Lake")
    parser.add_argument(
        "--camada",
        choices=["todas", "bronze", "prata", "ouro"],
        default="todas",
        help="Camada a executar (padrão: todas)",
    )
    parser.add_argument(
        "--data",
        default=date.today().isoformat(),
        help="Data de ingestão YYYY-MM-DD (padrão: hoje)",
    )
    args = parser.parse_args()
    executar_pipeline(args.camada, args.data)