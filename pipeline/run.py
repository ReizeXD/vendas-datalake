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

CAMADAS = {
    "bronze": executar_bronze,
    "prata": executar_prata,
    "ouro": executar_ouro,
}


def imprimir_resumo(metricas: list[dict], camada: str, elapsed: float) -> bool:
    ok = sum(1 for m in metricas if m["status"] == "ok")
    total = sum(m["linhas"] for m in metricas)
    log.info(f"\n{'─' * 52}")
    log.info(f"  {camada.upper()} concluído em {elapsed:.2f}s | Tabelas OK: {ok}/{len(metricas)} | Linhas: {total:,}")
    log.info(f"{'─' * 52}\n")
    return all(m["status"] == "ok" for m in metricas)


def executar_pipeline(camada: str = "todas", dt_ingestao: str | None = None) -> bool:
    """Executa as camadas pedidas e interrompe na primeira que falhar.

    Sem --data: a execução completa usa a data de hoje; uma camada isolada
    (prata/ouro) usa a partição mais recente da camada anterior.
    """
    if dt_ingestao is None and camada in ("todas", "bronze"):
        dt_ingestao = date.today().isoformat()

    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║   Pipeline Vendas Data Lake — Arquitetura Medalhão   ║")
    log.info(f"║   Data de ingestão: {dt_ingestao or 'partição mais recente':<33}║")
    log.info("╚══════════════════════════════════════════════════════╝\n")

    inicio = time.time()
    etapas = list(CAMADAS) if camada == "todas" else [camada]

    for nome in etapas:
        t = time.time()
        metricas = CAMADAS[nome](dt_ingestao)
        if not imprimir_resumo(metricas, nome, time.time() - t):
            log.error(f"✗ Pipeline interrompido: falha na camada {nome.upper()}.")
            return False

    log.info(f"✓ Pipeline finalizado em {time.time() - inicio:.2f}s")
    log.info("  • RustFS Console: http://localhost:9001 (admin / admin123)")
    log.info("  • Superset BI:    http://localhost:8088 (admin / admin)")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline Vendas Data Lake")
    parser.add_argument(
        "--camada",
        choices=["todas", *CAMADAS],
        default="todas",
        help="Camada a executar (padrão: todas)",
    )
    parser.add_argument(
        "--data",
        default=None,
        help="Data de ingestão YYYY-MM-DD (padrão: hoje na execução completa; "
             "partição mais recente ao rodar prata/ouro isoladamente)",
    )
    args = parser.parse_args()
    sys.exit(0 if executar_pipeline(args.camada, args.data) else 1)
