import os
from pathlib import Path

# Caminhos locais
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"


def _carregar_env(caminho: Path) -> None:
    """Lê um arquivo .env simples (CHAVE=valor) sem sobrescrever variáveis já definidas.

    O mesmo .env é lido pelo docker compose, então portas customizadas valem
    para os containers e para o pipeline ao mesmo tempo.
    """
    if not caminho.exists():
        return
    for linha in caminho.read_text(encoding="utf-8").splitlines():
        linha = linha.strip()
        if not linha or linha.startswith("#") or "=" not in linha:
            continue
        chave, valor = linha.split("=", 1)
        os.environ.setdefault(chave.strip(), valor.strip().strip('"').strip("'"))


_carregar_env(BASE_DIR / ".env")

# RustFS (Object Storage S3)
S3_ENDPOINT = os.environ.get(
    "S3_ENDPOINT", f"http://localhost:{os.environ.get('S3_API_PORT', '9000')}"
)
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "admin123")

BUCKET_BRONZE = "bronze"
BUCKET_PRATA = "prata"
BUCKET_OURO = "ouro"

# PostgreSQL (Camada Ouro / Serving BI)
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5433"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "dw_vendas")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "vendas")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "vendas123")

POSTGRES_URI = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Fontes CSV locais
FONTES_CSV = {
    "vendas": RAW_DIR / "vendas.csv",
    "lojas": RAW_DIR / "lojas.csv",
    "metas_lojas": RAW_DIR / "metas_lojas.csv",
}
