import os
from pathlib import Path

# Caminhos locais
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"

# RustFS (Object Storage S3)
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
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