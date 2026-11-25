# Vendas Data Lake — Arquitetura Medalhão

Projeto de Engenharia de Dados e Business Intelligence que implementa um **Data Lakehouse** seguindo a **Arquitetura Medalhão (Bronze → Prata → Ouro)** para análise de vendas de varejo.

---

## Arquitetura e Tecnologias

* **Object Storage (Data Lake):** [RustFS](http://localhost:9001) (compatível com API Amazon S3) armazenando arquivos **Apache Parquet** (compressão Snappy) nos buckets `bronze`, `prata` e `ouro`.
* **Processamento ETL (Python):** `pandas`, `pyarrow`, `boto3` e `SQLAlchemy`.
* **Camada de Serving / Data Warehouse:** **PostgreSQL 15** (`dw_vendas`) populado com a modelagem dimensional em **Star Schema**.
* **Visualização e BI:** [Apache Superset](http://localhost:8088) conectado diretamente ao Data Warehouse.

### Camadas do Pipeline

1. **Bronze (`pipeline/bronze.py`):** Ingestão bruta dos arquivos CSV (`vendas.csv`, `lojas.csv`, `metas_lojas.csv`) enriquecidos com metadados de auditoria (`_dt_ingestao`, `_fonte`) e particionados em Parquet no bucket `s3://bronze/`.
2. **Prata (`pipeline/prata.py`):** Limpeza, padronização de tipos, remoção de duplicatas exatas, enriquecimento de categorias/estilos de produtos e integração entre vendas, lojas e metas no bucket `s3://prata/`.
3. **Ouro (`pipeline/ouro.py`):** Construção do modelo dimensional (**Star Schema**) com as tabelas `dim_tempo`, `dim_loja`, `dim_produto` e `fato_vendas`, persistidas tanto em Parquet no bucket `s3://ouro/` quanto no PostgreSQL (`dw_vendas`).

---

## Estrutura do Projeto

```text
vendas-datalake/
├── data/
│   └── raw/                          # Fontes originais CSV (vendas, lojas, metas_lojas)
├── pipeline/
│   ├── config.py                     # Configurações e variáveis de ambiente (S3 e Postgres)
│   ├── bronze.py                     # Camada Bronze (Ingestão bruta → Parquet)
│   ├── prata.py                      # Camada Prata (Saneamento, deduplicação e integração)
│   ├── ouro.py                       # Camada Ouro (Modelagem Star Schema → S3 + PostgreSQL)
│   └── run.py                        # Orquestrador CLI do pipeline
├── superset/
│   ├── Dockerfile                    # Imagem do Superset com driver psycopg2-binary
│   └── dashboard_vendas_ouro.zip     # Pacote exportado do Dashboard Executivo pronto
├── docker-compose.yml                # Orquestração dos serviços (RustFS, Postgres, Superset)
└── requirements.txt                  # Dependências Python do pipeline
```

---

## Como Executar

### 1. Subir a infraestrutura (Docker)

```bash
docker compose up -d --build
```

Serviços disponíveis:
* **RustFS Console (S3):** `http://localhost:9001` (usuário: `admin` / senha: `admin123`)
* **PostgreSQL (Camada Ouro):** `localhost:5433` (banco: `dw_vendas` / usuário: `vendas` / senha: `vendas123`)
* **Apache Superset:** `http://localhost:8088` (usuário: `admin` / senha: `admin`)

### 2. Executar o pipeline de dados (Bronze → Prata → Ouro)

Crie e ative o ambiente virtual Python, instale as dependências e execute o orquestrador:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python pipeline/run.py
```

> Também é possível executar uma camada específica ou informar a data da partição:
> ```powershell
> python pipeline/run.py --camada ouro --data 2026-11-23
> ```

### 3. Visualizar o Dashboard no Superset

* Para carregar o dashboard pronto no Superset, basta acessar `http://localhost:8088` (login `admin` / `admin`), ir em **Dashboards → Importar** (ícone de importação no canto superior direito) e subir o arquivo `superset/dashboard_vendas_ouro.zip` para carregar todo o painel pronto (caso o Superset solicite a senha do banco PostgreSQL na importação, informe `vendas123`).
