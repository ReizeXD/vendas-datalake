# Vendas Data Lake — Arquitetura Medalhão

Projeto de Engenharia de Dados e Business Intelligence que implementa um **Data Lakehouse** seguindo a **Arquitetura Medalhão (Bronze → Prata → Ouro)** para análise de vendas de varejo.

---

## Arquitetura e Tecnologias

* **Object Storage (Data Lake):** [RustFS](http://localhost:9001) 1.0.6 (compatível com a API do Amazon S3) armazenando arquivos **Apache Parquet** (compressão Snappy) nos buckets `bronze`, `prata` e `ouro`.
* **Processamento ETL (Python ≥ 3.10):** `pandas`, `pyarrow`, `boto3` e `SQLAlchemy`.
* **Camada de Serving / Data Warehouse:** **PostgreSQL 15** (`dw_vendas`) com o modelo dimensional em **Star Schema**, com chaves primárias, estrangeiras e índices.
* **Visualização e BI:** [Apache Superset](http://localhost:8088) 6.1.0 conectado diretamente ao Data Warehouse.

### Fontes de dados

| Arquivo | Conteúdo | Linhas |
| --- | --- | --- |
| `data/raw/vendas.csv` | Itens de venda (código da venda, data, loja, produto, quantidade, preço) — jan a nov/2019 | 93.910 |
| `data/raw/lojas.csv` | Cadastro das lojas (cidade, UF, região) | 25 |
| `data/raw/metas_lojas.csv` | Meta de faturamento **mensal** de cada loja | 25 |

As três fontes são integradas pela chave `id_loja`.

### Camadas do Pipeline

1. **Bronze (`pipeline/bronze.py`):** ingestão bruta dos CSVs, **sem inferência de tipos** (tudo como texto), com metadados de auditoria (`_dt_ingestao`, `_fonte`), particionada por data de ingestão em `s3://bronze/<tabela>/dt_ingestao=AAAA-MM-DD/`.
2. **Prata (`pipeline/prata.py`):** tipagem, padronização de textos, recálculo de `valor_final = quantidade × valor_unitario`, remoção de duplicatas exatas, regras de qualidade, classificação de produtos em categoria/estilo e integração de vendas + lojas + metas em `s3://prata/`.
3. **Ouro (`pipeline/ouro.py`):** construção do **Star Schema** (`dim_tempo`, `dim_loja`, `dim_produto`, `fato_vendas`), carregado no PostgreSQL **em uma única transação** e, depois, persistido em Parquet no bucket `s3://ouro/`.

O orquestrador (`pipeline/run.py`) executa as camadas em sequência e **interrompe na primeira falha**, encerrando com código de saída 1.

#### Regras de qualidade aplicadas na Prata

| Regra | Ação |
| --- | --- |
| Linhas idênticas em todas as colunas (160 no dataset, R$ 46.557) | Removidas — tratadas como erro de carga |
| `valor_final` diferente de `quantidade × valor_unitario` | Recalculado; divergências são registradas no log |
| Quantidade ou preço ≤ 0 | Linha descartada e registrada no log |
| Venda de loja sem cadastro | Pipeline interrompido (erro de integridade) |
| Loja sem meta / produto sem categoria mapeada | Aviso no log (`Outros` como categoria) |

### Modelo dimensional (Camada Ouro)

```text
                 dim_tempo (sk_data PK)
                        │
dim_loja (sk_loja PK) ──┼── fato_vendas (id_fato PK) ── dim_produto (sk_produto PK)
```

* **`fato_vendas`** — grão: uma linha de item de venda. Medidas: `vl_quantidade`, `vl_preco_unit`, `vl_final` (`NUMERIC(12,2)`); `cd_venda` como dimensão degenerada.
* **`dim_tempo`** — calendário diário completo do período, com mês/trimestre/dia da semana em português e indicador de fim de semana.
* **`dim_loja`** — loja, cidade, UF, região e meta mensal.
* **`dim_produto`** — produto, categoria (5) e estilo (6), derivados do nome do produto.

---

## Estrutura do Projeto

```text
vendas-datalake/
├── data/
│   └── raw/                          # Fontes originais CSV (vendas, lojas, metas_lojas)
├── pipeline/
│   ├── config.py                     # Configurações (lê variáveis de ambiente e o .env)
│   ├── storage.py                    # Acesso ao S3/RustFS compartilhado pelas camadas
│   ├── bronze.py                     # Camada Bronze (Ingestão bruta → Parquet)
│   ├── prata.py                      # Camada Prata (Saneamento, qualidade e integração)
│   ├── ouro.py                       # Camada Ouro (Star Schema → PostgreSQL + Parquet)
│   └── run.py                        # Orquestrador CLI do pipeline
├── sql/
│   └── consultas_analiticas.sql      # Consultas analíticas sobre o Star Schema
├── superset/
│   ├── Dockerfile                    # Imagem do Superset com driver psycopg2-binary
│   ├── dashboard_vendas_ouro.zip     # Dashboard Executivo pronto para importar
│   └── dashboard_1_example.zip       # Export antigo em formato "example" (não usar)
├── .env.example                      # Portas configuráveis (docker compose + pipeline)
├── docker-compose.yml                # Orquestração dos serviços (RustFS, Postgres, Superset)
└── requirements.txt                  # Dependências Python do pipeline
```

---

## Como Executar

### 1. Subir a infraestrutura (Docker)

Se alguma das portas padrão (`5433`, `9000`, `9001`, `8088`) já estiver em uso na sua máquina, copie `.env.example` para `.env` e ajuste. O mesmo `.env` é lido pelo docker compose e pelo pipeline.

```bash
cp .env.example .env    # opcional
docker compose up -d --build
```

Serviços disponíveis:
* **RustFS Console (S3):** `http://localhost:9001` (usuário: `admin` / senha: `admin123`)
* **PostgreSQL (Camada Ouro):** `localhost:5433` (banco: `dw_vendas` / usuário: `vendas` / senha: `vendas123`)
* **Apache Superset:** `http://localhost:8088` (usuário: `admin` / senha: `admin`)

### 2. Executar o pipeline de dados (Bronze → Prata → Ouro)

Requer **Python 3.10 ou superior**.

Linux / macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python pipeline/run.py
```

Windows (PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python pipeline/run.py
```

Opções:

```bash
python pipeline/run.py --camada ouro                     # usa a partição mais recente da Prata
python pipeline/run.py --camada prata --data 2026-11-23  # usa uma partição específica
```

Sem `--data`, a execução completa usa a data de hoje; uma camada isolada (`prata` ou `ouro`) usa a partição mais recente da camada anterior.

### 3. Visualizar o Dashboard no Superset

Acesse `http://localhost:8088` (login `admin` / `admin`), vá em **Dashboards → Importar** (ícone no canto superior direito) e envie `superset/dashboard_vendas_ouro.zip`. Se o Superset pedir a senha do banco PostgreSQL, informe `vendas123`.

> Se uma versão anterior do dashboard já foi importada, **apague o dashboard, os gráficos e o dataset `cubo_vendas_ouro`** antes de importar novamente: na importação de dashboards o Superset não sobrescreve gráficos que já existem.

O dashboard contém: KPI de faturamento total, evolução mensal, Top 5 produtos, faturamento por região e categoria e a tabela de desempenho por loja (faturamento médio mensal × meta mensal), com filtros por Região, Trimestre, Categoria e Loja.

### 4. Consultas analíticas

`sql/consultas_analiticas.sql` reúne consultas sobre o Star Schema: indicadores gerais e ticket médio, faturamento mensal com variação, Top 5 produtos, participação por região/categoria, atingimento de meta por loja e mês, ranking de lojas e sazonalidade por dia da semana.

```bash
docker exec -i postgres-gold psql -U vendas -d dw_vendas < sql/consultas_analiticas.sql
```
