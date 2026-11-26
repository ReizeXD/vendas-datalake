import logging

import pandas as pd

from config import BUCKET_BRONZE, BUCKET_PRATA
from storage import chave_particao, get_s3, gravar_parquet, ler_parquet, resolver_particao

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


def ler_bronze(s3, tabela: str, dt_ingestao: str) -> pd.DataFrame:
    return ler_parquet(s3, BUCKET_BRONZE, chave_particao(tabela, dt_ingestao))


def gravar_prata(s3, df: pd.DataFrame, tabela: str, dt_ingestao: str) -> int:
    s3_key = chave_particao(tabela, dt_ingestao)
    linhas = gravar_parquet(s3, df, BUCKET_PRATA, s3_key)
    log.info(f"    ✓ {linhas:,} linhas → s3://{BUCKET_PRATA}/{s3_key}")
    return linhas


def classificar_produto(nome: str) -> tuple[str, str]:
    partes = nome.split()
    categoria = MAPA_CATEGORIA.get(partes[0], "Outros") if partes else "Outros"
    estilo = next((e for e in ESTILOS if nome.endswith(e)), "Padrão")
    return categoria, estilo


def executar_prata(dt_ingestao: str | None = None) -> list[dict]:
    log.info("=" * 60)
    log.info(" PRATA — Saneamento, deduplicação e integração")

    s3 = get_s3()
    metricas = []

    try:
        dt_ingestao = resolver_particao(s3, BUCKET_BRONZE, "vendas", dt_ingestao)
        log.info(f" Partição: dt_ingestao={dt_ingestao}")
        log.info("=" * 60)

        # 1. Leitura da Bronze (todas as colunas chegam como texto)
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
        sem_meta = lojas_integradas.loc[lojas_integradas["meta_mensal"].isna(), "id_loja"].tolist()
        if sem_meta:
            log.warning(f"  ! Lojas sem meta cadastrada: {sem_meta}")

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
        vendas["data"] = pd.to_datetime(vendas["data"], format="%Y-%m-%d").dt.date
        vendas["codigo_venda"] = vendas["codigo_venda"].astype(int)
        vendas["quantidade"] = vendas["quantidade"].astype(int)
        vendas["valor_unitario"] = vendas["valor_unitario"].astype(float).round(2)

        # valor_final é recalculado a partir de quantidade × valor_unitario
        valor_final_origem = vendas["valor_final"].astype(float)
        vendas["valor_final"] = (vendas["quantidade"] * vendas["valor_unitario"]).round(2)
        divergentes = int(((valor_final_origem - vendas["valor_final"]).abs() > 0.01).sum())
        log.info(f"  ▸ valor_final divergente de quantidade × valor_unitario: {divergentes}")

        # Regras de qualidade: quantidade e preço devem ser positivos
        invalidas = (vendas["quantidade"] <= 0) | (vendas["valor_unitario"] <= 0)
        if invalidas.any():
            log.warning(f"  ! Linhas descartadas por quantidade/preço inválidos: {int(invalidas.sum())}")
            vendas = vendas.loc[~invalidas]

        antes = len(vendas)
        vendas = vendas.drop_duplicates().reset_index(drop=True)
        removidas = antes - len(vendas)
        log.info(f"  ▸ Duplicatas exatas removidas em vendas: {removidas} ({antes:,} → {len(vendas):,})")

        # Integridade referencial: toda venda precisa de uma loja cadastrada
        lojas_desconhecidas = sorted(set(vendas["id_loja"]) - set(lojas_integradas["id_loja"]))
        if lojas_desconhecidas:
            raise ValueError(f"Vendas com lojas sem cadastro: {lojas_desconhecidas}")

        # 4. Enriquecimento de Produto + Integração com Lojas
        classificacao = {p: classificar_produto(p) for p in vendas["produto"].unique()}
        vendas["categoria"] = vendas["produto"].map(lambda p: classificacao[p][0])
        vendas["estilo"] = vendas["produto"].map(lambda p: classificacao[p][1])
        sem_categoria = sorted(p for p, (cat, _) in classificacao.items() if cat == "Outros")
        if sem_categoria:
            log.warning(f"  ! Produtos sem categoria mapeada (→ 'Outros'): {sem_categoria}")

        vendas_integradas = vendas.merge(lojas_integradas, on="id_loja", how="left")
        linhas_vendas = gravar_prata(s3, vendas_integradas, "vendas_integradas", dt_ingestao)
        metricas.append({"tabela": "vendas_integradas", "linhas": linhas_vendas, "status": "ok"})

    except Exception as exc:
        log.error(f"  ✗ Erro na camada Prata: {exc}")
        metricas.append({"tabela": "prata", "linhas": 0, "status": "erro", "erro": str(exc)})

    return metricas
