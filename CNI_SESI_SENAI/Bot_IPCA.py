"""
Bot de Dados - IPCA (IBGE / SIDRA)
Autor: Willdeglan S. S.
Descrição:
    Este script consome dados do IPCA no formato JSON, organiza em tabela,
    e salva no formato Parquet para consumo analítico.
"""

import json
import pandas as pd
from pathlib import Path

# =========================
# Função 1 - Ler dados JSON
# =========================
def carregar_dados_json(caminho_arquivo: str) -> dict:
    """Carrega o JSON bruto do IPCA a partir de um arquivo local ou request"""
    with open(caminho_arquivo, "r", encoding="utf-8") as f:
        return json.load(f)

# ===================================
# Função 2 - Transformar para tabular
# ===================================
def transformar_em_tabela(dados_json: dict) -> pd.DataFrame:
    """Transforma os períodos do JSON em DataFrame pandas"""
    periodos = dados_json["Periodos"]["Periodos"]

    df = pd.DataFrame(periodos)
    # renomear colunas para padrão snake_case
    df = df.rename(columns={
        "Id": "id",
        "Codigo": "codigo",
        "Nome": "mes_referencia",
        "Disponivel": "disponivel",
        "DataLiberacao": "data_liberacao"
    })
    return df

# ====================================
# Função 3 - Salvar em formato Parquet
# ====================================
def salvar_parquet(df: pd.DataFrame, caminho_saida: str) -> None:
    """Salva os dados tabulares em formato parquet"""
    Path(caminho_saida).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(caminho_saida, engine="pyarrow", index=False)

# =========================
# Execução principal do bot
# =========================
if __name__ == "__main__":
    # Etapa 1: Carregar dados (simulando request da API)
    dados = carregar_dados_json("https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1")

    # Etapa 2: Transformar em tabela
    df_ipca = transformar_em_tabela(dados)

    # Etapa 3: Salvar parquet
    salvar_parquet(df_ipca, "output/ipca.parquet")

    print("✅ Bot executado com sucesso. Arquivo salvo em 'output/ipca.parquet'")
