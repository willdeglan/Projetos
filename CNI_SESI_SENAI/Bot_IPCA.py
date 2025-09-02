"""
Bot de Dados - IPCA (IBGE / SIDRA)
Autor: Willdeglan S. S.
Descrição:
    Este script consome dados do IPCA no formato JSON, organiza em tabela,
    e salva no formato Parquet para consumo analítico.
"""
%pip install -U requests pyarrow

import json
import pandas as pd
from pathlib import Path # <--- para criar pastas de saída com segurança.
import requests  # <--- necessário para acessar a URL

# =========================
# Função 1 - Ler dados JSON
# =========================
def carregar_dados_json(origem: str) -> dict:
    """Carrega o JSON bruto do IPCA (arquivo local ou URL)"""
    if origem.startswith("http"):
        response = requests.get(origem)
        response.raise_for_status() # informa o codigo do erro, se houver
        return response.json()
    else: # caso a origem nao seja uma URL, será lido essa parte
        with open(origem, "r", encoding="utf-8") as f:
            return json.load(f)


# ===================================
# Função 2 - Transformar para tabular
# ===================================
def transformar_em_tabela(dados_json: dict) -> pd.DataFrame:
    """Transforma os períodos do JSON em DataFrame pandas, adaptando colunas automaticamente"""
    periodos = dados_json["Periodos"]["Periodos"]

    df = pd.DataFrame(periodos)

    # dicionário de renomeação (só para colunas conhecidas)
    rename_map = {
        "Id": "id",
        "Codigo": "codigo",
        "Nome": "mes_referencia",
        "Disponivel": "disponivel",
        "DataLiberacao": "data_liberacao"
    }

    # aplica rename só nas colunas que existem
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

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
    # Etapa 1: Carregar dados (arquivo local OU request da API)
    url = "https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1"
    dados = carregar_dados_json(url)

    # Etapa 2: Transformar em tabela
    df_ipca = transformar_em_tabela(dados)

    # Etapa 3: Salvar parquet
    salvar_parquet(df_ipca, "output/ipca.parquet")

    print("✅ Bot rodando com sucesso. Arquivo salvo em 'output/ipca.parquet'")
    print("output/ipca.parquet")
