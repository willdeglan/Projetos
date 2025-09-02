"""
Bot de Dados - IPCA (IBGE / SIDRA)
Autor: Willdeglan do SQL Dicas
Descrição:
    Este script consome dados do IPCA no formato JSON, organiza em tabela,
    e salva no formato Parquet para consumo analítico.

No databricks não precisa rodar esse comando, mas se for rodar o codigo 
fora do databricks, é necessário rodar o seguinte comando:
 
    %pip install -U requests pyarrow
"""

import pandas as pd
from pathlib import Path # <--- para criar pastas de saída com segurança (output)
import requests  # <--- necessário para acessar a URL
import json # <--- necessário para acessar JSON local

# =========================
# Função 1 - Ler dados JSON
# =========================
"""Carrega o JSON bruto do IPCA (arquivo local ou URL)"""
def carregar_dados_json(origem: str) -> dict:
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
"""Transforma os períodos do JSON em DataFrame pandas, se precisar ele adapta novas colunas automaticamente"""
def transformar_em_tabela(dados_json: dict) -> pd.DataFrame:
    periodos = dados_json["Periodos"]["Periodos"]

    df = pd.DataFrame(periodos)

    # dicionário de renomeação (quando a coluna for conhecida) no padrão snake_case
    rename_map = {
        "Id": "id",
        "Codigo": "codigo",
        "Nome": "mes_referencia",
        "Disponivel": "disponivel",
        "DataLiberacao": "data_liberacao"
    }

    df = df.rename(columns=rename_map)

    return df

# ====================================
# Função 3 - Salvar em formato Parquet
# ====================================
"""Salva os dados tabulares em formato parquet"""
def salvar_parquet(df: pd.DataFrame, caminho_saida: str) -> None:

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
    salvar_parquet(df_ipca, "parquet/ipca.parquet")

# =============================
# Exibindo as informações do DF
# =============================
print("✅ Bot rodando com sucesso. Arquivo salvo em 'output/ipca.parquet'")
print(df_ipca.head())  # Mostra as primeiras 5 linhas
print(f"Número de linhas: {len(df_ipca)}")  # faz a contagem das linhas
