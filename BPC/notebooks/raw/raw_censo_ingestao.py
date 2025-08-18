# raw_censo_ingestao.py
import requests
import pandas as pd
import os

# Diretório de destino
source_dir = "/Workspace/bpc/source"
arquivo_destino = f"{source_dir}/censo_2022.csv"

# URL da API SIDRA para população do Censo 2022
url_censo = "https://servicodados.ibge.gov.br/api/v3/agregados/9514/periodos/2022/variaveis/93?localidades=N6[all]"

try:
    print(f"Baixando Censo 2022 via API SIDRA...")
    resp = requests.get(url_censo, timeout=30)
    resp.raise_for_status()
    
    # Salva o arquivo JSON bruto (sem conversão para CSV por agora)
    with open(arquivo_destino, "wb") as f:
        f.write(resp.content)
    
    print(f"✅ Arquivo salvo com sucesso em: {arquivo_destino}")
except Exception as e:
    print(f"❌ Erro ao baixar arquivo: {e}")
    raise e

#========================================
# raw_censo_ingestao.py (ler o arquivo)
#========================================

# Caminho do arquivo no workspace
source_path = "/Workspace/bpc/source/censo_2022.csv"

try:
    print(f"Lendo arquivo Censo 2022 de: {source_path}")
    # Lê o arquivo CSV com pandas
    df = pd.read_csv(source_path)
    print(f"✅ Arquivo lido com sucesso. Primeiras linhas:\n{df.head()}")
except Exception as e:
    print(f"❌ Erro ao ler arquivo: {e}")
    raise e
