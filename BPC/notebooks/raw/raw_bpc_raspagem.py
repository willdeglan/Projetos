%pip install openpyxl

# raw_bpc_ingestao.py
import requests
import os
import pandas as pd
from io import BytesIO

# ============================
# CONFIGURAÇÕES
# ============================

# Pasta onde os arquivos serão salvos (caminho relativo ao projeto)
source_dir = "/Volumes/bpc/raw/source/"
os.makedirs(source_dir, exist_ok=True)

# Lista de arquivos para baixar
# Formato: (url, "YYYY_MM")
arquivos_inss = [
    ("https://armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/PDA_2023_2025/Grupos_de_dados/Benef%C3%ADcios+concedidos/CONCEDIDOS_DADOS_ABERTOS_JANEIRO+2025.xlsx", "2025_01"),
    ("https://armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/PDA_2023_2025/Grupos_de_dados/Benef%C3%ADcios+concedidos/CONCEDIDOS_DADOS+ABERTOS_FEVEREIRO+2025.xlsx", "2025_02"),
    ("https://armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/PDA_2023_2025/Grupos_de_dados/Benef%C3%ADcios+concedidos/BEN_CONCEDIDOS_032025.xlsx", "2025_03"),
    ("https://armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/PDA_2023_2025/Grupos_de_dados/Benef%C3%ADcios+concedidos/BEN_CONCEDIDOS_042025.xlsx", "2025_04"),
    ("https://armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/PDA_2023_2025/Grupos_de_dados/Benef%C3%ADcios+concedidos/CONCEDIDOS+DADOS+ABERTOS+MAIO+2025.xlsx", "2025_05"),
    ("https://armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/PDA_2023_2025/Grupos_de_dados/Benef%C3%ADcios+concedidos/CONCEDIDOS+DADOS+ABERTOS+JUNHO+2025.xlsx", "2025_06"),
]

# ============================
# DOWNLOAD + CONVERSÃO
# ============================

for url, competencia in arquivos_inss:
    nome_csv = f"inss_{competencia}.csv"
    caminho_csv = os.path.join(source_dir, nome_csv)
    
    try:
        print(f"⬇️  Baixando {nome_csv}...")
        response = requests.get(url)
        response.raise_for_status()
        
        # Lê Excel direto da memória
        df = pd.read_excel(BytesIO(response.content))
        
        # Salva em CSV (padrão UTF-8 e ; como separador, igual INSS usa nos CSVs)
        df.to_csv(caminho_csv, index=False, sep=";", encoding="utf-8")
        #df.to_json(caminho_json, orient="records", force_ascii=False)

        print(f"✅ {nome_csv} salvo em {source_dir}")
    except Exception as e:
        print(f"❌ Erro ao processar {nome_csv}: {e}")

print("\n📦 Download concluído.")
