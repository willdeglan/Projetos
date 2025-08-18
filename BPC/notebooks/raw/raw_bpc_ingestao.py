# salvando_os_dados_bpc_em_raw
import requests
import os

# ============================
# CONFIGURAÇÕES
# ============================

# Pasta onde os arquivos serão salvos (caminho relativo ao projeto)
source_dir = "/Workspace/bpc/source"
os.makedirs(source_dir, exist_ok=True)

# Lista de arquivos para baixar, no formato: (url, "YYYY_MM")
arquivos_inss = [
    ("https://armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/PDA_2023_2025/Grupos_de_dados/Benef%C3%ADcios+concedidos/CONCEDIDOS_DADOS_ABERTOS_JANEIRO+2025.xlsx", "2025_01"),
    ("https://armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/PDA_2023_2025/Grupos_de_dados/Benef%C3%ADcios+concedidos/CONCEDIDOS_DADOS+ABERTOS_FEVEREIRO+2025.xlsx", "2025_02"),
    ("https://armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/PDA_2023_2025/Grupos_de_dados/Benef%C3%ADcios+concedidos/BEN_CONCEDIDOS_032025.xlsx", "2025_03"),
    ("https://armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/PDA_2023_2025/Grupos_de_dados/Benef%C3%ADcios+concedidos/BEN_CONCEDIDOS_042025.xlsx", "2025_04"),
    ("https://armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/PDA_2023_2025/Grupos_de_dados/Benef%C3%ADcios+concedidos/CONCEDIDOS+DADOS+ABERTOS+MAIO+2025.xlsx", "2025_05"),
    ("https://armazenamento-dadosabertos.s3.sa-east-1.amazonaws.com/PDA_2023_2025/Grupos_de_dados/Benef%C3%ADcios+concedidos/CONCEDIDOS+DADOS+ABERTOS+JUNHO+2025.xlsx", "2025_06"),
]

# ============================
# DOWNLOAD DOS ARQUIVOS
# ============================

for url, competencia in arquivos_inss:
    nome_arquivo = f"inss_{competencia}.xlsx"
    caminho_arquivo = os.path.join(source_dir, nome_arquivo)
    
    try:
        print(f"⬇️  Baixando {nome_arquivo}...")
        response = requests.get(url)
        response.raise_for_status()
        
        with open(caminho_arquivo, "wb") as f:
            f.write(response.content)
        
        print(f"✅ {nome_arquivo} salvo em {source_dir}")
    except Exception as e:
        print(f"❌ Erro ao baixar {nome_arquivo}: {e}")

print("\n📦 Download concluído.")
