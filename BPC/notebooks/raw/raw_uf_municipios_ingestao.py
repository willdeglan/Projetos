import os
import pandas as pd

# 1. Define o diretório no workspace
source_dir = "/Workspace/bpc/source"

# 2. Cria o diretório se não existir
try:
    os.makedirs(source_dir, exist_ok=True)
    print(f"📂 Diretório criado/verificado: {source_dir}")
except Exception as e:
    print(f"❌ Erro ao criar diretório: {str(e)}")

# 3. Caminho completo do arquivo
output_path = os.path.join(source_dir, "uf_municipios.csv")

try:
    # 4. Carrega os dados (usando fonte alternativa)
    df = pd.read_csv("https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/main/csv/municipios.csv")
    
    # 5. Processamento mínimo
    df = df.rename(columns={
        'codigo_ibge': 'codigo_municipio',
        'nome': 'municipio'
    })
    
    # 6. Salva como CSV
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"✅ Arquivo salvo em: {output_path}")
    
    # 7. Verificação
    if os.path.exists(output_path):
        print(f"✔ Arquivo existe! Tamanho: {os.path.getsize(output_path)/1024:.2f} KB")
    else:
        print("⚠ Arquivo não foi criado corretamente")

except Exception as e:
    print(f"❌ Erro durante o processo: {str(e)}")
    print("""
    Soluções alternativas:
    1. Verifique as permissões do workspace
    2. Tente baixar manualmente o csv em "https://raw.githubusercontent.com/kelvins/Municipios-Brasileiros/main/csv/municipios.csv"
    """)
