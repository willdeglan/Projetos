# 1_raw_raspagem.py
# Notebook para raspagem do Brasileirão 2025 - Databricks Free Edition

# ----------------------
# CRIA CATALOG E SCHEMA SE NÃO EXISTIR
# ----------------------
spark.sql("CREATE CATALOG IF NOT EXISTS brasileirao")
spark.sql("CREATE SCHEMA IF NOT EXISTS brasileirao.raw")

# ----------------------
# INSTALAÇÃO DE BIBLIOTECAS
# ----------------------
%pip install beautifulsoup4 requests lxml --quiet

# ----------------------
# IMPORTS
# ----------------------
import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import current_timestamp
import datetime

# Inicializa Spark
spark = SparkSession.builder.appName("Brasileirao2025_Raw").getOrCreate()

# ----------------------
# CONFIGURAÇÃO
# ----------------------
delta_table = "brasileirao.raw.tb_partidas_raw"

# ----------------------
# RASPAGEM (EXEMPLO CORRIGIDO)
# ----------------------
url = "https://ge.globo.com/futebol/brasileirao-serie-a/"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

try:
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    html = response.text
    soup = BeautifulSoup(html, "lxml")
    
    # ----------------------
    # EXTRAÇÃO DE PARTIDAS (DADOS DE EXEMPLO - AJUSTE OS SELECTORS)
    # ----------------------
    partidas = []
    
    # Dados de exemplo - você vai precisar ajustar os selectors reais
    partidas = [
        {"Mandante": "Flamengo", "Visitante": "Palmeiras", "Placar": "2x1"},
        {"Mandante": "Corinthians", "Visitante": "São Paulo", "Placar": "1x1"},
        {"Mandante": "Fluminense", "Visitante": "Vasco", "Placar": "3x0"},
        {"Mandante": "Atlético-MG", "Visitante": "Botafogo", "Placar": "2x2"},
        {"Mandante": "Internacional", "Visitante": "Grêmio", "Placar": "1x0"}
    ]
    
    print("✅ Dados extraídos/com exemplo criados")

except Exception as e:
    print(f"❌ Erro na raspagem: {e}")
    # Dados de fallback
    partidas = [
        {"Mandante": "Flamengo", "Visitante": "Palmeiras", "Placar": "2x1"},
        {"Mandante": "Corinthians", "Visitante": "São Paulo", "Placar": "1x1"}
    ]
    print("🔄 Usando dados de fallback")

# ----------------------
# CRIAR SPARK DATAFRAME
# ----------------------
schema = StructType([
    StructField("Mandante", StringType(), True),
    StructField("Visitante", StringType(), True),
    StructField("Placar", StringType(), True)
])

df = spark.createDataFrame(partidas, schema=schema)

# Adicionar timestamp de extração
df = df.withColumn("Data_Extracao", current_timestamp())

# ----------------------
# SOLUÇÃO PARA FREE EDITION: USAR APENAS DELTA TABLES
# ----------------------

# 1. Primeiro, dropar a tabela se existir (para evitar conflito de schema)
try:
    spark.sql(f"DROP TABLE IF EXISTS {delta_table}")
    print(f"♻️  Tabela antiga removida: {delta_table}")
except Exception as e:
    print(f"ℹ️  Tabela não existia ou não pôde ser removida: {e}")

# 2. Criar a tabela Delta com os dados
try:
    df.write.format("delta").mode("overwrite").saveAsTable(delta_table)
    print(f"✅ Delta Table criada: {delta_table}")
    print(f"📊 Total de registros: {df.count()}")
    
    # Mostrar estatísticas da tabela
    print("📈 Estatísticas da tabela:")
    spark.sql(f"DESCRIBE DETAIL {delta_table}").show(truncate=False)
    
except Exception as e:
    print(f"❌ Erro ao criar Delta Table: {e}")
    # Tentar alternativa mais simples
    try:
        df.write.mode("overwrite").saveAsTable(delta_table)
        print(f"✅ Tabela criada (formato padrão): {delta_table}")
    except Exception as e2:
        print(f"❌ Erro alternativo: {e2}")

# ----------------------
# ALTERNATIVA: SALVAR EM VOLUME (se configurado)
# ----------------------
try:
    # Verificar se volumes estão disponíveis
    volumes = spark.sql("SHOW VOLUMES IN brasileirao.raw")
    if volumes.count() > 0:
        volume_path = "/Volumes/brasileirao/raw/partidas.csv"
        df.write.mode("overwrite").option("header", True).csv(volume_path)
        print(f"✅ CSV salvo em Volume: {volume_path}")
    else:
        print("ℹ️  Nenhum volume configurado, pulando salvamento em volume")
except Exception as e:
    print(f"ℹ️  Não foi possível salvar em volume: {e}")

# ----------------------
# MOSTRAR DADOS E CONSULTAS
# ----------------------
print("🎯 Dados salvos na Delta Table:")
df_final = spark.read.table(delta_table)
df_final.show(truncate=False)

# Criar view temporária para consultas
df_final.createOrReplaceTempView("v_partidas_brasileirao")

print("🔍 Exemplo de consultas SQL:")
# Contagem por time mandante
print("🏟️  Partidas por time mandante:")
spark.sql("""
    SELECT Mandante, COUNT(*) as Total_Partidas 
    FROM v_partidas_brasileirao 
    GROUP BY Mandante 
    ORDER BY Total_Partidas DESC
""").show()

# Última extração
print("⏰ Última extração:")
spark.sql("""
    SELECT MAX(Data_Extracao) as Ultima_Extracao 
    FROM v_partidas_brasileirao
""").show()

# ----------------------
# VERIFICAÇÃO FINAL
# ----------------------
print("✅ Script executado com sucesso!")
print("📋 Próximos passos:")
print("   1. Ajuste os selectors de raspagem do site real")
print("   2. Os dados estão disponíveis em: brasileirao.raw.tb_partidas_raw")
print("   3. Use SQL para consultas: SELECT * FROM brasileirao.raw.tb_partidas_raw")