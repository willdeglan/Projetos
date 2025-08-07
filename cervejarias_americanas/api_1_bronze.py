# Importando as bibliotecas
import requests
import pandas as pd

# URL da API
url = "https://api.openbrewerydb.org/v1/breweries"

#Requisição GET
response = requests.get(url)

#verifica se a requisição foi bem sucedida
if response.status_code ==200:
    data = response.json()
else:
    print(f"Deu Erro PAEE! Falhou na requisição: {response.status_code}")   

#Converter o json para dataframe
df_pandas = pd.json_normalize(data)

#Converte DataFrame pandas para Spark
df_spark = spark.createDataFrame(df_pandas)

from pyspark.sql.functions import current_date
from pyspark.sql.functions import expr

# incluir colunas de controle
df_spark = df_spark.withColumn("data_carga", current_date())
df_spark = df_spark.withColumn("data_hora_carga", expr("current_timestamp() - INTERVAL 3 HOURS"))

# Verifique se o catálogo existe, caso contrário, crie-o
spark.sql("CREATE CATALOG IF NOT EXISTS api_cerveja")

# Verifique se o esquema existe, caso contrário, crie-o
spark.sql("CREATE SCHEMA IF NOT EXISTS api_cerveja.1_bronze")

# Atribuir as variáveis da estreutura
catalogo = "api_cerveja"
schema = "1_bronze"
tabela = "tb_cerveja"

df_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalogo}.{schema}.{tabela}")

print("Dados salvos com sucesso em " + f"{catalogo}.{schema}.{tabela}")
