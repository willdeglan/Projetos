# Importações necessárias
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json

# Criar uma função para obter dados da API (exemplo com API Futebol)
def obter_dados_api(url_api):
    response = requests.get(url_api)
    if response.status_code == 200:
        dados = response.json()
        return dados
    else:
        raise Exception(f"Falha na requisição da API: status {response.status_code}")

# Exemplo de URL da API (ajustar para endpoint válido do Brasileirão 2025)
url_api_partidas = "https://api-futebol.com.br/v1/partidas?campeonato=brasileirao2025"

# Obter os dados da API em json
json_partidas = obter_dados_api(url_api_partidas)

# Criar DataFrame PySpark a partir do JSON carregado
df_partidas = spark.read.json(sc.parallelize([json.dumps(json_partidas)]))

# Mostrar esquema e amostra dos dados
df_partidas.printSchema()
df_partidas.show(5, truncate=False)
