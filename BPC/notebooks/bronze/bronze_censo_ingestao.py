# bronze_censo_ingestao.py
# Variaveis 
catalogo = "bpc"
esquema = "bronze"
tabela = "tb_bronze_censo_2022"
volume = "/Volumes/bpc/raw/source/"

# Importando as bibliotecas
import pandas as pd
import json

# Lê o arquivo inteiro como JSON
with open(f"{volume}censo_2022.csv", "r", encoding="utf-8") as f:
    data = json.load(f)  # vira lista de dicts

# 3. Normalizar: pegar os municípios e populações
linhas = []
for item in data:
    variavel = item["variavel"]
    unidade = item["unidade"]
    for resultado in item["resultados"]:
        for serie in resultado["series"]:
            municipio = serie["localidade"]["nome"]
            ano, valor = list(serie["serie"].items())[0]  # ex: {"2022":"21494"}
            linhas.append([variavel, unidade, municipio, ano, int(valor)])

# 4. Criar dataframe final
df_final = pd.DataFrame(linhas, columns=["variavel", "unidade", "municipio", "ano", "valor"])

# Converter para Spark DataFrame
df_spark = spark.createDataFrame(df_final)

# Salvar no catálogo bpc, schema bronze, tabela tb_bronze_censo_2022
(
df_spark.write
    .format("delta")
    .option("overwriteSchema", "true")
    .mode("overwrite")  # "overwrite" sobrescreve, pode trocar para "append" se for incremental
    .saveAsTable(f"{catalogo}.{esquema}.{tabela}")
)

# exibir a tabela delta salva e contagem de linhas
df_spark_result = spark.read.table(
    f"{catalogo}.{esquema}.{tabela}"
)
display(df_spark_result.limit(5))
total_linhas = df_spark_result.count()
print(f"Total de linhas: {total_linhas}")
