from pyspark.sql import functions as F  # Importa funções do PySpark
import re  # Módulo para operações com expressões regulares 

# Databricks notebook source
volume = "/Volumes/bpc/raw/source/"

# Leitura de todos arquivos csv da pasta benef_conced contidos no volume

df = (
    spark.read.format("csv") 
    .option("header", "true") # se tem cabeçalho
    .option("inferSchema", "true") # inferir o schema do arquivo csv
    .option("delimiter", ";") # delimitador do arquivo csv
    .option("encoding", "UTF-8")  # encoding do arquivo csv
    .load(f"dbfs:{volume}")
)

# criando função de limpeza das colunas
def limpar_nome_coluna(nome):
    # 1. Remove espaços no início/fim
    nome = nome.strip()
    
    # 2. Substitui caracteres especiais por underscore
    nome = re.sub(r"[ ,{}()\n\t=]", "_", nome)
    
    # 3. Remove underscores consecutivos
    nome = re.sub(r"__+", "_", nome)
    
    # 4. Remove underscores no início/fim
    nome = nome.strip("_")
    
    # 5. Converte tudo para minúsculas
    return nome.lower()

# Aplica a função a todas as colunas do DataFrame
df = df.select([F.col(c).alias(limpar_nome_coluna(c)) for c in df.columns]) # Renomeia as colunas

# salvar os dados com tabela delta
df.write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema","true")\
  .saveAsTable("bcp.bronze.tb_bronze_bpc_2025_mes01a06")

print("Tabela Delta salva com sucesso!")
