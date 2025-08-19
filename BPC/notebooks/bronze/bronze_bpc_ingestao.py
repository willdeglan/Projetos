# Databricks notebook source
# DBTITLE 1,em pandas - funcionando
# importando as bibliotecas 
import pandas as pd
import glob
from pyspark.sql import SparkSession

# variaveis de estrutura 
catalogo = "bpc"
esquema_bronze = f"{catalogo}.bronze"
tabela_bronze = f"{esquema_bronze}.tb_bronze_inss_bpc_2025_01a06"
volume = "/Volumes/bpc/raw/source/"

# Caminho dos arquivos
files = glob.glob(f"{volume}inss_2025_*.csv")

# Lista para armazenar os DataFrames
dfs = []

for file in files:
    print(f"Lendo {file}...")
    df_temp = pd.read_csv(
        file,
        sep=";",
        header=1,     # pula o cabeçalho extra
        encoding="utf-8"
    )
    dfs.append(df_temp)

# Concatenar todos os arquivos
df_final = pd.concat(dfs, ignore_index=True)

# Visualizar primeiras linhas
display(df_final.head(5))

# Conferir número de registros e colunas
print("Total de linhas:", len(df_final))
print("Total de colunas:", len(df_final.columns))
print("Colunas:", df_final.columns.tolist())


# COMMAND ----------

# Dicionário de mapeamento para renomear as colunas
mapeamento_colunas = {
    'APS': 'Codigo_APS',
    'APS.1': 'Descricao_APS',
    'Competência concessão': 'Competencia_concessao',
    'Espécie': 'Codigo_Especie',
    'Espécie.1': 'Descricao_Especie',
    'CID': 'Codigo_CID',
    'CID.1': 'Descricao_CID',
    'Despacho': 'Codigo_Despacho',
    'Dt Nascimento': 'Data_Nascimento',
    'Sexo.': 'Sexo',
    'Clientela': 'Clientela',
    'Mun Resid': 'Municipio_Residencia',
    'Vínculo dependentes': 'Vinculo_Dependentes',
    'Forma Filiação': 'Forma_Filiacao',
    'UF': 'UF',
    'Qt SM RMI': 'Quantidade_SM_RMI',
    'Ramo Atividade': 'Ramo_Atividade',
    'Dt DCB': 'Data_DCB',
    'Dt DDB': 'Data_DDB',
    'Dt DIB': 'Data_DIB',
    'País de Acordo Internacional': 'Pais_Acordo_Internacional',
    'Classificador PA': 'Classificador_PA',
    'Despacho.1': 'Descricao_Despacho',
    ' Qt SM RMI ': 'Quantidade_SM_RMI_2',  # Nota: espaço no nome original
    'CNAE 2.0': 'Codigo_CNAE_20',
    'CNAE 2.0.1': 'Descricao_CNAE_20',
    'Grau Instrução': 'Grau_Instrucao'
}

# Renomear as colunas do DataFrame
df_final = df_final.rename(columns=mapeamento_colunas)

# Visualizar as novas colunas
print("Colunas após renomeação:")
print(df_final.columns.tolist())

# Mostrar as primeiras linhas para verificar
display(df_final.head(2))

# Converter colunas de data
colunas_data = ['Data_Nascimento', 'Data_DCB', 'Data_DDB', 'Data_DIB']
for coluna in colunas_data:
    df_final[coluna] = pd.to_datetime(df_final[coluna], errors='coerce', format='%Y-%m-%d %H:%M:%S')

# Converter colunas numéricas
df_final['Quantidade_SM_RMI'] = pd.to_numeric(df_final['Quantidade_SM_RMI'], errors='coerce')
df_final['Quantidade_SM_RMI_2'] = pd.to_numeric(df_final['Quantidade_SM_RMI_2'], errors='coerce')

# Substituir valores problemáticos
df_final.replace({'null': None, '{ñ class}': 'Não Classificado', '00/00/0000': None}, inplace=True)

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

# Criar schema explícito baseado nos tipos reais do pandas
schema = StructType([
    StructField("Codigo_APS", IntegerType(), True),
    StructField("Descricao_APS", StringType(), True),
    StructField("Competencia_concessao", IntegerType(), True),
    StructField("Codigo_Especie", IntegerType(), True),
    StructField("Descricao_Especie", StringType(), True),
    StructField("Codigo_CID", StringType(), True),
    StructField("Descricao_CID", StringType(), True),
    StructField("Codigo_Despacho", StringType(), True),
    StructField("Data_Nascimento", TimestampType(), True),
    StructField("Sexo", StringType(), True),
    StructField("Clientela", StringType(), True),
    StructField("Municipio_Residencia", StringType(), True),
    StructField("Vinculo_Dependentes", StringType(), True),
    StructField("Forma_Filiacao", StringType(), True),
    StructField("UF", StringType(), True),
    StructField("Quantidade_SM_RMI", DoubleType(), True),
    StructField("Ramo_Atividade", StringType(), True),
    StructField("Data_DCB", TimestampType(), True),
    StructField("Data_DDB", TimestampType(), True),
    StructField("Data_DIB", TimestampType(), True),
    StructField("Pais_Acordo_Internacional", StringType(), True),
    StructField("Classificador_PA", StringType(), True),
    StructField("Descricao_Despacho", StringType(), True),
    StructField("Quantidade_SM_RMI_2", DoubleType(), True),
    StructField("Codigo_CNAE_20", DoubleType(), True),
    StructField("Descricao_CNAE_20", StringType(), True),
    StructField("Grau_Instrucao", StringType(), True)
])

# Verificar o que há de errado na coluna Codigo_Despacho
print("Valores únicos em Codigo_Despacho (amostra):")
print(df_final['Codigo_Despacho'].unique()[:20])

print("\nTipos de dados encontrados:")
print(f"Tipo da série: {df_final['Codigo_Despacho'].dtype}")
print(f"Valores nulos: {df_final['Codigo_Despacho'].isnull().sum()}")
print(f"Total de registros: {len(df_final)}")

# Verificar se há valores não-string
print("\nTipos de valores encontrados:")
for valor in df_final['Codigo_Despacho'].dropna().unique()[:10]:
    print(f"Valor: {valor} | Tipo: {type(valor)}")


# Converter TODOS os valores da coluna Codigo_Despacho para string
df_final['Codigo_Despacho'] = df_final['Codigo_Despacho'].astype(str)

# Verificar se agora está tudo como string
print("Tipos de valores após conversão:")
for valor in df_final['Codigo_Despacho'].dropna().unique()[:10]:
    print(f"Valor: {valor} | Tipo: {type(valor)}")

print(f"\nTipo da série agora: {df_final['Codigo_Despacho'].dtype}")


# Converter para Spark DataFrame com schema explícito
df_spark = spark.createDataFrame(df_final, schema=schema)

# Mostrar schema para verificar se está correto
df_spark.printSchema()

# COMMAND ----------

# Salvar como tabela Delta
df_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bpc.bronze.tb_bronze_inss_bpc_2025_01a06")

print("Tabela Delta salva com sucesso!")

# COMMAND ----------

df_spark_result = spark.read.table(
    "bpc.bronze.tb_bronze_inss_bpc_2025_01a06"
)
display(df_spark_result.limit(10))
total_linhas = df_spark_result.count()
print(f"Total de linhas: {total_linhas}")