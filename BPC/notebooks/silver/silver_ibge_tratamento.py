# Databricks notebook source
from pyspark.sql import functions as F

# =========================================================
# Função para remover acentos (Spark nativo com translate)
# =========================================================
def remover_acentos(col_name: str):
    acentos = "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
    sem_acentos = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"
    return F.translate(F.col(col_name), acentos, sem_acentos)

# =========================================================
# Definições
# =========================================================
catalogo = "bpc"
schema_bronze = "bronze"
schema_silver = "silver"
tabela_bronze_ibge = "tb_bronze_municipios_ibge"
tabela_silver_ibge = "tb_silver_municipios_ibge"

# =========================================================
# Carregar Bronze do IBGE
# =========================================================
df_ibge = spark.table(f"{catalogo}.{schema_bronze}.{tabela_bronze_ibge}")

# =========================================================
# Extrair informações
# =========================================================
df_silver = df_ibge.withColumn(
    "Codigo_Municipio", F.col("id").cast("int")
).withColumn(
    "Nome_Municipio", F.initcap(F.trim(F.col("nome")))
).withColumn(
    "UF", F.upper(F.trim(F.col("`microrregiao.mesorregiao.UF.sigla`")))
).withColumn(
    "Nome_UF", F.initcap(F.trim(F.col("`microrregiao.mesorregiao.UF.nome`")))
).withColumn(
    "Regiao", F.initcap(F.trim(F.col("`microrregiao.mesorregiao.UF.regiao.nome`")))
).withColumn(
    "Sigla_Regiao", F.upper(F.trim(F.col("`microrregiao.mesorregiao.UF.regiao.sigla`")))
).withColumn(
    "Nome_Mesorregiao", F.initcap(F.trim(F.col("`microrregiao.mesorregiao.nome`")))
).withColumn(
    "Nome_Microrregiao", F.initcap(F.trim(F.col("`microrregiao.nome`")))
)

# =========================================================
# Criar coluna auxiliar para join (sem acento, minúsculo, underscore)
# =========================================================
df_silver = df_silver.withColumn(
    "Chave_Join", 
    F.concat_ws(
        "_", 
        F.col("UF"), 
        F.lower(
            F.regexp_replace(
                remover_acentos("Nome_Municipio"),
                "[^a-zA-Z0-9]", "_"
            )
        )
    )
)

# =========================================================
# Selecionar apenas as colunas relevantes
# =========================================================
colunas_silver = [
    "Codigo_Municipio",
    "Nome_Municipio", 
    "UF",
    "Nome_UF",
    "Regiao",
    "Sigla_Regiao",
    "Nome_Mesorregiao",
    "Nome_Microrregiao",
    "Chave_Join"
]
df_silver = df_silver.select(*colunas_silver)

# =========================================================
# Deduplicar e filtrar inválidos
# =========================================================
df_silver = df_silver.filter(F.col("Codigo_Municipio").isNotNull())
df_silver = df_silver.dropDuplicates(["Codigo_Municipio", "Chave_Join"])

# =========================================================
# Salvar Silver
# =========================================================
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalogo}.{schema_silver}.{tabela_silver_ibge}")

print("✅ Silver criada com sucesso!")
print("📊 Estrutura da tabela silver:")
df_silver.printSchema()
print(f"📍 Tabela: {catalogo}.{schema_silver}.{tabela_silver_ibge}")
print(f"📈 Total de registros: {df_silver.count()}")

display(df_silver.limit(10))
