# Databricks notebook source
from pyspark.sql import functions as F

# =========================================================
# Helpers
# =========================================================
def remover_acentos(col_name: str):
    acentos = "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
    sem     = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"
    return F.translate(F.col(col_name), acentos, sem)

# =========================================================
# Definições
# =========================================================
catalogo = "bpc"
schema_bronze = "bronze"
schema_silver = "silver"

tabela_bronze_censo = "tb_bronze_censo_2022"
tabela_silver_censo = "tb_silver_censo_2022"

spark.sql(f"USE CATALOG {catalogo}")

# =========================================================
# Carregar Bronze do Censo
# =========================================================
dfb = spark.table(f"{catalogo}.{schema_bronze}.{tabela_bronze_censo}")

print("📊 Colunas no Bronze:", dfb.columns)
display(dfb.limit(5))

# =========================================================
# Filtrar apenas população residente
# =========================================================
df = dfb.filter(F.lower(F.col("variavel")).like("%população residente%"))

# =========================================================
# Separar Municipio e UF
# =========================================================
df = df.withColumn("Nome_Municipio", F.initcap(F.trim(F.split(F.col("municipio"), "-").getItem(0))))
df = df.withColumn("UF", F.upper(F.trim(F.split(F.col("municipio"), "-").getItem(1))))

# População total
df = df.withColumn("Populacao_Total", F.col("valor").cast("double"))

# Criar chave join compatível com IBGE
df = df.withColumn(
    "Chave_Join",
    F.concat_ws(
        "_",
        F.col("UF"),
        F.lower(
            F.regexp_replace(
                remover_acentos("Nome_Municipio"),
                "[^a-zA-Z0-9]",
                "_"
            )
        )
    )
)

# =========================================================
# Selecionar colunas finais
# =========================================================
df = df.select(
    "Nome_Municipio",
    "UF",
    "Populacao_Total",
    "Chave_Join"
)

# Deduplicar
df = df.dropDuplicates(["Nome_Municipio", "UF"])

# =========================================================
# Salvar Silver
# =========================================================
(
    df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalogo}.{schema_silver}.{tabela_silver_censo}")
)

print("✅ Silver do Censo criada com sucesso!")
print("📍 Tabela:", f"{catalogo}.{schema_silver}.{tabela_silver_censo}")
print("📈 Registros:", df.count())
df.printSchema()
display(df.limit(20))
