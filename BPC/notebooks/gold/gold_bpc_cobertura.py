# =========================================================
# GOLD - Indicador de Cobertura do BPC por Município
# =========================================================

from pyspark.sql import functions as F

# =========================================================
# Definições
# =========================================================
catalogo = "bpc"
schema_silver = "silver"
schema_gold = "gold"

tabela_bpc = "tb_silver_inss_bpc_2025"
tabela_censo = "tb_silver_censo_2022"
tabela_ibge = "tb_silver_municipios_ibge"
tabela_gold = "tb_gold_cobertura_bpc"

# Criar schema Gold se não existir
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{schema_gold}")

# =========================================================
# Carregar tabelas Silver
# =========================================================
df_bpc = spark.table(f"{catalogo}.{schema_silver}.{tabela_bpc}")
df_censo = spark.table(f"{catalogo}.{schema_silver}.{tabela_censo}")
df_ibge = spark.table(f"{catalogo}.{schema_silver}.{tabela_ibge}")

# =========================================================
# Agregar beneficiários por município
# =========================================================
df_bpc_agregado = df_bpc.groupBy("Municipio_Residencia").agg(
    F.count("*").alias("Total_Beneficiarios")
)

# Criar chave de join
df_bpc_agregado = df_bpc_agregado.withColumn(
    "Chave_Join",
    F.lower(
        F.regexp_replace(F.col("Municipio_Residencia"), "[^a-zA-Z0-9]", "_")
    )
)

# =========================================================
# Juntar com Censo e IBGE
# =========================================================
df_censo = (
    df_censo
    .withColumnRenamed("Nome_Municipio", "Nome_Municipio_Censo")
    .withColumnRenamed("UF", "UF_Censo")
)

df_ibge = (
    df_ibge
    .withColumnRenamed("Nome_Municipio", "Nome_Municipio_IBGE")
    .withColumnRenamed("UF", "UF_IBGE")
)

df_gold = (
    df_bpc_agregado
    .join(df_censo, on="Chave_Join", how="left")
    .join(df_ibge, on="Chave_Join", how="left")
)

# =========================================================
# Calcular cobertura
# =========================================================
df_gold = df_gold.withColumn(
    "Percentual_Cobertura_BPC",
    F.when(F.col("Populacao_Total") > 0,
           (F.col("Total_Beneficiarios") / F.col("Populacao_Total")) * 100
    ).otherwise(0.0)
)

# =========================================================
# Selecionar colunas finais (usando IBGE como referência)
# =========================================================
df_gold = df_gold.select(
    "Codigo_Municipio",
    F.col("Nome_Municipio_IBGE").alias("Nome_Municipio"),
    F.col("UF_IBGE").alias("UF"),
    "Total_Beneficiarios",
    "Populacao_Total",
    F.round("Percentual_Cobertura_BPC", 2).alias("Percentual_Cobertura_BPC")
)

# =========================================================
# Salvar GOLD
# =========================================================
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalogo}.{schema_gold}.{tabela_gold}")

print("✅ Gold criada com sucesso:", f"{catalogo}.{schema_gold}.{tabela_gold}")
display(df_gold.orderBy(F.desc("Percentual_Cobertura_BPC")).limit(20))
