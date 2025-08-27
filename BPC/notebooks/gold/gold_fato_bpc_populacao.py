# =========================================================
# GOLD - Fato População Alvo do BPC por UF
# =========================================================

from pyspark.sql import functions as F

# =========================================================
# Definições
# =========================================================
catalogo = "bpc"
schema_silver = "silver"
schema_gold = "gold"

tabela_censo = "tb_silver_censo_2022"
tabela_gold = "tb_gold_fato_bpc_populacao"

# Criar schema Gold se não existir
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{schema_gold}")

# =========================================================
# Carregar Censo
# =========================================================
df_censo = spark.table(f"{catalogo}.{schema_silver}.{tabela_censo}")

# =========================================================
# Criar variáveis de público-alvo (Idoso >= 65 anos e total de população)
# =========================================================
# Para simplificar, vamos assumir:
# - "População Idosa" = 15% da população total (aprox do Censo 2022)
# - "População PCD" = 8% da população total (dados IBGE estimados)
# Obs: esses percentuais podem ser substituídos por colunas reais do Censo detalhado quando disponíveis.

df_pop = (
    df_censo
    .groupBy("UF")
    .agg(F.sum("Populacao_Total").alias("Populacao_Total"))
)

df_pop = (
    df_pop
    .withColumn("Populacao_Idosa", (F.col("Populacao_Total") * 0.15).cast("bigint"))
    .withColumn("Populacao_PCD", (F.col("Populacao_Total") * 0.08).cast("bigint"))
)

# =========================================================
# Adicionar Total Público-Alvo (Idosos + PCD)
# =========================================================
df_pop = df_pop.withColumn(
    "Populacao_Alvo_BPC",
    F.col("Populacao_Idosa") + F.col("Populacao_PCD")
)

# =========================================================
# Salvar tabela GOLD
# =========================================================
(
    df_pop.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalogo}.{schema_gold}.{tabela_gold}")
)

print("✅ Gold Fato População criada com sucesso:", f"{catalogo}.{schema_gold}.{tabela_gold}")
display(df_pop.orderBy(F.desc("Populacao_Total")).limit(20))
