# =========================================================
# GOLD - Dimensão Benefício
# =========================================================

from pyspark.sql import functions as F

# =========================================================
# Definições
# =========================================================
catalogo = "bpc"
schema_silver = "silver"
schema_gold = "gold"

tabela_silver_bpc = "tb_silver_inss_bpc_2025"
tabela_gold = "tb_gold_dim_beneficio"

# Criar schema Gold se não existir
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{schema_gold}")

# =========================================================
# Carregar Silver BPC
# =========================================================
df_bpc = spark.table(f"{catalogo}.{schema_silver}.{tabela_silver_bpc}")

# =========================================================
# Extrair benefícios distintos
# =========================================================
df_beneficio = (
    df_bpc
    .select("Descricao_Especie")
    .distinct()
    .withColumn("Descricao_Especie", F.trim(F.col("Descricao_Especie")))
)

# =========================================================
# Mapear benefícios em categorias (Idoso / Deficiente)
# =========================================================
df_beneficio = df_beneficio.withColumn(
    "Tipo_Beneficio",
    F.when(F.lower(F.col("Descricao_Especie")).like("%idos%"), "BPC - Idoso")
     .when(F.lower(F.col("Descricao_Especie")).like("%deficien%"), "BPC - Pessoa com Deficiência")
     .otherwise("Outro")
)

# Criar ID numérico para a dimensão
df_beneficio = df_beneficio.withColumn("Beneficio_ID", F.monotonically_increasing_id())

# =========================================================
# Selecionar colunas finais
# =========================================================
df_beneficio = df_beneficio.select(
    "Beneficio_ID",
    "Descricao_Especie",
    "Tipo_Beneficio"
)

# =========================================================
# Salvar Gold
# =========================================================
(
    df_beneficio.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalogo}.{schema_gold}.{tabela_gold}")
)

print("✅ Dimensão Benefício criada com sucesso:", f"{catalogo}.{schema_gold}.{tabela_gold}")
display(df_beneficio)
