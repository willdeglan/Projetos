# =========================================================
# GOLD - Dimensão UF e Região
# =========================================================

from pyspark.sql import functions as F

# =========================================================
# Definições
# =========================================================
catalogo = "bpc"
schema_silver = "silver"
schema_gold = "gold"

tabela_silver_ibge = "tb_silver_municipios_ibge"
tabela_gold = "tb_gold_dim_uf_regiao"

# Criar schema Gold se não existir
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{schema_gold}")

# =========================================================
# Carregar Silver IBGE
# =========================================================
df_ibge = spark.table(f"{catalogo}.{schema_silver}.{tabela_silver_ibge}")

# =========================================================
# Extrair dimensões de UF e Região
# =========================================================
df_uf_regiao = (
    df_ibge
    .select("UF", "Nome_UF", "Regiao", "Sigla_Regiao")
    .distinct()
    .orderBy("UF")
)

# Criar ID numérico para chave primária
df_uf_regiao = df_uf_regiao.withColumn("UF_ID", F.monotonically_increasing_id())

# =========================================================
# Selecionar colunas finais
# =========================================================
df_uf_regiao = df_uf_regiao.select(
    "UF_ID",
    "UF",
    "Nome_UF",
    "Regiao",
    "Sigla_Regiao"
)

# =========================================================
# Salvar Gold
# =========================================================
(
    df_uf_regiao.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalogo}.{schema_gold}.{tabela_gold}")
)

print("✅ Dimensão UF/Região criada com sucesso:", f"{catalogo}.{schema_gold}.{tabela_gold}")
display(df_uf_regiao)
