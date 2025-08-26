# =========================================================
# GOLD - Indicador de Judicialização do BPC por Município
# =========================================================

from pyspark.sql import functions as F

# =========================================================
# Definições
# =========================================================
catalogo = "bpc"
schema_silver = "silver"
schema_gold = "gold"

tabela_inss = "tb_silver_inss_bpc_2025"
tabela_ibge = "tb_silver_municipios_ibge"
tabela_gold = "tb_gold_judicializacao_bpc"

# Criar schema Gold se não existir
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{schema_gold}")

# =========================================================
# Carregar bases Silver
# =========================================================
df_inss = spark.table(f"{catalogo}.{schema_silver}.{tabela_inss}")
df_ibge = spark.table(f"{catalogo}.{schema_silver}.{tabela_ibge}")

# =========================================================
# Classificação das concessões
# =========================================================
df_inss = df_inss.withColumn(
    "Tipo_Concessao",
    F.when(F.col("Descricao_Despacho").like("%Judicial%"), "Judicial")
     .when(F.col("Descricao_Despacho") == "Concessao em Fase Recursal", "Judicial")
     .when(F.col("Descricao_Despacho").isNull(), "Indefinido")
     .when(F.col("Descricao_Despacho") == "Não Classificado", "Indefinido")
     .otherwise("Administrativo")
)

# =========================================================
# Agregar por município
# =========================================================
df_grouped = df_inss.groupBy("Municipio_Residencia", "UF").agg(
    F.count("*").alias("Total_Concessoes"),
    F.sum(F.when(F.col("Tipo_Concessao") == "Judicial", 1).otherwise(0)).alias("Qtd_Judicial"),
    F.sum(F.when(F.col("Tipo_Concessao") == "Administrativo", 1).otherwise(0)).alias("Qtd_Administrativo"),
    F.sum(F.when(F.col("Tipo_Concessao") == "Indefinido", 1).otherwise(0)).alias("Qtd_Indefinido")
)

# Percentual de judicialização
df_grouped = df_grouped.withColumn(
    "Perc_Judicializacao",
    F.round((F.col("Qtd_Judicial") / F.col("Total_Concessoes")) * 100, 2)
)

# =========================================================
# Criar chave de join (padronizada)
# =========================================================
df_grouped = df_grouped.withColumn(
    "Chave_Join",
    F.concat_ws(
        "_",
        F.col("UF"),
        F.lower(F.regexp_replace(F.col("Municipio_Residencia"), "[^a-zA-Z0-9]", "_"))
    )
)

# =========================================================
# Enriquecer com IBGE (códigos, regiões, nomes oficiais)
# =========================================================
df_gold = (
    df_grouped
    .join(
        df_ibge
        .withColumnRenamed("Nome_Municipio", "Nome_Municipio_IBGE")
        .withColumnRenamed("UF", "UF_IBGE"),
        on="Chave_Join", how="left"
    )
)

# =========================================================
# Selecionar colunas finais
# =========================================================
df_gold = df_gold.select(
    "Codigo_Municipio",
    F.col("Nome_Municipio_IBGE").alias("Nome_Municipio"),
    F.col("UF_IBGE").alias("UF"),
    "Nome_UF",
    "Regiao",
    "Sigla_Regiao",
    "Total_Concessoes",
    "Qtd_Judicial",
    "Qtd_Administrativo",
    "Qtd_Indefinido",
    "Perc_Judicializacao"
)

# =========================================================
# Salvar tabela GOLD
# =========================================================
(
    df_gold.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalogo}.{schema_gold}.{tabela_gold}")
)

print("✅ Gold criada com sucesso:", f"{catalogo}.{schema_gold}.{tabela_gold}")
display(df_gold.orderBy(F.desc("Perc_Judicializacao")).limit(20))
