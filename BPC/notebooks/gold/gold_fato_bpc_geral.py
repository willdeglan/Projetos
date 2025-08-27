# =========================================================
# GOLD - Fato Geral do BPC (Cobertura, Judicialização e Prazos Médios)
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
tabela_gold = "tb_gold_fato_bpc_geral"

# Criar schema Gold se não existir
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{schema_gold}")

# =========================================================
# Carregar tabelas Silver
# =========================================================
df_bpc = spark.table(f"{catalogo}.{schema_silver}.{tabela_bpc}")
df_censo = spark.table(f"{catalogo}.{schema_silver}.{tabela_censo}")
df_ibge = spark.table(f"{catalogo}.{schema_silver}.{tabela_ibge}")

# =========================================================
# Classificar concessão administrativa vs judicial
# =========================================================
df_bpc = df_bpc.withColumn(
    "Tipo_Concessao",
    F.when(F.lower(F.col("Descricao_Despacho")).like("%judicial%"), "Judicial")
     .otherwise("Administrativa")
)

# =========================================================
# Calcular prazo em dias até o despacho
# =========================================================
df_bpc = df_bpc.withColumn(
    "Dias_Ate_Despacho",
    F.datediff(F.col("Data_DDB"), F.col("Data_DIB"))
)

# =========================================================
# Agregar geral (sem UF/município ainda)
# =========================================================
df_agregado = (
    df_bpc.groupBy("Tipo_Concessao")
    .agg(
        F.count("*").alias("Qtd"),
        F.avg("Dias_Ate_Despacho").alias("Prazo_Medio")
    )
)

# Pivotar para trazer Administrativo/Judicial lado a lado
df_agregado = (
    df_agregado.groupBy()
    .pivot("Tipo_Concessao", ["Administrativa", "Judicial"])
    .agg(F.first("Qtd").alias("Qtd"), F.first("Prazo_Medio").alias("Prazo_Medio"))
)

# Renomear colunas
df_agregado = df_agregado.selectExpr(
    "`Administrativa_Qtd` as Beneficios_Administrativos",
    "`Judicial_Qtd` as Beneficios_Judiciais",
    "`Administrativa_Prazo_Medio` as Prazo_Medio_Administrativo",
    "`Judicial_Prazo_Medio` as Prazo_Medio_Judicial"
)

# Total e percentual judicialização
df_agregado = df_agregado.withColumn(
    "Total_Beneficios",
    F.col("Beneficios_Administrativos") + F.col("Beneficios_Judiciais")
).withColumn(
    "Percentual_Judicializacao",
    (F.col("Beneficios_Judiciais") / F.col("Total_Beneficios")) * 100
)

# =========================================================
# Cobertura: integrar com População total (Censo)
# =========================================================
pop_total = df_censo.agg(F.sum("Populacao_Total")).collect()[0][0]

df_agregado = df_agregado.withColumn("Populacao_Total", F.lit(pop_total))

df_agregado = df_agregado.withColumn(
    "Cobertura_BPC_percent",
    (F.col("Total_Beneficios") / F.col("Populacao_Total")) * 100
)

# =========================================================
# Salvar tabela GOLD
# =========================================================
(
    df_agregado.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalogo}.{schema_gold}.{tabela_gold}")
)

print("✅ Gold Fato Geral criada com sucesso:", f"{catalogo}.{schema_gold}.{tabela_gold}")
display(df_agregado)
