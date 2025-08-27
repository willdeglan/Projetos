# =========================================================
# GOLD - Fato do BPC por UF (Cobertura, Judicialização e Prazos Médios)
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
tabela_gold = "tb_gold_fato_bpc_uf"

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
# Agregar por Município e Tipo de Concessão
# =========================================================
df_agregado = (
    df_bpc.groupBy("Municipio_Residencia", "Tipo_Concessao")
    .agg(
        F.count("*").alias("Qtd"),
        F.avg("Dias_Ate_Despacho").alias("Prazo_Medio")
    )
)

# Pivotar para Administrativo/Judicial
df_agregado = (
    df_agregado.groupBy("Municipio_Residencia")
    .pivot("Tipo_Concessao", ["Administrativa", "Judicial"])
    .agg(
        F.first("Qtd").alias("Qtd"),
        F.first("Prazo_Medio").alias("Prazo_Medio")
    )
)

# Renomear colunas
df_agregado = df_agregado.selectExpr(
    "Municipio_Residencia",
    "`Administrativa_Qtd` as Beneficios_Administrativos",
    "`Judicial_Qtd` as Beneficios_Judiciais",
    "`Administrativa_Prazo_Medio` as Prazo_Medio_Administrativo",
    "`Judicial_Prazo_Medio` as Prazo_Medio_Judicial"
)

# Criar chave join
df_agregado = df_agregado.withColumn(
    "Chave_Join",
    F.lower(
        F.regexp_replace(
            F.col("Municipio_Residencia"),
            "[^a-zA-Z0-9]",
            "_"
        )
    )
)

# =========================================================
# Juntar com IBGE (para pegar UF)
# =========================================================
df_uf = (
    df_agregado
    .join(
        df_ibge.select(
            "Codigo_Municipio", "Nome_Municipio", "UF", "Chave_Join"
        ),
        on="Chave_Join", how="left"
    )
)

# =========================================================
# Agregar por UF
# =========================================================
df_uf = (
    df_uf.groupBy("UF")
    .agg(
        F.sum("Beneficios_Administrativos").alias("Beneficios_Administrativos"),
        F.sum("Beneficios_Judiciais").alias("Beneficios_Judiciais"),
        F.avg("Prazo_Medio_Administrativo").alias("Prazo_Medio_Administrativo"),
        F.avg("Prazo_Medio_Judicial").alias("Prazo_Medio_Judicial")
    )
)

df_uf = df_uf.withColumn(
    "Total_Beneficios",
    F.col("Beneficios_Administrativos") + F.col("Beneficios_Judiciais")
).withColumn(
    "Percentual_Judicializacao",
    (F.col("Beneficios_Judiciais") / F.col("Total_Beneficios")) * 100
)

# =========================================================
# Juntar com População (Censo)
# =========================================================
df_pop = (
    df_censo.groupBy("UF")
    .agg(F.sum("Populacao_Total").alias("Populacao_Total"))
)

df_uf = df_uf.join(df_pop, on="UF", how="left")

df_uf = df_uf.withColumn(
    "Cobertura_BPC_percent",
    (F.col("Total_Beneficios") / F.col("Populacao_Total")) * 100
)

# =========================================================
# Salvar tabela GOLD
# =========================================================
(
    df_uf.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalogo}.{schema_gold}.{tabela_gold}")
)

print("✅ Gold Fato por UF criada com sucesso:", f"{catalogo}.{schema_gold}.{tabela_gold}")
display(df_uf.orderBy(F.desc("Cobertura_BPC_percent")).limit(20))
