# Databricks notebook source
from pyspark.sql import functions as F

# =========================================================
# Definições
# =========================================================
catalogo = "bpc"
schema_bronze = "bronze"
schema_silver = "silver"

tabela_bronze = "tb_bronze_inss_bpc_2025_01a06"
tabela_silver = "tb_silver_inss_bpc_2025"

# =========================================================
# Carregar Bronze
# =========================================================
df = spark.table(f"{catalogo}.{schema_bronze}.{tabela_bronze}")

# =========================================================
# 1. Coalesce de duplicatas
# =========================================================
if "quantidade_sm_rmi_2" in df.columns:
    df = df.withColumn(
        "quantidade_sm_rmi",
        F.coalesce(F.col("quantidade_sm_rmi"), F.col("quantidade_sm_rmi_2"))
    ).drop("quantidade_sm_rmi_2")

# =========================================================
# 2. Conversões de datas (com fallback de formatos)
# =========================================================
for c in ["data_nascimento", "data_dcb", "data_ddb", "data_dib"]:
    if c in df.columns:
        df = df.withColumn(
            c,
            F.coalesce(
                F.to_date(F.col(c), "yyyy-MM-dd"),
                F.to_date(F.col(c), "yyyy-MM-dd HH:mm:ss"),
                F.to_date(F.col(c), "dd/MM/yyyy")
            )
        )

# Competência (YYYYMM -> 1º dia do mês)
if "competencia_concessao" in df.columns:
    df = df.withColumn(
        "competencia_data",
        F.to_date(
            F.concat_ws(
                "-",
                F.substring("competencia_concessao", 1, 4),
                F.substring("competencia_concessao", 5, 2),
                F.lit("01")
            )
        )
    )

# =========================================================
# 3. Conversões numéricas
# =========================================================
if "quantidade_sm_rmi" in df.columns:
    df = df.withColumn(
        "quantidade_sm_rmi",
        F.regexp_replace("quantidade_sm_rmi", ",", ".").cast("double")
    )

# =========================================================
# 4. Normalização de texto
# =========================================================
if "sexo" in df.columns:
    df = df.withColumn("sexo", F.initcap(F.trim(F.col("sexo"))))

if "uf" in df.columns:
    df = df.withColumn("uf", F.upper(F.trim(F.col("uf"))))

for c in ["clientela", "forma_filiacao", "descricao_especie", "descricao_cid",
          "descricao_despacho", "descricao_cnae_20", "municipio_residencia"]:
    if c in df.columns:
        df = df.withColumn(c, F.initcap(F.trim(F.col(c))))

# =========================================================
# 5. Criar coluna limpa de município
# =========================================================
if "municipio_residencia" in df.columns:
    df = df.withColumn(
        "municipio_residencia_nome",
        F.when(
            F.col("municipio_residencia").rlike("^[0-9]{5}-[A-Z]{2}-.*"),
            F.regexp_extract("municipio_residencia", "^[0-9]{5}-[A-Z]{2}-(.*)$", 1)
        ).otherwise(F.col("municipio_residencia"))
    )

# =========================================================
# 6. Filtros
# =========================================================
if "uf" in df.columns:
    df = df.filter(F.col("uf").isNotNull() & (F.col("uf") != "Não informado"))

if "municipio_residencia_nome" in df.columns:
    df = df.filter(
        F.col("municipio_residencia_nome").isNotNull()
        & (~F.col("municipio_residencia_nome").like("%Zerada%"))
    )

# Deduplicar
df = df.dropDuplicates()

# =========================================================
# 7. Salvar Silver
# =========================================================
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalogo}.{schema_silver}.{tabela_silver}")

print("✅ Silver criada com sucesso:", f"{catalogo}.{schema_silver}.{tabela_silver}")
display(df.limit(20))
