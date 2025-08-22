# ✅ O que esse script faz: 
# Normaliza nomes de colunas → snake_case.
# Converte datas → yyyy-MM-dd.
# Converte valores numéricos (qt_sm_rmi).
# Normaliza strings → Title Case (município, UF, sexo, clientela, forma_filiacao).
# Preenche nulos com "Não informado".
# Limpa CNPJs/APS (mantém só números).
# Salva na tabela bpc.silver.tb_silver_inss_bpc_2025.

# Databricks notebook source
from pyspark.sql import functions as F

# =========================================================
# Definições de Catálogo / Tabelas
# =========================================================
catalogo = "bpc"
schema_bronze = "bronze"
schema_silver = "silver"

tabela_bronze = "tb_bronze_inss_bpc_2025_01a06"
tabela_silver = "tb_silver_inss_bpc_2025"

# =========================================================
# Carregar tabela Bronze
# =========================================================
df = spark.table(f"{catalogo}.{schema_bronze}.{tabela_bronze}")

# =========================================================
# Tratamentos - Camada Silver
# =========================================================

# 1. Padronizar nomes das colunas
df = df.toDF(*[c.strip().lower().replace(" ", "_").replace(".", "_") for c in df.columns])

# 2. Converter datas
for col in ["dt_nascimento", "dt_dcb", "dt_ddb", "dt_dib"]:
    if col in df.columns:
        df = df.withColumn(col, F.to_date(F.col(col), "yyyy-MM-dd"))

# 3. Converter colunas numéricas
for col in ["qt_sm_rmi"]:
    if col in df.columns:
        df = df.withColumn(col, F.regexp_replace(F.col(col), ",", "."))
        df = df.withColumn(col, F.col(col).cast("double"))

# 4. Normalizar strings (title case)
for col in ["sexo_", "clientela", "forma_filiacao", "uf", "mun_resid"]:
    if col in df.columns:
        df = df.withColumn(col, F.initcap(F.col(col)))

# 5. Preencher valores nulos
df = df.fillna({
    "sexo_": "Não informado",
    "clientela": "Não informado",
    "forma_filiacao": "Não informado",
    "uf": "Não informado",
    "mun_resid": "Não informado"
})

# 6. Padronizar CNPJs (manter apenas números por enquanto)
for col in ["aps", "aps_1"]:
    if col in df.columns:
        df = df.withColumn(col, F.regexp_replace(F.col(col), "[^0-9]", ""))

# =========================================================
# Salvar tabela Silver
# =========================================================
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalogo}.{schema_silver}.{tabela_silver}")

# =========================================================
# Verificação
# =========================================================
display(df.limit(20))
