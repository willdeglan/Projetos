# =========================================================
# GOLD - Dimensão Calendário
# =========================================================

from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from datetime import date, timedelta

# =========================================================
# Definições
# =========================================================
catalogo = "bpc"
schema_gold = "gold"
tabela_gold = "tb_gold_dim_calendario"

# Criar schema Gold se não existir
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{schema_gold}")

# =========================================================
# Gerar intervalo de datas
# =========================================================
data_inicio = date(2024, 1, 1)
data_fim = date(2025, 12, 31)

# Lista de datas
datas = [(data_inicio + timedelta(days=i),) for i in range((data_fim - data_inicio).days + 1)]

df_calendario = spark.createDataFrame(datas, ["Data"]).withColumn("Data", F.col("Data").cast(DateType()))

# =========================================================
# Enriquecer com colunas de tempo
# =========================================================
df_calendario = (
    df_calendario
    .withColumn("Ano", F.year("Data"))
    .withColumn("Mes", F.month("Data"))
    .withColumn("Dia", F.dayofmonth("Data"))
    .withColumn("Trimestre", F.quarter("Data"))
    .withColumn("Semestre", F.when(F.col("Mes") <= 6, F.lit(1)).otherwise(F.lit(2)))
    .withColumn("Dia_Semana_Num", F.dayofweek("Data")) # 1=Domingo, 7=Sábado
    .withColumn("Dia_Semana_Nome", F.date_format("Data", "EEEE"))
    .withColumn("Fim_Semana", F.when(F.col("Dia_Semana_Num").isin(1,7), F.lit(1)).otherwise(F.lit(0)))
    .withColumn("Ano_Mes", F.date_format("Data", "yyyy-MM"))
    .withColumn("Ano_Trimestre", F.concat_ws("T", F.col("Ano"), F.col("Trimestre")))
)

# =========================================================
# Salvar Gold
# =========================================================
(
    df_calendario.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalogo}.{schema_gold}.{tabela_gold}")
)

print("✅ Dimensão Calendário criada com sucesso:", f"{catalogo}.{schema_gold}.{tabela_gold}")
display(df_calendario.limit(20))
