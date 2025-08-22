from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import unicodedata

# =========================================================
# UDF para remover acentos
# =========================================================
def remover_acentos(texto):
    if texto is None:
        return None
    texto_normalizado = unicodedata.normalize('NFD', texto)
    texto_sem_acento = ''.join(
        char for char in texto_normalizado 
        if unicodedata.category(char) != 'Mn'
    )
    return texto_sem_acento

remover_acentos_udf = F.udf(remover_acentos, StringType())

# =========================================================
# Definições
# =========================================================
catalogo = "bpc"
schema_bronze = "bronze"
schema_silver = "silver"
tabela_bronze_ibge = "tb_bronze_municipios_ibge"
tabela_silver_ibge = "tb_silver_municipios_ibge"

# =========================================================
# Carregar Bronze do IBGE
# =========================================================
df_ibge = spark.table(f"{catalogo}.{schema_bronze}.{tabela_bronze_ibge}")

# =========================================================
# Extrair informações USANDO BACKTICKS para escapar nomes com pontos
# =========================================================
df_silver = df_ibge.withColumn(
    "codigo_municipio", 
    F.col("id").cast("int")
).withColumn(
    "nome_municipio", 
    F.initcap(F.trim(F.col("nome")))
).withColumn(
    "uf", 
    F.upper(F.trim(F.col("`microrregiao.mesorregiao.UF.sigla`")))
).withColumn(
    "nome_uf", 
    F.initcap(F.trim(F.col("`microrregiao.mesorregiao.UF.nome`")))
).withColumn(
    "regiao", 
    F.initcap(F.trim(F.col("`microrregiao.mesorregiao.UF.regiao.nome`")))
).withColumn(
    "sigla_regiao", 
    F.upper(F.trim(F.col("`microrregiao.mesorregiao.UF.regiao.sigla`")))
).withColumn(
    "nome_mesorregiao",
    F.initcap(F.trim(F.col("`microrregiao.mesorregiao.nome`")))
).withColumn(
    "nome_microrregiao", 
    F.initcap(F.trim(F.col("`microrregiao.nome`")))
)

# =========================================================
# Criar coluna auxiliar para join - TUDO EM MINÚSCULAS
# =========================================================
df_silver = df_silver.withColumn(
    "chave_join", 
    F.concat_ws(
        "_", 
        F.lower(F.col("uf")),  # UF em minúsculas
        F.lower(  # Tudo em minúsculas
            F.regexp_replace(
                remover_acentos_udf(F.trim(F.col("nome_municipio"))),
                "[^a-zA-Z0-9]", "_"
            )
        )
    )
)

# =========================================================
# Selecionar apenas as colunas relevantes para silver
# =========================================================
colunas_silver = [
    "codigo_municipio",
    "nome_municipio", 
    "uf",
    "nome_uf",
    "regiao",
    "sigla_regiao",
    "nome_mesorregiao",
    "nome_microrregiao",
    "chave_join"
]

df_silver = df_silver.select(*colunas_silver)

# =========================================================
# Deduplicar e filtrar inválidos
# =========================================================
df_silver = df_silver.filter(F.col("codigo_municipio").isNotNull())
df_silver = df_silver.dropDuplicates(["codigo_municipio", "chave_join"])

# =========================================================
# Salvar Silver (sobrescrever a tabela existente)
# =========================================================
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalogo}.{schema_silver}.{tabela_silver_ibge}")

print("✅ Silver criada com sucesso!")
print("📊 Estrutura da tabela silver:")
df_silver.printSchema()
print(f"📍 Tabela: {catalogo}.{schema_silver}.{tabela_silver_ibge}")
print(f"📈 Total de registros: {df_silver.count()}")
display(df_silver.limit(10))
