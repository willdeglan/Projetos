import requests, json, pandas as pd

url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
municipios = requests.get(url).json()

df = pd.json_normalize(municipios)

# Renomear colunas para snake_case sem pontos
df = df.rename(columns={
    "id": "codigo_municipio",
    "nome": "municipio",
    "microrregiao.id": "microrregiao_id",
    "microrregiao.nome": "microrregiao_nome",
    "microrregiao.mesorregiao.id": "mesorregiao_id",
    "microrregiao.mesorregiao.nome": "mesorregiao_nome",
    "microrregiao.mesorregiao.UF.id": "uf_id",
    "microrregiao.mesorregiao.UF.sigla": "uf_sigla",
    "microrregiao.mesorregiao.UF.nome": "uf_nome",
    "microrregiao.mesorregiao.UF.regiao.id": "regiao_id",
    "microrregiao.mesorregiao.UF.regiao.sigla": "regiao_sigla",
    "microrregiao.mesorregiao.UF.regiao.nome": "regiao_nome",
    "regiao-imediata.id": "regiao_imediata_id",
    "regiao-imediata.nome": "regiao_imediata_nome",
    "regiao-imediata.regiao-intermediaria.id": "regiao_intermediaria_id",
    "regiao-imediata.regiao-intermediaria.nome": "regiao_intermediaria_nome",
    "regiao-imediata.regiao-intermediaria.UF.id": "uf_intermediaria_id",
    "regiao-imediata.regiao-intermediaria.UF.sigla": "uf_intermediaria_sigla",
    "regiao-imediata.regiao-intermediaria.UF.nome": "uf_intermediaria_nome",
    "regiao-imediata.regiao-intermediaria.UF.regiao.id": "regiao_intermediaria_regiao_id",
    "regiao-imediata.regiao-intermediaria.UF.regiao.sigla": "regiao_intermediaria_regiao_sigla",
    "regiao-imediata.regiao-intermediaria.UF.regiao.nome": "regiao_intermediaria_regiao_nome"
})

# Salvar o CSV pronto para Spark
df.to_csv("/Volumes/bpc/raw/source/municipios_ibge.csv", index=False, encoding="utf-8")

# Salvando a tabela ("bpc.bronze.tb_bronze_municipios_ibge") a aparir do csv 
df_spark = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/bpc/raw/source/municipios_ibge.csv")
)
spark.sql("DROP TABLE IF EXISTS bpc.bronze.tb_bronze_municipios_ibge;")

df_spark.write.format("delta").mode("overwrite").saveAsTable("bpc.bronze.tb_bronze_municipios_ibge")

# limpando os dados e salvando no esquema silver --- essa parte pode até levar para a pasta silver 
df_silver = (
    df_spark.selectExpr(
        "id as codigo_municipio",
        "nome as municipio",
        "`microrregiao.mesorregiao.UF.id` as codigo_uf",
        "`microrregiao.mesorregiao.UF.sigla` as uf_sigla",
        "`microrregiao.mesorregiao.UF.nome` as uf_nome",
        "`microrregiao.mesorregiao.UF.regiao.nome` as regiao_nome"
    )
)

spark.sql("CREATE SCHEMA IF NOT EXISTS bpc.silver;")

df_silver.write.format("delta").mode("overwrite").saveAsTable("bpc.silver.tb_silver_municipios_ibge")
