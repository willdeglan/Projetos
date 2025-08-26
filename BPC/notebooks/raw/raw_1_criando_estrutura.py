# Antes de iniciar, deve ser criado a estrutura do projeto 
# esse codigo, cria a estrutura no databricks, onde o projeto foi implementado

# deve ser criado o CATALOG (bpc)
# os SCHEMA (raw, bronze, silver e gold)
# e o VOLUME (raw.source)

# CRIANDO O CATALOG
spark.sql("CREATE CATALOG IF NOT EXISTS bpc;")

# CRIANDO O SCHEMA RAW
spark.sql("CREATE SCHEMA IF NOT EXISTS bpc.raw;")

# CRIANDO O VOLUME SOURCE
spark.sql("CREATE VOLUME IF NOT EXISTS bpc.raw.source;")

# CRIANDO O SCHEMA BRONZE
spark.sql("CREATE SCHEMA IF NOT EXISTS bpc.bronze;")

# CRIANDO O SCHEMA SILVER
spark.sql("CREATE SCHEMA IF NOT EXISTS bpc.silver;")

# CRIANDO O SCHEMA GOLD
spark.sql("CREATE SCHEMA IF NOT EXISTS bpc.gold;")
