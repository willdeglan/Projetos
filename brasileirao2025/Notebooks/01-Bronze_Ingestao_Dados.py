# Databricks notebook source
# MAGIC %md
# MAGIC ## 📋 Notebook 01 - Parte 1: Configuração Inicial
# MAGIC 
# MAGIC Vamos testar passo a passo a configuração do ambiente.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Passo 1: Verificar se podemos criar um Catalog**
# MAGIC 
# MAGVamos tentar criar o catalog e ver o que acontece na free edition.

# COMMAND ----------

# Tenta criar o catalog
try:
    spark.sql("CREATE CATALOG IF NOT EXISTS brasileirao2025")
    print("✅ Catalog 'brasileirao2025' criado com sucesso!")
except Exception as e:
    print(f"❌ Erro ao criar catalog: {e}")
    print("⚠️  Provavelmente estamos na free edition - usando hive_metastore")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Passo 2: Listar os catalogs disponíveis**

# COMMAND ----------

# Mostra todos os catalogs disponíveis
try:
    catalogs = spark.sql("SHOW CATALOGS")
    display(catalogs)
except Exception as e:
    print(f"❌ Erro ao listar catalogs: {e}")
    print("Usando hive_metastore como padrão")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Passo 3: Definir nossa estratégia com base no resultado**

# COMMAND ----------

# Vamos ver qual catalog estamos usando
current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
print(f"📊 Catalog atual: {current_catalog}")

# Estratégia: Se não conseguimos criar catalog, usamos hive_metastore
if current_catalog == "hive_metastore":
    schema_name = "brasileirao2025_bronze"
    full_table_path = f"hive_metastore.{schema_name}.jogos_raw"
    print(f"🎯 Usando hive_metastore com schema: {schema_name}")
else:
    schema_name = "bronze"
    full_table_path = f"brasileirao2025.{schema_name}.jogos_raw"
    print(f"🎯 Usando Unity Catalog: {full_table_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Passo 4: Criar o schema/database**

# COMMAND ----------

# Criar o schema onde vamos guardar nossos dados
try:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
    print(f"✅ Schema '{schema_name}' criado com sucesso!")
except Exception as e:
    print(f"❌ Erro ao criar schema: {e}")

# Mostrar todos os schemas/databases
print("\n📋 Schemas disponíveis:")
databases = spark.sql("SHOW DATABASES")
display(databases)