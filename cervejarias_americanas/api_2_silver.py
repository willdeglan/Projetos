# Verifique se o esquema existe, caso contrário, crie-o
spark.sql("CREATE SCHEMA IF NOT EXISTS api_cerveja.2_silver")
spark.sql("DROP TABLE IF EXISTS api_cerveja.2_silver.tb_cerveja_limpo")

spark.sql('''
    CREATE TABLE api_cerveja.2_silver.tb_cerveja_limpo (
    id_brewery STRING COMMENT 'Identificador único da Cervejaria',
    name_brewery STRING COMMENT 'Nome da Cervejaria',
    brewery_type STRING COMMENT 'Tipo de Cervejaria',
    address_1 STRING COMMENT 'Endereço da Cervejaria',
    address_2 STRING COMMENT 'Endereço da Cervejaria',
    city STRING COMMENT 'Cidade da Cervejaria',
    state_province STRING COMMENT 'Estado da Cervejaria',
    postal_code STRING COMMENT 'CEP da Cervejaria',
    country STRING COMMENT 'País da Cervejaria',
    longitude DOUBLE COMMENT 'Longitude da Cervejaria',
    latitude DOUBLE COMMENT 'Latitude da Cervejaria',
    phone STRING COMMENT 'Telefone da Cervejaria',
    website_url STRING COMMENT 'Site da Cervejaria',
    state STRING COMMENT 'Estado da Cervejaria',
    street STRING COMMENT 'Rua da Cervejaria'
)
COMMENT 'Esta tabela é uma entidade corporativa que contém cervejarias do EUA'
''')

%sql
INSERT INTO api_cerveja.2_silver.tb_cerveja_limpo
SELECT
    id AS id_brewery,
    name AS name_brewery,
    brewery_type AS brewery_type,
    address_1 AS address_1,
    address_2 AS address_2,
    city AS city,
    state_province AS state_province,
    postal_code AS postal_code,
    country AS country,
    longitude AS longitude,
    latitude AS latitude,
    phone AS phone,
    website_url AS website_url,
    state AS state,
    street AS street
FROM api_cerveja.1_bronze.tb_cerveja;

spark.sql('''
ALTER TABLE api_cerveja.2_silver.tb_cerveja_limpo ADD COLUMNS (
  data_carga DATE,
  data_hora_carga TIMESTAMP
);
''')

%sql
UPDATE api_cerveja.2_silver.tb_cerveja_limpo
SET 
  data_carga = current_date(),
  data_hora_carga = from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo');

%sql
SELECT COUNT(*) FROM api_cerveja.2_silver.tb_cerveja_limpo WHERE address_2 IS NULL;


spark.sql('''
ALTER TABLE api_cerveja.2_silver.tb_cerveja_limpo
SET TBLPROPERTIES (
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5',
  'delta.columnMapping.mode' = 'name'
);
''')

%sql
-- COMO SO TEMOS 1 LINHA COM VALOR NA COLUNA ADDRESS_2 ELA SE TORNA INSIGNIFICANTE PARA MIM, LOGO VAMOS REMOVE-LA
ALTER TABLE api_cerveja.2_silver.tb_cerveja_limpo 
DROP COLUMN address_2;


%sql
-- DUPLICADAS
ALTER TABLE api_cerveja.2_silver.tb_cerveja_limpo 
DROP COLUMN state;


%sql
-- DUPLICADAS
ALTER TABLE api_cerveja.2_silver.tb_cerveja_limpo 
DROP COLUMN street;


%sql
OPTIMIZE api_cerveja.2_silver.tb_cerveja_limpo;


%sql
VACUUM api_cerveja.2_silver.tb_cerveja_limpo 
RETAIN 168 HOURS;





