%sql
-- Verifique se o esquema existe, caso contrário, crie-o
CREATE SCHEMA IF NOT EXISTS api_cerveja.3_gold;

DROP TABLE IF EXISTS api_cerveja.3_gold.tb_cerveja_insight;
     

%sql
CREATE TABLE IF NOT EXISTS api_cerveja.3_gold.tb_cerveja_insight (
  id_brewery STRING COMMENT 'Identificador único da Cervejaria',
  name_brewery STRING COMMENT 'Nome da Cervejaria',
  city STRING COMMENT 'Cidade da Cervejaria',
  state_province STRING COMMENT 'Estado da Cervejaria',
  longitude DOUBLE COMMENT 'Longitude da Cervejaria',
  latitude DOUBLE COMMENT 'Latitude da Cervejaria'
)
COMMENT 'Esta tabela é uma entidade corporativa para realizar insights sobre as cervejarias'
;
     

%sql
INSERT INTO api_cerveja.3_gold.tb_cerveja_insight
SELECT 
  id_brewery,
  name_brewery,
  city,
  state_province,
  longitude,
  latitude
FROM api_cerveja.2_silver.tb_cerveja_limpo;
