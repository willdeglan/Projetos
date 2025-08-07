%sql
USE CATALOG api_cerveja;
USE SCHEMA 3_gold;
     

%sql
SELECT state_province AS nome_estado
      , COUNT(state_province) AS qtd_estado 
FROM 3_gold.tb_cerveja_insight 
GROUP BY state_province 
  ORDER BY COUNT(state_province) DESC;


%sql
SELECT name_brewery
     , longitude
     , latitude
     , state_province 
FROM 3_gold.tb_cerveja_insight;
