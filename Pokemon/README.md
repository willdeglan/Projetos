# PokeAPI, A API dos Pokémon 
<img width="2468" height="712" alt="image" src="https://github.com/user-attachments/assets/3d718dcf-a976-4892-9cbe-8c863357b530" />

---

   É uma fonte de dados online que contém informações sobre todos os Pokémon conhecidos. É como uma enorme \
enciclopédia Pokémon na internet! Você pode acessar essa enciclopédia usando seu computador e até mesmo criar \
programas que pegam essas informações para fazer coisas divertidas.


```python
# importando bibliotecas
import boto3  # para conexão com aws
import json   # para organizar ler e salvar json 
import requests  #para uso de solicitações da API

# função recuperar dados da api
def extract_all_pokemon():
  base_url = "https://pokeapi.co/api/v2/pokemon/"
  pokemon_list = []
  next_url = base_url

  while next_url:
    response = requests.get(next_url)
    if response.status_code == 200:
      data = response.json()
      pokemon_list.extend(data['results'])
      next_url = data.get('next')
    else:
      raise Exception(f"Failed to fetch pokémon data. Satatus code: {response.status_code}")
  
  return pokemon_list

# recebenco os valores da função e gravando em um dataframe
pokemons = spark.createDataFrame(extract_all_pokemon())

# criando a estrutura e salvando o dataframe em uma table
spark.sql("CREATE CATALOG IF NOT EXISTS pokemon")
spark.sql("CREATE SCHEMA IF NOT EXISTS pokemon.bronze")

# salvando no bronze
pokemons.write.mode("overwrite").saveAsTable("pokemon.bronze.api")
```

```sql
%sql
-- Selecionando as informações da tabela pokemon.bronze.API
SELECT * FROM pokemon.bronze.api
```
<img width="1353" height="715" alt="image" src="https://github.com/user-attachments/assets/3d8d14e0-ac5c-4ad2-90e1-399acd5550e1" />
