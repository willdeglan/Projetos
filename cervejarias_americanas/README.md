# 🍺 Web Scraping - Cervejarias EUA

## 📋 Descrição

Este projeto realiza a raspagem de dados utilizando a API pública [OpenBreweryDB](https://api.openbrewerydb.org)
para coletar informações sobre cervejarias nos Estados Unidos. O objetivo é construir uma estrutura de dados
em camadas (medalhão) dentro do Databricks, passando desde os dados crus até uma versão refinada para análise
e geração de insights.

## 🏗️ Estrutura do Projeto

O pipeline segue o padrão de arquitetura em camadas (bronze, silver e gold):

- **Bronze**: Coleta os dados brutos diretamente da API.
- **Silver**: Limpeza, tratamento e padronização dos dados.
- **Gold**: Agregações, enriquecimentos e preparação para análises.
- **Insights**: Consultas SQL para geração de insights prontos para visualização.

---

## 📂 Notebooks Detalhados

### 1️⃣ `api_1_bronze.ipynb` - Coleta de Dados

- **Objetivo:** Realiza a raspagem de dados diretamente da [API OpenBreweryDB](https://api.openbrewerydb.org).
- **Linguagens/Bibliotecas:** Python, `requests`, `pandas`.
- **Passos:**
  - Realiza uma requisição `GET` para coletar os dados de cervejarias.
  - Converte a resposta JSON em um DataFrame com `pd.json_normalize`.
  - (Etapas de persistência podem ser realizadas fora do notebook, como exportar para CSV ou Delta Lake).

---

### 2️⃣ `api_2_silver.ipynb` - Transformação e Limpeza

- **Objetivo:** Limpa e padroniza os dados extraídos, preparando uma tabela estruturada.
- **Ferramenta:** Spark SQL via notebooks do Databricks.
- **Passos:**
  - Cria o schema `api_cerveja.2_silver` se não existir.
  - Define a estrutura da tabela `tb_cerveja_limpo` com tipos e comentários nas colunas.
  - Insere os dados da camada bronze com `SELECT` e renomeações adequadas.

---

### 3️⃣ `api_3_gold.ipynb` - Modelagem Analítica

- **Objetivo:** Cria a camada final (gold) com dados prontos para análise e dashboards.
- **Ferramenta:** Spark SQL.
- **Passos:**
  - Cria o schema `api_cerveja.3_gold`.
  - Cria a tabela `tb_cerveja_insight` com colunas de identificação e localização.
  - Insere os dados limpos da silver para análise geográfica e por estado/cidade.

---

### 4️⃣ `api_4_insights.ipynb` - Análises e Visualizações

- **Objetivo:** Executa consultas analíticas para gerar insights relevantes.
- **Consultas executadas:**
  - Quantidade de cervejarias por estado (`GROUP BY` e `ORDER BY`).
  - Listagem das cervejarias com informações geográficas (longitude, latitude).
- **Uso:** Pode ser base para visualizações externas ou dashboards (Power BI, Tableau, etc).

---

## ⚙️ Tecnologias Utilizadas

- Python
- Pandas
- Spark SQL
- Databricks Notebooks
- API pública: [https://api.openbrewerydb.org](https://api.openbrewerydb.org)

## 📦 Requisitos

- Ambiente Databricks configurado
- Conexão com a internet para chamadas de API
- Permissões para criação de schemas e tabelas

## ▶️ Execução

Execute os notebooks na ordem listada acima:

1. `api_1_bronze.ipynb` – Raspagem dos dados da API e conversão para DataFrame.
2. `api_2_silver.ipynb` – Criação da tabela limpa com campos bem definidos.
3. `api_3_gold.ipynb` – Seleção de colunas e estruturação para análise.
4. `api_4_insights.ipynb` – Consultas SQL com agrupamentos e dados para visualização.

---

## 🤝 Contribuições

Contribuições são bem-vindas! Fique à vontade para abrir issues ou pull requests com sugestões de melhoria.

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

## 👨‍💻 Autor

### *Willdeglan de S. Santos*
Data Engineer | DBA | Criador do SQL Dicas  
🔗 [LinkedIn: @Willdeglan](https://www.linkedin.com/in/willdeglan)  
📘 [LinkedIn: @sqldicas](https://www.linkedin.com/company/sqldicas)  

_Obs.: esse projeto foi uma produção baseada no [projeto](https://www.linkedin.com/in/talessrocha/details/projects/?profileUrn=urn%3Ali%3Afsd_profile%3AACoAADkzjvgBbQpwiwvZ_Zwl5CaI0zl49iSg_KI) do [Tales Rocha](https://www.linkedin.com/in/talessrocha/)_
