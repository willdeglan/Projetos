# Avaliação Técnica 123 – Engenheiro de Dados
### **Nome do Candidato:** Willdeglan de Sousa Santos
### **Data:** 02/09/2025   [**E-mail**](willdeglan@gmail.com)
---

## **Parte 1 – Questões de Múltipla Escolha**

### 1. Sobre a arquitetura em camadas (Bronze, Silver e Gold), qual das opções descreve corretamente suas funções?
- [ ] A. Bronze: dados limpos; Silver: dados crus; Gold: dados agregados\
- [ ] B. Bronze: dados estruturados; Silver: dados não estruturados; Gold: dados arquivados\
- [x] C. Bronze: ingestão de dados crus; Silver: dados refinados e com qualidade; Gold: dados prontos para consumo analítico\
- [ ] D. Bronze: dados replicados; Silver: dados eliminados; Gold: dados transformados em imagens\
- [ ] E. Bronze: arquivos temporários; Silver: dados de machine learning; Gold: dados brutos


### 2. Durante o desenvolvimento de pipelines de dados, qual das práticas abaixo está mais alinhada com princípios de engenharia de dados escalável e sustentável?
- [ ] A. Realizar transformações diretamente na camada de apresentação para reduzir latência
- [ ] B. Evitar versionamento de dados para reduzir espaço em disco
- [ ] C. Centralizar toda a lógica de negócios em dashboards analíticos
- [x] D. Separar responsabilidades em etapas reutilizáveis e com controle de versionamento
- [ ] E. Construir pipelines únicos com lógica acoplada e parametrização mínima

### 3. Em um processo de ingestão de dados de fontes externas, qual prática contribui para maior resiliência e confiabilidade do pipeline?
- [ ] A. Executar a carga manualmente para garantir precisão
- [ ] B. Evitar logs para não gerar arquivos desnecessários
- [x] C. Implementar mecanismos de monitoramento, retry e logging para falhas
- [ ] D. Eliminar checkpoints para reduzir a complexidade do código
- [ ] E. Utilizar múltiplas fontes simultaneamente, sem controle de concorrência

### 4. Qual das opções a seguir representa uma vantagem do uso de formatos de dados orientados a colunas (como Parquet ou ORC) em ambientes analíticos?
- [x] A. Melhor desempenho em leitura seletiva de colunas e compressão eficiente
- [ ] B. Facilidade para edições linha a linha em arquivos
- [ ] C. Compatibilidade com arquivos XML sem necessidade de conversão
- [ ] D. Estruturação ideal para uso com bancos de dados relacionais
- [ ] E. Suporte automático à normalização de dados

### 5. Em relação ao conceito de data lineage, qual das alternativas melhor descreve sua utilidade em um ambiente de dados?
- [ ] A. Minimizar o uso de metadados em ambientes de produção
- [ ] B. Automatizar a modelagem relacional dos dados
- [x] C. Mapear a origem, transformação e destino dos dados para auditoria e governança
- [ ] D. Otimizar diretamente a performance de consultas em camadas analíticas
- [ ] E. Armazenar logs de acesso a dashboards para métricas de uso

 
## Parte 2 – Questão Discursiva
```
Explique, com suas próprias palavras, como você estruturaria um pipeline de dados em arquitetura
do tipo "medalhão", considerando todas as etapas de:
•	Extração de dados (batch e streaming, se aplicável)
•	Transformação e limpeza dos dados
•	Armazenamento em camadas (Bronze, Silver e Gold)
•	Orquestração do processo
•	Disponibilização dos dados para consumo (API, dashboards, relatórios, camadas analíticas)

Utilize exemplos práticos e mencione os principais componentes. A resposta deve ter no máximo uma página.

Em projetos pessoais que desenvolvo, foco em organização como arquitetura medalhão assim o projeto
fica organizado, rastreável e tem uma boa qualidade no fluxo de dados 
```
1.	**Extração de dados:** \
   pode ser por dois (2) caminhos: Batch ou Streaming 
•	Batch: A raspagem ou ingestão de dados são carregados em lotes ou arquivos (CSV, JSON ou Planilhas) para a Silver ou pode armazenar em um storage (raw) e inserir em tabelas do formato original (cru).
•	Streaming: quando necessário, é utilizado conectores para consumir dados em tempo real (por exemplo, Kafka ou Event Hub no Azure).

2.	Transformação e limpeza dados: \
   depois dos arquivos obtidos na extração, é realizar a padronização (tipagem dos dados, normalização ou “desnormalização”, correção de colunas) tratar as inconsistências e imperfeições como duplicatas, nulos ou dados inconsistentes.

3.	Armazenamento em camadas: \
•	BRONZE: datasets crus, como obtidos, podem ser organizados por data de carga, ou até particionado. \
•	SILVER: já começa a ser tratado, agrupado, padronizado e até enriquecido, eliminando dados sujos ou incompletos.\
•	 GOLD: dados pronto para ser consumido, disponibilizado como StarSchema, metricas já calculadas e agregações.

4.	Orquestração: \
   No Databricks Free Edition, por exemplo, de forma automatizada, organizo um código python para cada etapa (1_bronze, 2_silver, 3_gold) e agendo a execução em sequência. Em ambientes corporativos, ferramentas como Airflow, Databricks Jobs e ADF fazem o monitoramento e alertas em caso de falhas.

5.	Disponibilização: \
   para consumo na Gold os dados podem ser disponibilizados de algumas formas: \
•	APIs: para integração com aplicação \
•	DASHBOARD: utilizando BI \
•	CONSULTAS ANALÍTICAS: possibilitando ser consumido em relatórios com SQL
 
## Parte 3 – Desafio Prático – Construção de Bot de Dados
```
1. Desenvolva um bot (robô) em Python que atenda aos critérios abaixo:
•	Utilize a linguagem python (versão 3 ou superior)
•	Obtenha os dados do IPCA em: https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1
•	Coloque os dados no formato tabular (estruturado) e grave um arquivo com este conteúdo no formato “parquet”
•	Construa ao menos 3 funções (ou métodos) e as utilize no código
•	Documente as etapas do processo dentro do próprio código
•	Disponibilize o(s) código(s) e o arquivo final gerado pelo bot (parquet) em um projeto do
    GitHub (repositório público - https://github.com/)

```

### Repositorio no github com o desafio:
https://github.com/willdegl4n/Projetos/tree/main/CNI_SESI_SENAI

Arquivo: Bot_IPCA.py
```python
"""
Bot de Dados - IPCA (IBGE / SIDRA)
Autor: Willdeglan do SQL Dicas
Descrição:
    Este script consome dados do IPCA no formato JSON, organiza em tabela,
    e salva no formato Parquet para consumo analítico.

No databricks não precisa rodar esse comando, mas se for rodar o codigo 
fora do databricks, é necessário rodar o seguinte comando:
 
    %pip install -U requests pyarrow
"""


import pandas as pd
from pathlib import Path # <--- para criar pastas de saída com segurança (output)
import requests  # <--- necessário para acessar a URL
import json # <--- necessário para acessar JSON local

# =========================
# Função 1 - Ler dados JSON
# =========================
"""Carrega o JSON bruto do IPCA (arquivo local ou URL)"""
def carregar_dados_json(origem: str) -> dict:
    if origem.startswith("http"):
        response = requests.get(origem)
        response.raise_for_status() # informa o codigo do erro, se houver
        return response.json()
    else: # caso a origem nao seja uma URL, será lido essa parte
        with open(origem, "r", encoding="utf-8") as f:
            return json.load(f)


# ===================================
# Função 2 - Transformar para tabular
# ===================================
"""Transforma os períodos do JSON em DataFrame pandas, se precisar ele adapta novas colunas automaticamente"""
def transformar_em_tabela(dados_json: dict) -> pd.DataFrame:
    periodos = dados_json["Periodos"]["Periodos"]

    df = pd.DataFrame(periodos)

    # dicionário de renomeação (quando a coluna for conhecida) no padrão snake_case
    rename_map = {
        "Id": "id",
        "Codigo": "codigo",
        "Nome": "mes_referencia",
        "Disponivel": "disponivel",
        "DataLiberacao": "data_liberacao"
    }

    df = df.rename(columns=rename_map)

    return df

# ====================================
# Função 3 - Salvar em formato Parquet
# ====================================
"""Salva os dados tabulares em formato parquet"""
def salvar_parquet(df: pd.DataFrame, caminho_saida: str) -> None:

    Path(caminho_saida).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(caminho_saida, engine="pyarrow", index=False)

# =========================
# Execução principal do bot
# =========================
if __name__ == "__main__":
    # Etapa 1: Carregar dados (arquivo local OU request da API)
    url = "https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1"
    dados = carregar_dados_json(url)

    # Etapa 2: Transformar em tabela
    df_ipca = transformar_em_tabela(dados)

    # Etapa 3: Salvar parquet
    salvar_parquet(df_ipca, "parquet/ipca.parquet")

# =============================
# Exibindo as informações do DF
# =============================
print("✅ Bot rodando com sucesso. Arquivo salvo em 'parquet/ipca.parquet'")
print(df_ipca.head())  # Mostra as primeiras 5 linhas
print(f"Número de linhas: {len(df_ipca)}")  # faz a contagem das linhas

```
```
2. No exemplo do bot que você construiu, o conjunto de dados necessários estava disponível no site diretamente
por meio de um link já definido. No entanto como você resolveria o problema da captura dos dados caso fosse
necessário antes navegar no site (executando passos e cliques por meio de menus, login, botões, links) para
se chegar ao arquivo alvo (se não existisse um link direto para o conjunto de dados).

Como resposta para este item, crie um arquivo (txt), disponibilize-o no referido projeto do GitHub e inclua
no seu conteúdo um texto explicativo (com suas próprias palavras) que descreva uma proposta de solução.
```

Arquivo: resposta_parte2.txt
```text
Se caso a url com o dataset não estivesse disponível em um link direto, seria necessário 
automatizar a navegação até o arquivo alvo.

A solução possiveis é utilizar bibliotecas de automação de navegação, como 
* Selenium (Python) - https://www.youtube.com/watch?app=desktop&v=XLkxOBY965w
* Playwright - https://www.youtube.com/watch?v=yNuTu8csOU0

Com essas bibliotecas, conseguimos simular a interação humana em um navegador:
* login
* clique em menus e botões ou 
* download de arquivos.

O processo seria estruturado em etapas:
1. Abrir o navegador de forma programática (headless).
2. Executar login (se necessário) enviando usuário/senha.
3. Navegar pelos menus e links, simulando cliques.
4. Localizar o link de download ou fazer o download automático.
5. Assim que conseguir o dataset com os dados bruto (CSV/JSON), o pipeline segue normalmente:
   transformação > padronização > armazenamento em parquet > ingestão na arquitetura Bronze

Esse processo vai garantir a automação e reprodutibilidade, tornando desnecessária a intervenção 
manual e tornando o pipeline resiliente.
```

### *Willdeglan de S. Santos*
Data Engineer | DBA | Criador do SQL Dicas  
🔗 LinkedIn: [@willdeglan](https://www.linkedin.com/in/willdeglan) | 📘 [@sqldicas](https://www.linkedin.com/company/sqldicas)  
