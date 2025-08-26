# BPC - Análise de Judicialização, Cobertura e Prazos
<img width="870" height="420" alt="image" src="bpc.jpg" />

O **Benefício de Prestação Continuada (BPC)** é voltado para pessoas
idosas ou com deficiência em situação de vulnerabilidade.\
Diferente dos benefícios previdenciários, não exige contribuição prévia,
o que o torna um tema de grande relevância social, política e
orçamentária.

A concessão pode ocorrer de duas formas: 
- **Administrativa** → diretamente pelo INSS.
- **Judicial** → quando o pedido é negado e o requerente recorre à Justiça.

Compreender **quais regiões apresentam índices elevados de
judicialização e como se comportam os indicadores de cobertura e
prazos** permite decisões estratégicas, tanto na gestão pública quanto
na advocacia previdenciária.

------------------------------------------------------------------------

# 🎯 Objetivo do Projeto

-   Monitorar concessões do BPC iniciadas em 2025 (jan-jun).\
-   Avaliar cobertura territorial, prazos e judicializações por tipo de
    benefício.\
-   Apoiar decisões estratégicas em advocacia previdenciária e gestão
    pública.

------------------------------------------------------------------------

# 🧩 Arquitetura Medalhão

O projeto segue a **Arquitetura Medalhão (Medallion Architecture)** no Databricks, dividida em camadas:

```plaintext
A- Raw\CSVs Originais
B- Bronze\Delta Tables Brutas
C- Silver\Dados Tratados e Padronizados
D- Gold\Indicadores Analíticos

    A --> B
    B --> C
    C --> D

```

------------------------------------------------------------------------

# 📂 Estrutura dos Notebooks
``` plaintext
📂 bpc
│
├── 📂 raw
│   ├── 📄 raw_1_criando_estrutura.py
│   ├── 📄 raw_bpc_raspagem.py
│   ├── 📄 raw_censo_raspagem.py
│   └── 📄 raw_uf_municipios_raspagem.py
│
├── 📂 bronze
│   ├── 📄 bronze_bpc_ingestao.py
│   ├── 📄 bronze_censo_ingestao.py
│   └── 📄 bronze_municipios_ibge_ingestao.py
│
├── 📂 silver
│   ├── 📄 silver_bpc_tratamento.py
│   ├── 📄 silver_ibge_tratamento.py
│   └── 📄 silver_censo_tratamento.py
│
└── 📂 gold
    ├── 📄 gold_bpc_cobertura.py
    └── 📄 gold_bpc_judicializacao.py
```
## 🔹 Raw - Ingestão de Dados

Responsável por **trazer os CSVs originais** para dentro do ambiente.\
Aqui não há transformação, apenas ingestão.

-   **`raw_bpc_ingestao.py`** → Ingestão dos arquivos de concessões do
    INSS (jan-jun/25).\
-   **`raw_censo_ingestao.py`** → Ingestão do Censo 2022 (população
    residente).\
-   **`raw_uf_municipios_ingestao.py`** → Ingestão de
    municípios/UF/regiões do IBGE.\
-   **`raw_criando_estrutura.py`** → Script auxiliar para estruturar
    diretórios e tabelas.

------------------------------------------------------------------------

## 🔹 Bronze - Dados Brutos Padronizados

Nesta camada, os dados são convertidos em **Delta Tables**.\
Não há grandes transformações, apenas padronização inicial (snake_case).

-   **`bronze_bpc_ingestao.py`** → Cria tabela
    `tb_bronze_inss_bpc_2025_01a06`.\
-   **`bronze_censo_ingestao.py`** → Cria tabela
    `tb_bronze_censo_2022`.\
-   **`bronze_municipios_ibge_ingestao.py`** → Cria tabela
    `tb_bronze_municipios_ibge`.

------------------------------------------------------------------------

## 🔹 Silver - Tratamento e Enriquecimento

Aqui os dados são **limpos, padronizados e enriquecidos** para permitir
integrações corretas.

-   **`silver_bpc_tratamento.py`**\
    Entrada: `tb_bronze_inss_bpc_2025_01a06`.\
    Saída: `tb_silver_inss_bpc_2025`.\
    Explicação: Limpeza de colunas, padronização de tipos e normalização
    de descrições.

-   **`silver_censo_tratamento.py`**\
    Entrada: `tb_bronze_censo_2022`.\
    Saída: `tb_silver_censo_2022`.\
    Explicação: Normaliza nomes de municípios e prepara chave de
    integração.

-   **`silver_ibge_tratamento.py`**\
    Entrada: `tb_bronze_municipios_ibge`.\
    Saída: `tb_silver_municipios_ibge`.\
    Explicação: Padroniza informações de municípios, estados e regiões.

------------------------------------------------------------------------

## 🔹 Gold - Indicadores Analíticos

Camada final com **tabelas analíticas prontas** para dashboards
(Databricks SQL / Power BI).

-   **`gold_bpc_cobertura.py`**\
    Entrada: `tb_silver_inss_bpc_2025` + `tb_silver_censo_2022` +
    `tb_silver_municipios_ibge`.\
    Saída: `tb_gold_cobertura_bpc`.\
    Explicação: Calcula a **cobertura do BPC** (% da população
    atendida).

-   **`gold_bpc_judicializacao.py`**\
    Entrada: `tb_silver_inss_bpc_2025` + `tb_silver_municipios_ibge`.\
    Saída: `tb_gold_judicializacao_bpc`.\
    Explicação: Calcula a **judicialização do BPC** (% concessões
    judiciais vs administrativas).



------------------------------------------------------------------------

# 🗄️ Estrutura no Catálogo (Unity Catalog / Hive Metastore)

    🏦 bpc (Catalog)
    │
    ├── 🛢️ raw (Schema)
    │   └── 📂 source (Volume)
    │       ├── 📄 censo_2022.csv
    │       ├── 📄 inss_2025_01.csv 
    │       ├── 📄 inss_2025_02.csv
    │       ├── 📄 inss_2025_03.csv
    │       ├── 📄 inss_2025_04.csv
    │       ├── 📄 inss_2025_05.csv
    │       ├── 📄 inss_2025_06.csv
    │       ├── 📄 municipios_ibge.csv
    │       └── 📄 uf_municipios.csv
    │
    ├── 🛢️ bronze (Schema)
    │   ├── 🗂️ tb_bronze_censo_2022
    │   ├── 🗂️ tb_bronze_inss_bpc_2025_01a06
    │   └── 🗂️ tb_bronze_municipios_ibge
    │
    ├── 🛢️ silver (Schema)
    │   ├── 🗂️ tb_silver_censo_2022
    │   ├── 🗂️ tb_silver_inss_bpc_2025
    │   └── 🗂️ tb_silver_municipios_ibge
    │
    └── 🛢️ gold (Schema)
        ├── 🗂️ tb_gold_cobertura_bpc
        └── 🗂️ tb_gold_judicializacao_bpc

Legenda:\
📂 diretório de notebooks (workspace)\
📄 notebook/arquivo Python\
🛢️ schema dentro do catálogo\
🗂️ tabela Delta\
📄 (no catálogo/raw) = CSV bruto

------------------------------------------------------------------------

# 📑 Fonte de Dados

Os dados utilizados no projeto foram extraídos de três principais fontes
públicas:

-   **INSS**: Dados de concessões do BPC por mês (2025 jan-jun), disponível em csv.\
    [Fonte: INSS - Dados
    Abertos](https://dadosabertos.inss.gov.br/dataset/beneficios-concedidos-plano-de-dados-abertos-jun-2023-a-jun-2025)
    -   Quantidade de arquivos: 6 CSVs (concessões de jan/25 a jun/25)
-   **IBGE (Censo 2022)**: População total por município, utilizada para
    cálculo de cobertura e identificação do público-alvo.\
    [Fonte: Censo IBGE
    2022](https://www.ibge.gov.br/estatisticas/sociais/trabalho/22827-censo-demografico-2022.html?=&t=downloads/)
    -   Quantidade de arquivos: 1 CSV
-   **Municípios/UF/Região**: Dados de referência com códigos de
    municípios, sigla UF e região geográfica.\
    [Fonte: IBGE -- Tabela de Referência
    Territorial](https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html/)
    -   Quantidade de arquivos: 1 CSV
    -   
------------------------------------------------------------------------

# 🚀 Roadmap

-   [x] Ingestão **Raw → Bronze**\
-   [x] Padronização de colunas\
-   [x] Tratamentos na **Silver**\
-   [x] Construção da **Gold** (cobertura + judicialização)\
-   [ ] Dashboard interativo (Databricks SQL / Power BI)
