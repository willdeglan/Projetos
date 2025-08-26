# BPC -- Análise de Judicialização, Cobertura e Prazos

O **Benefício de Prestação Continuada (BPC)** é voltado para pessoas
idosas ou com deficiência em situação de vulnerabilidade.\
Diferente dos benefícios previdenciários, não exige contribuição prévia,
o que o torna um tema de grande relevância social, política e
orçamentária.

A concessão pode ocorrer de duas formas: - **Administrativa** →
diretamente pelo INSS.\
- **Judicial** → quando o pedido é negado e o requerente recorre à
Justiça.

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

O projeto segue a **Arquitetura Medalhão (Medallion Architecture)** no
Databricks, dividida em camadas:

``` mermaid
flowchart LR
    A[Raw\n(CSVs Originais)] --> B[Bronze\nDelta Tables Brutas]
    B --> C[Silver\nDados Tratados e Padronizados]
    C --> D[Gold\nIndicadores Analíticos]
```

------------------------------------------------------------------------

# 📂 Estrutura dos Notebooks

## 🔹 Raw -- Ingestão de Dados

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

## 🔹 Bronze -- Dados Brutos Padronizados

Nesta camada, os dados são convertidos em **Delta Tables**.\
Não há grandes transformações, apenas padronização inicial (snake_case).

-   **`bronze_bpc_ingestao.py`** → Cria tabela
    `tb_bronze_inss_bpc_2025_01a06`.\
-   **`bronze_censo_ingestao.py`** → Cria tabela
    `tb_bronze_censo_2022`.\
-   **`bronze_municipios_ibge_ingestao.py`** → Cria tabela
    `tb_bronze_municipios_ibge`.

------------------------------------------------------------------------

## 🔹 Silver -- Tratamento e Enriquecimento

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

## 🔹 Gold -- Indicadores Analíticos

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
    │       ├── censo_2022.csv
    │       ├── inss_2025_01.csv ... inss_2025_06.csv
    │       ├── municipios_ibge.csv
    │       └── uf_municipios.csv
    │
    ├── 🛢️ bronze (Schema)
    │   ├── tb_bronze_inss_bpc_2025_01a06
    │   ├── tb_bronze_censo_2022
    │   └── tb_bronze_municipios_ibge
    │
    ├── 🛢️ silver (Schema)
    │   ├── tb_silver_inss_bpc_2025
    │   ├── tb_silver_censo_2022
    │   └── tb_silver_municipios_ibge
    │
    └── 🛢️ gold (Schema)
        ├── tb_gold_cobertura_bpc
        └── tb_gold_judicializacao_bpc

Legenda:\
📂 diretório de notebooks (workspace)\
📄 notebook/arquivo Python\
🛢️ schema dentro do catálogo\
🗂️ tabela Delta\
📄 (no catálogo/raw) = CSV bruto

------------------------------------------------------------------------

# 📑 Fonte de Dados

-   **INSS** -- Concessões do BPC (2025 jan-jun).\
    [Fonte](https://dadosabertos.inss.gov.br/dataset/beneficios-concedidos-plano-de-dados-abertos-jun-2023-a-jun-2025)

-   **IBGE (Censo 2022)** -- População residente por município.\
    [Fonte](https://www.ibge.gov.br/estatisticas/sociais/trabalho/22827-censo-demografico-2022.html?=&t=downloads/)

-   **IBGE -- Malhas territoriais** -- Municípios, UFs e regiões.\
    [Fonte](https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html/)

------------------------------------------------------------------------

# 🚀 Roadmap

-   [x] Ingestão **Raw → Bronze**\
-   [x] Padronização de colunas\
-   [x] Tratamentos na **Silver**\
-   [x] Construção da **Gold** (cobertura + judicialização)\
-   [ ] Dashboard interativo (Databricks SQL / Power BI)
