# BPC -- Análise de Judicialização, Cobertura e Prazos

O **Benefício de Prestação Continuada (BPC)** é voltado para pessoas
idosas ou com deficiência em situação de vulnerabilidade. Diferente dos
benefícios previdenciários, não exige contribuição prévia, o que o torna
um tema de grande relevância social, política e orçamentária.

A concessão pode ocorrer: - **Administrativa** → diretamente pelo INSS.\
- **Judicial** → quando o pedido é negado e o requerente recorre à
Justiça.

Compreender **quais regiões apresentam índices elevados de
judicialização e como se comportam os indicadores de cobertura e
prazos** permite tomadas de decisão mais estratégicas. Por exemplo:

-   No **BPC-Idoso**, regiões com alta judicialização e baixa cobertura
    podem sinalizar alto potencial de novas ações.\
-   No **BPC-Deficiente**, que exige perícias mais complexas,
    indicadores como prazos médios e tipos de decisão ajudam a
    identificar áreas críticas.

Este projeto aplica a **Arquitetura Medalhão (Raw → Bronze → Silver →
Gold)** em ambiente **Databricks + Unity Catalog**, garantindo
organização, rastreabilidade e reprodutibilidade.

------------------------------------------------------------------------

# 🎯 Objetivo do Projeto

-   Monitorar concessões do BPC iniciadas em 2025 (jan-jun).\
-   Avaliar cobertura territorial, prazos e judicializações por tipo de
    benefício.\
-   Apoiar decisões estratégicas em advocacia previdenciária e gestão
    pública.

------------------------------------------------------------------------

## 📂 Workspace (Notebooks / Código)

    📂 bpc
    │
    ├── 📂 raw
    │   ├── 📄 conferindoCSVnopandas.py
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

------------------------------------------------------------------------

## 🗄️ Catálogo (Unity Catalog / Hive Metastore)

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
    │   ├── 🗂️ tb_bronze_inss_bpc_2025_01a06 
    │   ├── 🗂️ tb_bronze_censo_2022 
    │   └── 🗂️ tb_bronze_municipios_ibge 
    │
    ├── 🛢️ silver (Schema)
    │   ├── 🗂️ tb_silver_inss_bpc_2025
    │   ├── 🗂️ tb_silver_censo_2022
    │   └── 🗂️ tb_silver_municipios_ibge
    │
    └── 🛢️ gold (Schema)
        ├── 🗂️ tb_gold_cobertura_bpc
        └── 🗂️ tb_gold_judicializacao_bpc

### Esquema visual

    📂 → diretório de notebooks (workspace)
    📄 → notebook/arquivo Python
    🛢️ → schema dentro do catálogo
    🗂️ → tabela Delta
    📄 (no catálogo/raw) → CSV bruto

------------------------------------------------------------------------

## 🔄 Pipeline (Medallion Architecture)

1.  **Raw**
    -   Armazena os arquivos originais (CSV).\
2.  **Bronze**
    -   Ingestão dos dados brutos no formato Delta.\
    -   Ajuste inicial dos nomes de colunas (snake_case).\
3.  **Silver**
    -   Limpeza e padronização de dados.\
    -   Tratamento de duplicidades, normalização de strings e tipagem
        correta.\
    -   Enriquecimento com tabelas de referência (IBGE, Censo).\
4.  **Gold**
    -   Criação de tabelas analíticas:
        -   Cobertura (beneficiários / população).\
        -   Judicialização (% judicial vs administrativo).\
        -   Prazos médios de decisão.\
        -   Rankings por município e UF.

------------------------------------------------------------------------

## 📑 Fonte de Dados

-   **INSS** -- Concessões do BPC (2025 jan-jun).\
    [Fonte: INSS - Dados
    Abertos](https://dadosabertos.inss.gov.br/dataset/beneficios-concedidos-plano-de-dados-abertos-jun-2023-a-jun-2025)\
-   **IBGE (Censo 2022)** -- População residente por município.\
    [Fonte: Censo IBGE
    2022](https://www.ibge.gov.br/estatisticas/sociais/trabalho/22827-censo-demografico-2022.html?=&t=downloads/)\
-   **IBGE -- Malhas territoriais** -- Códigos de municípios, UFs e
    regiões.\
    [Fonte: IBGE - Malhas
    Territoriais](https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html/)

------------------------------------------------------------------------

## 🚀 Roadmap

-   [x] Ingestão **Raw → Bronze**\
-   [x] Padronização de colunas\
-   [x] Tratamentos na **Silver**\
-   [ ] Construção da **Gold** (cobertura + judicialização)\
-   [ ] Dashboard interativo (Databricks SQL / Power BI)
