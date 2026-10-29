# Projeto de Dados Automotivos — Hyundai i30

Projeto de engenharia de dados para acompanhar viagens, consumo, abastecimentos e ganhos com caronas. Os arquivos CSV são mantidos no GitHub e processados no Databricks Free Edition.

## Objetivos

- Versionar os arquivos de origem no GitHub.
- Ler os CSVs pelo Git folder sincronizado no Databricks.
- Persistir os dados brutos em tabelas Delta na camada Bronze.
- Tratar, limpar, tipar e enriquecer os dados na camada Silver.
- Consolidar KPIs diários e métricas financeiras na camada Gold.
- Automatizar a pipeline completa e orquestrar jobs.
- Criar um dashboard nativo no Databricks e um painel HTML/CSS/JSON desacoplado.

## Arquitetura

```text
GitHub — CSVs de origem (Landing Zone)
        │
        │ Sincronização do Git folder no Databricks
        ▼
Notebook de validação — testa leitura dos CSVs
        │
        ▼
Notebook 01_bronze_ingestion
        │
        ▼
Unity Catalog — tabelas Delta Bronze (`will.i30.bronze_*`)
        │
        ▼
Notebooks 02_silver_* — limpeza, tipagem, enriquecimento e Dicionário (`00_dicionario_de_dados_silver`)
        │
        ▼
Unity Catalog — tabelas Delta Silver (`will.i30.silver_*`)
        │
        ▼
Notebook 03_gold_kpis_diarios — consolidação e KPIs com arredondamento monetário (2 casas decimais) + Dicionário (`00_dicionario_de_dados_gold`)
        │
        ▼
Unity Catalog — tabela Delta Gold (`will.i30.gold_kpis_diarios`)
        ├───────────────────────────────────────┤
        ▼                                       ▼
Dashboard Nativo no Databricks         Painel HTML / CSS / JSON desacoplado
```

### GitHub como origem LND

Os CSVs são mantidos no repositório GitHub, na pasta `i30/datasource/`. O repositório é conectado ao Databricks por um Git folder. Após atualizar os arquivos no GitHub, sincronize o Git folder no Databricks antes de executar novamente a ingestão.

Os Notebooks são mantidos no repositório GitHub, na pasta `i30/notebook/`. 

O notebook lê os arquivos da cópia sincronizada no workspace. A camada Bronze persiste uma cópia dos dados em Delta no UnityCatalog.

## Estrutura do projeto

```text
i30/
├── README.md
├── datasource/
│   ├── registro_viagens.csv
│   ├── registro_caronas.csv
│   └── registro_abastecimento.csv
└── notebooks/
    ├── 00_validacao_csv_table
    ├── 00_dicionario_de_dados_silver
    ├── 00_dicionario_de_dados_gold
    ├── 01_bronze_ingestion
    ├── 02_silver_registro_viagens
    ├── 02_silver_registro_caronas
    ├── 02_silver_registro_abastecimento
    └── 03_gold_kpis_diarios
```

## Fontes de dados

### `registro_viagens.csv`

Registros diários de quilometragem e desempenho do veículo.

| Campo | Descrição |
|---|---|
| `DATA` | Data do registro da viagem. |
| `INICIAL` | Quilometragem do odômetro no início do percurso, em km. |
| `FINAL` | Quilometragem do odômetro no final do percurso, em km. |
| `PAINEL_L_100KM` | Consumo médio indicado pelo painel, em litros por 100 km. |
| `KM_H` | Velocidade média registrada, em km/h. |

### `registro_caronas.csv`

Registros de passageiros e valores recebidos com caronas.

| Campo | Descrição |
|---|---|
| `DATA` | Data do registro das caronas. |
| `MANHA_QTD` | Quantidade de passageiros no período da manhã. |
| `MANHA_RS` | Valor recebido com caronas pela manhã, em reais. |
| `NOITE_QTD` | Quantidade de passageiros no período da noite. |
| `NOITE_RS` | Valor recebido com caronas à noite, em reais. |

### `registro_abastecimento.csv`

Registros dos abastecimentos e respectivos custos.

| Campo | Descrição |
|---|---|
| `DATA` | Data do abastecimento. |
| `R$_LITRO` | Preço por litro de combustível, em reais. |
| `R$_TOTAL` | Valor total pago, em reais. |
| `LITROS` | Quantidade abastecida, em litros. |

## Camadas Implementadas (Bronze, Silver e Gold)

- Catálogo: `will`
- Esquema: `i30`

### 1. Camada Bronze (`bronze_*`)
Ingestão crua mantendo campos originais com metadados de auditoria (`_ingested_at`, `_source_file`) via `overwrite`.

### 2. Camada Silver (`silver_*`)
- **`silver_viagens`**: Padronização de datas, tipagem e cálculo de `KM_RODADAS` (`FINAL - INICIAL`).
- **`silver_caronas`**: Tipagem de passageiros e ganhos, calculando `TOTAL_PASSAGEIROS` e `TOTAL_GANHO_REAIS`.
- **`silver_abastecimento`**: Tipagem de preços e totais, calculando `LITROS_ABASTECIDOS`.
- **Dicionário aplicado**: `00_dicionario_de_dados_silver` preenchendo descrições e regras de negócio no Unity Catalog.

### 3. Camada Gold (`gold_kpis_diarios`)
- Cruzamento (`JOIN`) por `DATA` entre viagens, caronas e abastecimento.
- KPIs e colunas monetárias calculadas com arredondamento estrito de 2 casas decimais:
  - `LUCRO_LIQUIDO_DIA`: `TOTAL_GANHO_REAIS - CUSTO_ABASTECIMENTO_REAIS`
  - `CUSTO_POR_KM`: `CUSTO_ABASTECIMENTO_REAIS / KM_RODADAS`
- **Dicionário aplicado**: `00_dicionario_de_dados_gold` documentando a tabela e colunas no Unity Catalog.

## Validação de acesso aos CSVs

O notebook de validação é independente da ingestão Bronze. Sua função é somente confirmar que os CSVs do Git folder podem ser encontrados e lidos pelo Spark.

## Próximas etapas

1. Configurar orquestração e jobs agendados no Databricks.
2. Desenvolver o dashboard analítico nativo no Databricks.
3. Desenvolver o painel externo desacoplado (HTML, CSS e JSON).
