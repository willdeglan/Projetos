# Projeto de Dados Automotivos — Hyundai i30

Projeto de engenharia de dados para acompanhar viagens, consumo, abastecimentos e ganhos com caronas. Os arquivos CSV são mantidos no GitHub e processados no Databricks Free Edition.

## Objetivos

- Versionar os arquivos de origem no GitHub.
- Ler os CSVs pelo Git folder sincronizado no Databricks.
- Persistir os dados brutos em tabelas Delta na camada Bronze.
- Preparar camadas de tratamento e análise.
- Criar um dashboard com indicadores de consumo, gastos e ganhos com caronas.

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
Unity Catalog — tabelas Delta Bronze
        │
        ▼
Silver — limpeza, tipagem e validações (próxima etapa)
        │
        ▼
Gold — indicadores para análise (próxima etapa)
        │
        ▼
Dashboard Databricks (próxima etapa)
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
    ├── 00_dicionario_de_dados
    ├── 01_bronze_ingestion
    ├── 02_silver_[...]
    └── 03_gold_[...]
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

## Camada Bronze — implementada

O notebook `01_bronze_ingestion` lê os três CSVs sincronizados no Git folder e grava tabelas Delta no Unity Catalog:

- Catálogo: `will`
- Esquema: `i30`

| Tabela Delta | Arquivo de origem |
|---|---|
| `will.i30.bronze_viagens` | `registro_viagens.csv` |
| `will.i30.bronze_caronas` | `registro_caronas.csv` |
| `will.i30.bronze_abastecimento` | `registro_abastecimento.csv` |

A ingestão mantém os campos da origem sem inferir tipos e acrescenta:

- `_ingested_at`: data e hora da carga.
- `_source_file`: nome do arquivo CSV de origem.

A gravação usa `overwrite`: cada execução substitui o conteúdo das tabelas Bronze pelos dados da versão atual dos CSVs sincronizados.

O notebook também pode aplicar uma descrição à tabela e comentários às colunas, explicando o significado dos dados e dos campos de ingestão.

## Validação de acesso aos CSVs

O notebook de validação é independente da ingestão Bronze. Sua função é somente confirmar que os CSVs do Git folder podem ser encontrados e lidos pelo Spark. Ele não grava tabelas nem depende de variáveis criadas em outro notebook.

## Indicadores planejados

Com os campos atuais, as próximas camadas poderão preparar indicadores como:

- Quilômetros por dia (`FINAL - INICIAL`).
- Consumo indicado pelo painel por dia.
- Velocidade média por período.
- Litros abastecidos, gasto total e preço médio por litro.
- Passageiros e ganhos com caronas por dia.
- Resultado financeiro: ganhos com caronas menos gastos com combustível.
- Custo estimado por quilômetro, quando os dados necessários estiverem completos.

## Observações e limitações dos dados

- Valores vazios não devem ser interpretados automaticamente como zero: podem indicar dados ainda não preenchidos.
- O consumo indicado pelo painel é diferente do consumo real calculado entre abastecimentos.
- Para calcular o consumo real entre abastecimentos, será necessário registrar a quilometragem do odômetro e indicar se o tanque foi completado em cada abastecimento.
- Datas e valores devem ser validados e convertidos para tipos apropriados na camada Silver.

## Próximas etapas

1. Desenvolver a camada Silver com padronização, tipos e regras de qualidade.
2. Definir as regras de cálculo de consumo, custos e ganhos.
3. Criar tabelas Gold agregadas para análise.
4. Construir um dashboard no Databricks ligado às tabelas Gold.
