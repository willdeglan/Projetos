i30
# 🚗 Data Car Project - Pipeline de Dados Automotivo

Projeto de Engenharia de Dados desenvolvido para centralizar, processar e analisar o desempenho, custos e rendimento do veículo utilizando a arquitetura medalhão no **Databricks (Free Edition)** e versionamento de dados via **GitHub**.

---

## 🏗️ Arquitetura do Projeto
- **Origem:** Repositório no GitHub contendo arquivos CSV brutos.
- **Camada Bronze:** Ingestão dos dados brutos diretamente no Databricks mantendo o formato original.
- **Camada Silver:** Limpeza, padronização e enriquecimento dos dados.
- **Camada Gold:** Tabelas agregadas prontas para análise e visualização de KPIs.

---

## 📊 Tabelas Iniciais (Camada Bronze)

Abaixo estão descritas as tabelas e campos iniciais que alimentam o pipeline:

### 1. `registro_viagens`
Responsável por registrar o desempenho e as métricas de cada percurso realizado.
* **`DATA`**: Data em que a viagem foi realizada.
* **`KM_INICIAL`**: Quilometragem do veículo no início do percurso.
* **`KM_FINAL`**: Quilometragem do veículo ao término do percurso.
* **`PAINEL_L_100KM`**: Consumo médio de combustível indicado no painel (Litros por 100 km).
* **`VELOC_MEDIA_KM_H`**: Velocidade média desenvolvida durante a viagem (km/h).

### 2. `registro_caronas`
Controla o fluxo de passageiros e os ganhos financeiros obtidos com caronas.
* **`DATA`**: Data do registro dos ganhos e passageiros.
* **`PASSAG_MANHA`**: Quantidade de passageiros transportados no período da manhã.
* **`GANHO_MANHA_REAIS`**: Valor financeiro arrecadado com as caronas pela manhã (R$).
* **`PASSAG_NOITE`**: Quantidade de passageiros transportados no período da noite.
* **`GANHO_NOITE_REAIS`**: Valor financeiro arrecadado com as caronas à noite (R$).

### 3. `registro_abastecimento`
Mapeia os custos operacionais com combustível.
* **`DATA`**: Data em que foi realizado o abastecimento.
* **`POSTO_PRECO_LITRO`**: Preço cobrado por litro de combustível no posto (R$/L).
* **`POSTO_VALOR_TOTAL`**: Valor total pago no abastecimento (R$).

## 📂 Estrutura do Repositório

O projeto está organizado com a seguinte estrutura de diretórios:

```text
/
├── datasource/
│   ├── registro_viagens.csv
│   ├── registro_caronas.csv
│   └── registro_abastecimento.csv
└── notebooks/
    └── 01_bronze_ingestion.py
```


