## Estrutura do BPC
```
📦 bpc
│
├── 📁 notebooks
│    ├── 📁 raw
│    │   ├── raw_bpc_ingestao.py
│    │   ├── raw_censo_ingestao.py
│    │   └── raw_uf_municipios_ingestao.py
│    │
│    ├── 📁 bronze
│    ├── 📁 silver
│    └── 📁 gold
│
├── 📁 source
|    ├── inss_2025_01.xlsx
│    ├── inss_2025_02.xlsx
│    ├── inss_2025_03.xlsx
│    ├── inss_2025_04.xlsx
│    ├── inss_2025_05.xlsx
│    ├── inss_2025_06.xlsx
│    ├── censo_2022.csv
│    └── uf_municipios.csv
│      
└── README.md
```

---
## Fonte de Dados

Os dados utilizados no projeto foram extraídos de três principais fontes púplicas:

- **INSS**: Dados de concessões do BPC por mês, disponível em csv.
 [Fonte: INSS - Dados Abertos](https://dadosabertos.inss.gov.br/dataset/beneficios-concedidos-plano-de-dados-abertos-jun-2023-a-jun-2025)
  - Quantidade de arquivos: 6 cvs - Concessões de jan/25 a jun/25

- **IBGE (Censo 2022)**: População total por município, utilizada para cálculo de cobertura e identificação do público-alvo.  
  [Fonte: Censo IBGE 2022](https://www.ibge.gov.br/estatisticas/sociais/trabalho/22827-censo-demografico-2022.html?=&t=downloads/)
  - Quantidade de arquivos: 1 csv

- **Municípios/UF/Região**: Dados de referência com códigos de municípios, sigla UF e região geográfica.  
  [Fonte: IBGE – Tabela de Referência Territorial](https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html/)
  - Quantidade de arquivos: 1 csv
    
<img width="407" height="366" alt="image" src="https://github.com/user-attachments/assets/defab5e6-5e6f-4fc5-a4ad-f29b34cfa0f8" />

---
https://github.com/fdg-fer/bpc-pipeline-databricks/tree/main
