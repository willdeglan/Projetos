
# 📊 Análise Histórica dos Números da Mega-Sena

Este projeto tem como objetivo extrair e organizar os resultados históricos da Mega-Sena, desde 1996 até a data atual, utilizando **web scraping com BeautifulSoup** e **Pandas** para transformar os dados em um DataFrame estruturado.

---

## 🚀 Objetivos

- Coletar todos os números sorteados na Mega-Sena desde 1996.
- Organizar os dados em um DataFrame no formato tabular.
- Utilizar o notebook Databricks para facilitar a análise e visualização.

---

## 🧰 Tecnologias e Bibliotecas Utilizadas

- Python
- Pandas
- NumPy
- BeautifulSoup4
- urllib
- datetime
- Databricks (ambiente de execução)

---

## 📁 Estrutura do Código

### 🔹 1. Instalação de dependências

```python
%pip install beautifulsoup4
```

Instala o pacote necessário para fazer web scraping, conforme exemplo acima.

---

### 🔹 2. Importação de bibliotecas

```python
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
from datetime import date
```

Bibliotecas utilizadas para manipulação de dados, requisição HTTP e parsing do HTML.

---

### 🔹 3. Definição da URL e parâmetros

```python
url = 'https://asloterias.com.br/resultados-da-mega-sena-'
ano = 1996
data = date.today().year
```

Define o endereço base para scraping e o intervalo de anos (de 1996 até o ano atual).

---

### 🔹 4. Extração dos dados com web scraping

```python
html_list = []
for p in range(1, (data - ano)):
    list = []
    req = Request(url + str(ano + p), headers={'User-Agent': 'Mozilla/5.0'})
    html = urlopen(req)
    site = BeautifulSoup(html.read(), 'html.parser')
    list.append(site.find_all('span' , {'class': 'dezenas dezenas_mega'}))
    html_list.append(list)
```

Faz o scraping anual dos resultados da Mega-Sena e armazena os elementos HTML que contêm os números sorteados.

---

### 🔹 5. Transformação dos dados

```python
lista =[]
for a in range(len(html_list)):
    for i in html_list[a][0]:
        lista.append(i.text)
```

Extrai o texto (os números) dos elementos HTML e armazena todos em uma lista única.

---

### 🔹 6. Criação do DataFrame com Pandas

```python
df_numeros_da_mega = pd.DataFrame(lista)
```

Converte a lista de números em um DataFrame para futura análise e visualização.

---

## 📈 Exemplo de Saída

O DataFrame resultante contém os números sorteados da Mega-Sena organizados em uma única coluna.

| Ord | Números |
|-----|---------|
| 0   | 14      |
| 1   | 18      |
| 2   | 29      |
| ... | ...     |
| 16597 |	177    |
| 16598 |	197    |
| 16599 |	297    |
| 16600 |	507    |
| 16601 |	577    |

Total de aproximadamente **16.000 entradas** (equivalente à soma de todos os jogos realizados até hoje).

---


## 📄 Licença

Este projeto está sob a licença MIT. Sinta-se livre para utilizar, modificar e compartilhar com créditos ao autor.

---

## 👨‍💻 Autor

**Willdeglan S. S.**  
Data Engineer | DBA | Criador do SQL Dicas  
🔗 [LinkedIn: @Willdeglan](https://www.linkedin.com/in/willdeglan)  
📘 [LinkedIn: @sqldicas](https://www.linkedin.com/company/sqldicas)  

---

<img width="769" height="1503" alt="image" src="https://github.com/user-attachments/assets/d1b06074-3a60-4076-9aa6-00685c942f60" />

