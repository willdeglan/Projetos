
# 📊 Web Scraping - Histórica dos Números da Mega-Sena

Este projeto tem como objetivo extrair os resultados históricos da Mega-Sena, desde 1996 até a data atual, utilizando **web scraping com BeautifulSoup** e **Pandas** para transformar os dados em um DataFrame estruturado.

---

## 🚀 Objetivos

- Coletar todos os números sorteados na Mega-Sena desde 1996.
- Organizar os dados em um DataFrame no formato tabular.
- Utilizar o notebook (no Databricks) para facilitar a análise e visualização.

---

## 🧰 Tecnologias e Bibliotecas Utilizadas

- Python
- Pandas
- BeautifulSoup4
- urllib
- datetime
- Databricks (ambiente de execução)

---

## 📁 Estrutura do Código

### 🔹 1. Instalação de dependências

Instale os pacotes necessários para fazer web scraping, conforme exemplo abaixo.

```python
%pip install beautifulsoup4
```

---

### 🔹 2. Importação de bibliotecas

Essas são as bibliotecas utilizadas para manipulação de dados, requisição HTTP e parsing do HTML.

```python
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
from datetime import date
```

---

### 🔹 3. Definição da URL e parâmetros

Define o endereço base (URL) para scraping e o intervalo de tempo (de 1996 até o ano atual).

```python
url = 'https://asloterias.com.br/resultados-da-mega-sena-'
start_year = 1996
end_year = date.today().year
```

---

### 🔹 4. Extração dos dados com web scraping

Faz o scraping anual dos resultados da Mega-Sena e armazena os elementos HTML que contêm os números sorteados.

```python
html_list = []
for p in range(1, (end_year - start_year)):
    list = []
    req = Request(url + str(start_year + p), headers={'User-Agent': 'Mozilla/5.0'})
    html = urlopen(req)
    site = BeautifulSoup(html.read(), 'html.parser')
    list.append(site.find_all('span' , {'class': 'dezenas dezenas_mega'}))
    html_list.append(list)
```

---

### 🔹 5. Transformação dos dados

Extrai o texto (os números) dos elementos HTML e armazena todos em uma lista única.

```python
lista =[]
for a in range(len(html_list)):
    for i in html_list[a][0]:
        lista.append(i.text)
```

---

### 🔹 6. Criação do DataFrame com Pandas

Converte a lista de números em um DataFrame para futura análise e visualização.

```python
df_numeros_da_mega = pd.DataFrame(lista)
```

---

## 📈 Exemplo de Saída

O DataFrame resultante contém os números sorteados da Mega-Sena organizados em uma única coluna.

Total de aproximadamente **16.600 entradas** (equivalente à soma de todos os jogos realizados até hoje).

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

---


## 📄 Licença

Este projeto está sob a licença MIT. Sinta-se livre para utilizar, modificar e compartilhar com créditos ao autor.

---

## 👨‍💻 Autor

### *Willdeglan de S. Santos*
Data Engineer | DBA | Criador do SQL Dicas  
🔗 [LinkedIn: @Willdeglan](https://www.linkedin.com/in/willdeglan)  
📘 [LinkedIn: @sqldicas](https://www.linkedin.com/company/sqldicas)  

---

<img width="769" height="1503" alt="image" src="https://github.com/user-attachments/assets/d1b06074-3a60-4076-9aa6-00685c942f60" />

