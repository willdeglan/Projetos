# Instalação do pacote beautifulsoup4
%pip install beautifulsoup4

# import das bibliotecas
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
from datetime import date

# Definição da variáveis
url = 'https://asloterias.com.br/resultados-da-mega-sena-'
start_year = 1996
end_year = date.today().year

# Extração dos dados
html_list = []
for p in range(1, (end_year - start_year)):
    list = []
    req = Request(url + str(start_year + p), headers={'User-Agent': 'Mozilla/5.0'})
    html = urlopen(req)
    site = BeautifulSoup(html.read(), 'html.parser')
    list.append(site.find_all('span' , {'class': 'dezenas dezenas_mega'}))
    html_list.append(list)

# Transformação dos dados
lista =[]
for a in range(len(html_list)):
    for i in html_list[a][0]:
        lista.append(i.text)

#Criação do DataFrame
df_numeros_da_mega = pd.DataFrame(lista)

# Conferindo o dataframe
df_numeros_da_mega
