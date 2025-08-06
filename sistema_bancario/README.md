# Sistema Bancário em Python

Este é um projeto desenvolvido em Python com o objetivo de simular um sistema bancário básico, operando diretamente pelo terminal.

Criado com fins didáticos, este sistema é ideal para quem está iniciando no mundo da programação e deseja colocar em prática diversos conceitos importantes, como:

- Estruturas condicionais (`if`, `elif`, `else`)
- Laços de repetição (`while True`)
- Declaração e organização de funções
- Manipulação de datas com `datetime`
- Uso de listas e dicionários para representar dados de clientes e contas
- Argumentos posicionais (`/`) e nomeados (`*`) para tornar as funções mais seguras e legíveis
- Formatação visual no terminal com tabulação (`\t`)
- Aplicação da biblioteca `textwrap` para exibir menus alinhados de forma limpa

Este projeto também representa boas práticas de organização e modularização de código, incentivando o uso de funções bem definidas e separação clara de responsabilidades.

---

## Funcionalidades Implementadas

- 🧾 **Cadastro de Cliente (Usuário)** com CPF, nome, data de nascimento e endereço.
- 🧾 **Criação de Conta Corrente** associada a um cliente existente.
- 👥 **Listagem de Clientes**
- 💳 **Listagem de Contas Bancárias**
- 💰 **Depósito** com verificação de valores positivos e registro de data/hora.
- 🏧 **Saque** com controle de:
  - Valor limite por saque
  - Número máximo de saques diários
  - Verificação de saldo disponível
- 📄 **Extrato** de todas as operações com data e hora.
- ❌ **Encerrar o programa com segurança**

---

## Lógica por trás do código

O programa utiliza um **laço de repetição `while True`** para manter o menu sempre disponível até que o usuário deseje sair (`opção: Sair`).  
Cada operação é processada com estrutura `if/elif/else`.

O sistema foi **organizado em funções** específicas para cada operação, melhorando a legibilidade e manutenção do código.

Além disso, o projeto faz uso de:

- `argumento por posição` (`/`) → para garantir que certos parâmetros sejam passados apenas por ordem.
- `argumento nomeado` (`*`) → para forçar que certos parâmetros sejam passados explicitamente pelo nome, aumentando a clareza.
- As bibliotecas:
  -  ```textwrap``` → Usada para organizar visualmente o menu no terminal com indentação adequada.
  -  ```ABC``` → Usada para criação de classes abstratas e a definição de métodos abstratos
  -  ```datetime ``` → Usada para registrar a hora exata de cada transação.      

### 1. Exemplo do Menu Principal com `textwrap`

O menu é apresentado de forma organizada, com uso de tabulação (`\t`) para alinhamento e clareza, e utiliza a biblioteca `textwrap` para remover indentação indesejada do menu multi-linha.

```python
    ================ MENU ================
    |  [nu] Novo cliente                 |
    |  [nc] Nova conta                   |   
    --------------------------------------
    |  [lu] Listar cliente               |
    |  [lc] Listar contas                |
    --------------------------------------
    |  [d]  Depositar                    |
    |  [s]  Sacar                        |
    |  [e]  Extrato                      |
    --------------------------------------
    |  [q]  Sair                         |
    |                                    |
    ===== by Willdeglan / SQL Dicas ======
```

---
### `[nu]` Novo Cliente
Permite criar clientes bancario com os campos:
- nome
- CPF
- Data de nascimento
- Endereço
Alem de não aceitar que o CPF seja cadastrado mais de uma vez.

### `[nc]` Nova conta 
Cria uma conta para um cliente já cadastrado, associando dados automaticamente como:
- agência
- número da conta e
- dados do titular

### `[d]`  Depositar
O deposito é feito em uma conta especifica e deve ser informado o CPF, o Valor e a Conta seguindo os seguintes critérios:
- O valor digitado deve ser positivo.
- A data e hora do depósito são registradas com `datetime.now()`.
- O valor é adicionado ao saldo e registrado no extrato.

### `[s]`  Sacar
- Quando vai realizar o saque, o sistema realiza algumas validações antes de executar a operação:
  - Saldo: se é o suficiente
  - Limite de saque: se o valor está dentro do limite de saque diario
  - Quantidade de saques: se a quantidade de saque está dentro do limite estabelecido
- Se todas as validações foram verdadeiras:
  - Subtrai do saldo
  - Registra a data e hora
  - Atualiza o extrato

### `[e]`  Extrato 
Exibe todas as transações com: 
- Data/Hora
- Tipo de transação e 
- Valor.
Caso não haja movimentações, mostra uma mensagem informativa.
    
### `[lu]` Listar cliente
- Mostra a lista de todos os clientes cadastrados com seus dados.

### `[lc]` Listar contas
- Lista todas as contas bancárias com agência, número, titular e saldo.

### [q]  Sair  
- Sai do sistema 
---

### Quer testar esse projeto? 
1. Faz o clone o repositório aí:
```
git clone https://github.com/willdegl4n/SistemaBancario.git
```
2. Acessa a pasta:
```
cd SistemaBancario
```
3. Executa o arquivo:
```
python SistemaBancario.py
```

---

### Contribuições
Todas as contribuições são bem-vindas!
1. Faça um fork
2. Crie sua branch: ```git checkout -b minha-feature```
3. Commit suas alterações: ```git commit -m 'Adiciona nova feature'```
4. Push: ```git push origin minha-feature```
5. Crie um Pull Request

---

**Esse é um projeoto do Bootcamp `Engenheiro de dados com Python` da DIO e foi desenvolvido por:** <br> 
## Willdeglan de Sousa Santos
[LinkedIn](https://www.linkedin.com/in/willdeglan) | [GitHub](https://github.com/willdegl4n)

_Obs.: o desenvolvimento desse README foi inspirado no README da [Dayane Teodoro](https://github.com/Dayanebiaerafa)_
