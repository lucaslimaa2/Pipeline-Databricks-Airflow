# Pipeline de Dados com Azure Databricks e Airflow
![Slide1](https://github.com/user-attachments/assets/f8df3527-6f2b-4819-a46a-c55a04e78260)

Neste projeto, decidi unir dois assuntos que gosto bastante: dados e mercado financeiro (neste caso, cotações de moedas). O objetivo é levantar diariamente as cotações do Real Brasileiro (BRL) frente a uma cesta de outras moedas. Para atingir isso, desenvolvi este Pipeline que extrai os dados das moedas da API Exchange Rates (https://exchangeratesapi.io/), transforma e armazena-os em um destino final. 

A stack utilizada foi:
- Azure Databricks
- Python e Pyspark
- Apache Airflow

## 1) Extração de dados
O primeiro passo do projeto foi desenvolver um notebook no Databricks para realizar uma requisição da API da Exchange Rates, transformar os dados e salvar na camada bronze.

Foram desenvolvidas 3 funções no primeiro notebook para: 
# a) Realizar uma requisição do tipo GET:

![image](https://github.com/user-attachments/assets/519c3a60-d808-4b2d-b1d9-119a9db49f9e)

b) Transformar os dados do formato json para um dataframe:

![image](https://github.com/user-attachments/assets/d6394854-f556-4bac-bed8-ace69b1c11f9)

c) Criar a pasta no Databricks File System (DBFS), chamar a função do item 'b' para converter o formato dos dados para um dataframe, renomear colunas, criar coluna de data e, por fim, escrever o arquivo no formato parquet na camada bronze:

![image](https://github.com/user-attachments/assets/ef59b0cd-ac96-4e8f-961b-efd0db8bcb42)

Chamando as funções:

![image](https://github.com/user-attachments/assets/962c1e1b-f365-414a-bd2e-958c42ac69d6)

O parâmetro 'data_execucao' na primeira função é o dia atual da execução do notebook, este parâmetro será configurado no Airflow posteriormente.

## 2) Transformação dos dados
Agora com os dados na camada bronze, o pipeline executa o segundo notebook, que irá ler, processar e modelar os dados no formato final desejado utilizando a linguagem PySpark.

![image](https://github.com/user-attachments/assets/f8dd5fe1-476c-4b8a-aca4-78fa3c6c26ab)
![image](https://github.com/user-attachments/assets/90a22822-a683-40df-ab0b-6ffd627df64f)
![image](https://github.com/user-attachments/assets/26577763-a2ee-49cf-8f67-9bad9bd44557)
