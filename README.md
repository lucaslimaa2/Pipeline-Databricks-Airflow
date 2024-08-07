# Pipeline de Dados com Azure Databricks e Airflow
![Slide1](https://github.com/user-attachments/assets/f8df3527-6f2b-4819-a46a-c55a04e78260)

Neste projeto, decidi unir dois assuntos que gosto bastante: dados e mercado financeiro (neste caso, cotações de moedas). O objetivo é levantar diariamente as cotações do Real Brasileiro (BRL) frente a uma cesta de outras moedas. Para atingir isso, desenvolvi este Pipeline que extrai os dados das moedas da API Exchange Rates (https://exchangeratesapi.io/), transforma e armazena-os em um destino final. 

A stack utilizada foi:
- Azure Databricks
- Python e Pyspark
- Apache Airflow

## 1) Extração de dados e carregamento na camada Bronze
O primeiro passo do projeto foi desenvolver um notebook no Databricks para realizar uma requisição da API da Exchange Rates, transformar os dados e salvar na camada bronze.

Foram desenvolvidas 3 funções no primeiro notebook para: 
### a) Realizar uma requisição do tipo GET:

![image](https://github.com/user-attachments/assets/519c3a60-d808-4b2d-b1d9-119a9db49f9e)

### b) Transformar os dados do formato json para um dataframe:

![image](https://github.com/user-attachments/assets/d6394854-f556-4bac-bed8-ace69b1c11f9)

### c) Criar a pasta no Databricks File System (DBFS), chamar a função do item 'b' para converter o formato dos dados para um dataframe, renomear colunas, criar coluna de data e, por fim, escrever o arquivo no formato parquet na camada bronze:

![image](https://github.com/user-attachments/assets/ef59b0cd-ac96-4e8f-961b-efd0db8bcb42)

### Chamando as funções:

![image](https://github.com/user-attachments/assets/962c1e1b-f365-414a-bd2e-958c42ac69d6)

O parâmetro 'data_execucao' na primeira função é o dia atual da execução do notebook, este parâmetro será configurado no Airflow posteriormente.

### Os dados são armazenados na camada bronze particionados por dia, como mostra a imagem abaixo:
![bronze](https://github.com/user-attachments/assets/e544bfbb-cac6-469b-9a38-6e28ed3daa31)


## 2) Transformação dos dados e carregamento na camada Silver
Agora com os dados na camada bronze, o pipeline executa o segundo notebook, que irá ler todos os arquivos de todos os dias, processar e modelar os dados para o formato final desejado utilizando a linguagem PySpark.

![image](https://github.com/user-attachments/assets/f8dd5fe1-476c-4b8a-aca4-78fa3c6c26ab)
![image](https://github.com/user-attachments/assets/90a22822-a683-40df-ab0b-6ffd627df64f)
![image](https://github.com/user-attachments/assets/26577763-a2ee-49cf-8f67-9bad9bd44557)

### Este é dataframe final que será armazenado diariamente na camada Silver:
![Dataframe final](https://github.com/user-attachments/assets/06ba8296-4f3d-4520-a9d4-f3d6cf49332d)

## Repositórios no DBFS:
![Silver](https://github.com/user-attachments/assets/f2827155-f07c-4773-8cc6-33a368bbd101)

## 3) Setando os jobs
Agora que os dois notebooks responsáveis pelas etapas do Pipeline estão desenvolvidos, é necessário criar os Jobs no Databricks que irão executar os notebooks.

Job Notebook 1
![databricks](https://github.com/user-attachments/assets/733dc142-13e0-473e-ba11-4ec5c8860118)

Job Notebook 2
![job transformar](https://github.com/user-attachments/assets/493ee2ec-a321-48bf-a283-26449802f1fe)

## 4) Orquestração do Pipeline com Airflow
Primeiramente, as devidas conexões do Airflow com o Databricks foram realizadas. Neste projeto, executei o Airflow no modo Standalone em um ambiente virtual do Ubuntu.

![image](https://github.com/user-attachments/assets/20b5f428-88e9-47ee-8161-8377b21ec53e)

Com o Airflow sendo executado sem problemas, a DAG vou desenvolvida para executar os dois jobs, que por sua vez, executarão os notebooks.

Esta DAG é executada todo dia as 12 horas e 10 minutos (o schedule_interval poderia ser trocado somente por "@daily"). Além disso, o parâmetro 'execution_date' é a data atual em que a DAG está sendo executada, e esse parâmetro é transferido para o Databricks, pois ele é necessário na função de extração dos dados da API.

![DAG](https://github.com/user-attachments/assets/36e06d10-0575-4dac-adae-13d8ad5186fe)

### Histórico da execução da DAG

![Airflow](https://github.com/user-attachments/assets/727fa363-daaf-428e-9197-d0098b34833a)

