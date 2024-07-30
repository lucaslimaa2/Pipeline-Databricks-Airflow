# Databricks notebook source
dbutils.widgets.text("data_execucao", "")
data_execucao = dbutils.widgets.get("data_execucao")

print(f"Data de execução: {data_execucao}")


# COMMAND ----------

import requests
from pyspark.sql.functions import lit

# COMMAND ----------

def extraindo_dados(date, base="BRL"):

    url = f"https://api.apilayer.com/exchangerates_data/{date}&base={base}"

    headers= {
    "apikey": "my-api-key"
    }

    parametros = {"base": base}

    response = requests.request("GET", url, headers=headers, params=parametros)

    if response.status_code != 200:
        raise Exception("Extração mal sucedida")

    return response.json()

# COMMAND ----------

def dados_para_dataframe(dado_json):
    dados_tupla = [(moeda, float(taxa)) for moeda, taxa in dado_json["rates"].items()]
    return dados_tupla  

# COMMAND ----------

def salvar_arquivo_parquet(conversoes_extraidas):
    ano, mes, dia = conversoes_extraidas['date'].split('-')
    caminho_completo = f"dbfs:/projeto-databricks-airflow/bronze/{ano}/{mes}/{dia}"
    response = dados_para_dataframe(conversoes_extraidas)
    df_conversoes = spark.createDataFrame(response, schema=['moeda', 'taxa'])
    df_conversoes = df_conversoes.withColumn("data", lit(f"{ano}-{mes}-{dia}"))
    
    df_conversoes.write.format("parquet")\
                    .mode("overwrite")\
                    .save(caminho_completo)

    print(f"Os arquivos foram salvos em {caminho_completo}")

# COMMAND ----------

cotacoes = extraindo_dados(data_execucao) 
salvar_arquivo_parquet(cotacoes)

# COMMAND ----------


