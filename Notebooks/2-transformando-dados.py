# Databricks notebook source
df_consolidado = spark.read.parquet("dbfs:/projeto-databricks-airflow/bronze/*/*/*")
df_consolidado.show()

# COMMAND ----------

#Filtrando somente moedas de interesse.

moedas = ['USD', 'EUR', 'GBP']

df_consolidado = df_consolidado.filter(df_consolidado.moeda.isin(moedas))
df_consolidado.show()

# COMMAND ----------

df_consolidado.printSchema()

# COMMAND ----------

# Convertendo a coluna de data no tipo date
from pyspark.sql.functions import to_date

df_consolidado = df_consolidado.withColumn("data", to_date("data"))

# COMMAND ----------

# Realizando um pivot na tabela para que fique no modelo ideal esperado

from pyspark.sql.functions import first

df_consolidado_conversao = df_consolidado.groupby("data")\
                                .pivot("moeda")\
                                .agg(first("taxa"))\
                                .orderBy("data", ascending=True)

df_consolidado_conversao.show()

# COMMAND ----------

dF_conversao_reais = df_consolidado_conversao.select("*")

# COMMAND ----------

# Transformando a taxa de conversão de Real para as demais moedas

from pyspark.sql.functions import col, round

moedas = ['USD', 'EUR', 'GBP']

for moeda in moedas:
    df_consolidado_conversao = df_consolidado_conversao\
                            .withColumn(moeda, round(1/col(moeda), 4))

df_consolidado_conversao.show()

# COMMAND ----------

df_consolidado_conversao = df_consolidado_conversao.coalesce(1)
dF_conversao_reais = dF_conversao_reais.coalesce(1)

# COMMAND ----------

df_consolidado_conversao.write\
    .mode("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save("dbfs:/projeto-databricks-airflow/silver/taxas_conversao")


dF_conversao_reais.write\
    .mode("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save("dbfs:/projeto-databricks-airflow/silver/valores_reais")
