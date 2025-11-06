# Databricks notebook source
#Bibliotecas
from pyspark.sql.functions import current_date, current_timestamp, current_timezone, convert_timezone, lit, expr

#Nome do catalogo, database e tabelas
catalog = "kpuudata"
database = "bronze"
tabela = "estabelecimentos"

#Caminho Arquivo CSV
path_arquivo = "s3://bucket-s3-databricks/estabelecimentos.csv"

df = spark.read.option("Header", True).csv(path_arquivo)



# COMMAND ----------

df = df.withColumn("data_carga", current_date())
df = df.withColumn("data_carga_hora_local", expr("current_timestamp() - INTERVAL 3 HOURS"))




# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f'{catalog}.{database}.{tabela}')

