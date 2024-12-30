# Databricks notebook source
Lendo os dados na camada inbound:

path = "dbfs:/mnt/dados/inbound/dataset_imoveis_bruto.json"
dados = spark.read.json(path)
display(dados)

Removendo as colunas:

df_bronze = dados.drop("imagens", "usuario")
display(df_bronze)

Criando a coluna de identificação:

from pyspark.sql.functions import col

df_bronze = df_bronze.withColumn("id", col("anuncio.id"))
display(df_bronze)

Salvando na camada bronze:

path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode("overwrite").save(path)

Códigos do notebook bronze_to_silver utilizando Python:

Lendo os dados na camada bronze:

path = "dbfs:/mnt/dados/bronze/dataset_imoveis/"
df = spark.read.format("delta").load(path)
display(dados)

Transformando os campos do json em colunas:

display(df.select("anuncio.*"))

display(
  df.select("anuncio.*", "anuncio.endereco.*")
)

df_silver = df.select("anuncio.*", "anuncio.endereco.*")
display(df_silver)

Removendo colunas:

df_silver = df_silver.drop("caracteristicas", "endereco")
display(df_silver)

Salvando na camada silver:

path = "dbfs:/mnt/dados/silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(path)

