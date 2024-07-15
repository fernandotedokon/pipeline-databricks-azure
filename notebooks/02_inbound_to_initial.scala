// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/01_inbound")
// MAGIC

// COMMAND ----------

val path = "dbfs:/mnt/dados/01_inbound/dados_brutos_imoveis"
val dados = spark.read.json(path)

// COMMAND ----------

display(dados)

// COMMAND ----------

val dados_anuncio = dados.drop("imagens", "usuario")
display(dados_anuncio)

// COMMAND ----------

import org.apache.spark.sql.functions.col

val df_initial = dados_anuncio.withColumn("id", col("anuncio.id"))
display(df_initial)

// COMMAND ----------

val path = "dbfs:/mnt/dados/02_transformation_initial/dataset_imoveis"
df_initial.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------


