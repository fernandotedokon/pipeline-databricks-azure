// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/02_transformation_initial")

// COMMAND ----------

val path = "dbfs:/mnt/dados/02_transformation_initial/dataset_imoveis/"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

display(df)

// COMMAND ----------

display(df.select("anuncio.*"))

// COMMAND ----------

display(
  df.select("anuncio.*", "anuncio.endereco.*")
)

// COMMAND ----------

val dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")

// COMMAND ----------

display(dados_detalhados)

// COMMAND ----------

val df_end = dados_detalhados.drop("caracteristicas", "endereco")
display(df_end)

// COMMAND ----------

val path = "dbfs:/mnt/dados/03_inbound_to_end/dataset_imoveis"
df_end.write.format("delta").mode("overwrite").save(path)

// COMMAND ----------

val path = "dbfs:/mnt/dados/03_transformation_end/dataset_imoveis"
df_end.write.format("delta").mode("overwrite").save(path)

// COMMAND ----------


