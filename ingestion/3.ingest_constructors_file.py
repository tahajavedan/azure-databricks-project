# Databricks notebook source
client_id = dbutils.secrets.get(scope="formula1n-scope", key="formula1-client-id")
client_secret = dbutils.secrets.get(scope="formula1n-scope", key="formula1n-client-secret")
tenant_id = dbutils.secrets.get(scope="formula1n-scope", key="formula1n-tenant-id")


spark.conf.set("fs.azure.account.auth.type.databrickscoursetj.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.databrickscoursetj.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.databrickscoursetj.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.databrickscoursetj.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.databrickscoursetj.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


storage_account_name = "databrickscoursetj"
container_name = "raw"
file_name = "constructors.json"

adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_name}"


# COMMAND ----------

dbutils.widgets.text("data_source", "")
get_data_source = dbutils.widgets.get("data_source")


# COMMAND ----------

dbutils.widgets.text("p_file_get", "2021-03-21")
v_data_source = dbutils.widgets.get("p_file_get")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Including the configuration file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Including the common function file

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, concat, to_timestamp, lit , current_timestamp 

# COMMAND ----------

con_schema = "constructorId INT, constructorRef string, name string, nationality string, url string"

# COMMAND ----------

con_df = spark.read \
    .schema(con_schema) \
        .json(f"{raw_folder_path}/{v_data_source}/constructors.json")


# COMMAND ----------

con_df_drop = con_df.drop("url")


# COMMAND ----------

cons_final_df = (
    con_df_drop
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("constructorRef", "constructor_ref")
    .withColumn("ingestion_date", current_timestamp())
    .withColumn("data_source", lit(get_data_source))
    .withColumn("file_date", lit(v_data_source))
)

# COMMAND ----------

cons_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")



# COMMAND ----------

spark.sql("SELECT * FROM f1_processed.constructors").display()


# COMMAND ----------

dbutils.notebook.exit("Success")