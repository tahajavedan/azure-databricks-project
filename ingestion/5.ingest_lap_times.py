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
file_name = "results.json"

adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_name}"

# COMMAND ----------

dbutils.widgets.text("data_source", "")
get_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Including the configuration file

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Including the common function file

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, concat, to_timestamp, lit , current_timestamp

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType



lap_time_schema = StructType([
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True), 
    StructField("milliseconds", IntegerType(), True)
])



# COMMAND ----------

lap_times_df = spark.read.schema(lap_time_schema) \
    .csv(f"{raw_folder_path}/lap_times") 

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
        .withColumn("ingestion_date", current_timestamp()) \
            .withColumn("data_source", lit(get_data_source))

        


# COMMAND ----------

lap_times_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.lap_times")


# COMMAND ----------

dbutils.notebook.exit("Success")