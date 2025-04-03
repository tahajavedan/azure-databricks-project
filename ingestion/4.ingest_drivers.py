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
file_name = "drivers.csv"

adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_name}"

# COMMAND ----------

dbutils.widgets.text("data_source", "")
v_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_get", "2021-03-21")
v_data_source = dbutils.widgets.get("p_file_get")

# COMMAND ----------

# MAGIC
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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

driver_schema = StructType([
    StructField("code", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("driverRef", StringType(), True),
    StructField("name", StructType([
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True)
    ]), True),
    StructField("nationality", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("url", StringType(), True)
])




# COMMAND ----------

drivers_df = spark.read \
    .schema(driver_schema).json(f"{raw_folder_path}/{v_data_source}/drivers.json")

# COMMAND ----------

driver_renamed_df = drivers_df \
    .withColumnRenamed("driverId", "driver_Id") \
    .withColumnRenamed("driverRef", "driver_Ref") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("full_name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
    .withColumn("file_date", lit(v_data_source))

# COMMAND ----------

display(driver_renamed_df)

# COMMAND ----------


driver_final_df = driver_renamed_df.drop("url")


# COMMAND ----------

driver_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")


# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC SELECT * FROM f1_processed.drivers

# COMMAND ----------

# MAGIC %fs
# MAGIC ls abfss://processed@databrickscoursetj.dfs.core.windows.net/
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Success")