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
container_name = "demo"
file_name = "races.csv"

adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_name}"


# COMMAND ----------

dbutils.widgets.text("data_source", "")
get_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_data_source = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Including the configuration file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Including the common function file

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import col, concat, to_timestamp, lit , current_timestamp

# COMMAND ----------

race_df = spark.read \
    .option("inferSchema", "true") \
        .csv(f"abfss://raw@databrickscoursetj.dfs.core.windows.net/{v_data_source}/races.csv", header = True)


# COMMAND ----------

race_columns_selected = race_df.select(

col("raceId"),
col("year"),
col("round"),
col("circuitId"),
col("name"),
col("date"),
col("time")

)

# COMMAND ----------

race_columns_renamed = race_columns_selected.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
        .withColumnRenamed("circuitId", "circuit_id") \
            .withColumn("data_source", lit(get_data_source))\
                .withColumn("file_date", lit(v_data_source))
         

# COMMAND ----------

add_race_column = race_columns_renamed.withColumn("ingestion_date", current_timestamp())
##adding the second column(combining two columns)

race_time_stamp_df = add_race_column.withColumn(
    "race_timestamp", 
    to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')
)

# COMMAND ----------

race_final_df = race_time_stamp_df.select("race_id","race_year","round","circuit_id","name","ingestion_date","file_date","race_timestamp")


# COMMAND ----------

race_final_df.write.mode("overwrite") \
    .format("delta") \
    .saveAsTable("f1_processed.races")


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL f1_processed.races;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Success")