# Databricks notebook source
dbutils.widgets.text("data_source", "")
get_data_source = dbutils.widgets.get("data_source")


# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_date_source = dbutils.widgets.get("p_file_date")


# COMMAND ----------

v_date_source

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Including the configuration file
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Including the common function file
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Checking the defined virable for file path
# MAGIC

# COMMAND ----------

raw_folder_path

# COMMAND ----------

# MAGIC %md  
# MAGIC ### Securely Retrieving Credentials Using Databricks Secret Scope
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1n-scope", key="formula1-client-id")
client_secret = dbutils.secrets.get(scope="formula1n-scope", key="formula1n-client-secret")
tenant_id = dbutils.secrets.get(scope="formula1n-scope", key="formula1n-tenant-id")


# COMMAND ----------

# MAGIC %md  
# MAGIC ### Listing Secrets in "formula1n-scope"
# MAGIC

# COMMAND ----------

dbutils.secrets.list("formula1n-scope")



# COMMAND ----------

# MAGIC %md  
# MAGIC ### Connecting ADLS Storage to Databricks using OAuth
# MAGIC

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databrickscoursetj.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.databrickscoursetj.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.databrickscoursetj.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.databrickscoursetj.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.databrickscoursetj.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

# MAGIC %md  
# MAGIC ### Defining ADLS Path for Accessing Data
# MAGIC

# COMMAND ----------

storage_account_name = "databrickscoursetj"
container_name = "raw"
file_name = "circuits.csv"

adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{file_name}"


# COMMAND ----------

# MAGIC %md  
# MAGIC ### Reading CSV File into Spark DataFrame from Raw Folder
# MAGIC

# COMMAND ----------

circuits_df = spark.read \
    .option("inferSchema", "true") \
        .csv(f"{raw_folder_path}/{v_date_source}/circuits.csv", header = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only required columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, lit

circuits_selected_df = circuits_df.select(
    col("circuitId"), 
    col("circuitRef"), 
    col("name"), 
    col("location"), 
    col("country"), 
    col("lat"), 
    col("lng"), 
    col("alt")  

)



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Column Renaming ###
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

circuits_column_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
  .withColumnRenamed("circuitRef", "circuit_Ref") \
                  .withColumnRenamed("lat", "latitude") \
                      .withColumnRenamed("lng", "longitude") \
                          .withColumnRenamed("alt", "altitude") \
                            .withColumn("data_source", lit(get_data_source)) \
                              .withColumn("file_date", lit(v_date_source))




# COMMAND ----------

# MAGIC %md  
# MAGIC ### Adding "ingestion_date" Column with Current Timestamp
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
circuits_final_df = add_ingestion_date(circuits_column_renamed_df)


# COMMAND ----------

# MAGIC %md  
# MAGIC ### Writing DataFrame to Delta Format in Processed Folder
# MAGIC

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

    


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("Success")