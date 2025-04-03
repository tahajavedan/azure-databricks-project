# Databricks notebook source
client_id = dbutils.secrets.get(scope="formula1n-scope", key="formula1-client-id")
client_secret = dbutils.secrets.get(scope="formula1n-scope", key="formula1n-client-secret")
tenant_id = dbutils.secrets.get(scope="formula1n-scope", key="formula1n-tenant-id")


spark.conf.set("fs.azure.account.auth.type.databrickscoursetj.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.databrickscoursetj.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.databrickscoursetj.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.databrickscoursetj.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.databrickscoursetj.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

# MAGIC %run
# MAGIC
# MAGIC "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,  count

# COMMAND ----------

# MAGIC %md ### Reading all the files

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
race_df = spark.read.parquet(f"{processed_folder_path}/races")
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")
constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")
results_df = spark.read.parquet(f"{processed_folder_path}/results")

# COMMAND ----------

# MAGIC %md ### Renaming duplicate columns  

# COMMAND ----------

circuits_df = circuits_df.withColumnRenamed("name","circuit_name").withColumnRenamed("location", "circuit_location")
race_df = race_df.withColumnRenamed("name","race_name").withColumnRenamed("race_timestamp","race_date")
drivers_df = drivers_df.withColumnRenamed("name" , "driver_name").withColumnRenamed("nationality", "driver_nationality")
constructors_df = constructors_df.withColumnRenamed("name","team_name")



# COMMAND ----------

# MAGIC %md ### First Join with Circuits and Race

# COMMAND ----------

cir_race_df = race_df.join(circuits_df, race_df.circuit_id == circuits_df.circuit_id, "inner") \
    .select(race_df.race_id, race_df.race_name,race_df.race_year,race_df.race_date,circuits_df.circuit_location)


# COMMAND ----------

# MAGIC %md ### Join with Result and the remaining dataframes

# COMMAND ----------

race_results_df = results_df.join(cir_race_df, results_df.race_id == cir_race_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_Id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

# MAGIC %md ### Selecting final columns

# COMMAND ----------

all_final_df = race_results_df.select( "race_name", "race_year", "race_date", "circuit_location", "driver_name", "position", "driver_nationality","team_name","grid","fastestLap","points")


# COMMAND ----------

# MAGIC %md ### Adding date column

# COMMAND ----------

all_final_df = all_final_df.withColumn("created_date", current_timestamp())

# COMMAND ----------

# MAGIC %md ### Filtering data

# COMMAND ----------

## all_final_df = all_final_df.filter("(race_year in (2019,2020)) and race_name == 'Abu Dhabi Grand Prix'").orderBy("points", ascending = False)


# COMMAND ----------

# MAGIC %md ### Final DataFrame to write

# COMMAND ----------

all_final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results_final")

# COMMAND ----------

agg = spark.read.parquet(f"{presentation_folder_path}/race_results_final")


# COMMAND ----------

# MAGIC %md ### Driver with the most wins

# COMMAND ----------

demo_window = agg.groupBy("driver_name","race_year").sum("points").orderBy("sum(points)", ascending = False) \
    .withColumnRenamed("sum(points)", "total_points").orderBy("total_points", ascending = False)

# COMMAND ----------

from pyspark.sql import Window; from pyspark.sql.functions import desc, rank


demo_partition = Window.partitionBy("race_year").orderBy(desc("total_points"))   

# COMMAND ----------

rank = demo_window.withColumn("rank", rank().over(demo_partition))

# COMMAND ----------

display(rank.filter("race_year == 2020"))

# COMMAND ----------

