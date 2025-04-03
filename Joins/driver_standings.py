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

# MAGIC %md
# MAGIC ### Including the configuration file
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Including the common function file

# COMMAND ----------

from pyspark.sql.functions import col, concat, to_timestamp, lit , current_timestamp ,when

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results_final")

# COMMAND ----------

display(race_results)

# COMMAND ----------

from pyspark.sql.functions import sum, when, col

standing = (race_results
    .groupBy("race_year", "driver_name", "driver_nationality", "team_name")
    .agg(
        sum("points").alias("total_points"),  # Correct sum of points
        sum(when(col("position") == 1, 1).otherwise(0)).alias("wins")  # Correct sum for wins
    )
)

display(standing)


# COMMAND ----------

from pyspark.sql.window  import Window
from pyspark.sql.functions import rank

# COMMAND ----------

# MAGIC %md ###Filter for 2020 season

# COMMAND ----------


standing_2020 = standing.filter("race_year == 2020")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Define window specification for ranking

# COMMAND ----------

standing_window = Window.orderBy(col("total_points").desc(), col("wins").desc())

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Apply the ranking function to the filtered DataFrame

# COMMAND ----------



final_rank = standing_2020.withColumn("rank", rank().over(standing_window))

# COMMAND ----------

# MAGIC %md ### Display the result
# MAGIC
# MAGIC

# COMMAND ----------

final_rank.write.mode("overwrite").parquet(f"{presentation_folder_path}/drivers_final_rank")

# COMMAND ----------

