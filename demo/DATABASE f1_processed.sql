-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed



-- COMMAND ----------

USE f1_processed



-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC client_id = dbutils.secrets.get(scope="formula1n-scope", key="formula1-client-id")
-- MAGIC client_secret = dbutils.secrets.get(scope="formula1n-scope", key="formula1n-client-secret")
-- MAGIC tenant_id = dbutils.secrets.get(scope="formula1n-scope", key="formula1n-tenant-id")
-- MAGIC
-- MAGIC
-- MAGIC spark.conf.set("fs.azure.account.auth.type.databrickscoursetj.dfs.core.windows.net", "OAuth")
-- MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.databrickscoursetj.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.databrickscoursetj.dfs.core.windows.net", client_id)
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.databrickscoursetj.dfs.core.windows.net", client_secret)
-- MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.databrickscoursetj.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### CREATING CALCULTED RACE RESULTS TABLE IN THE PRESENTATION LAYER
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS
SELECT races.race_year,
       constructors.name AS team_name,
       drivers.name AS driver_name,
       results.position,
       results.points,
       11 - results.position AS calculated_points
  FROM results 
  JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
  JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
  JOIN f1_processed.races ON (results.race_id = races.race_id)
 WHERE results.position <= 10

-- COMMAND ----------

SELECT * from f1_presentation.calculated_race_results

-- COMMAND ----------

-- MAGIC
-- MAGIC
-- MAGIC  %md
-- MAGIC ### Create Races table
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for JSON files
-- MAGIC
-- MAGIC #### Create constructors table
-- MAGIC - Single Line JSON  
-- MAGIC - Simple structure  
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create drivers table
-- MAGIC * Single Line JSON
-- MAGIC * Complex structure
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md ##### Create results table
-- MAGIC * Single Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create pit stops table
-- MAGIC * Multi Line JSON
-- MAGIC * Simple structure
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Lap Times Table
-- MAGIC * CSV file
-- MAGIC * Multiple files
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Qualifying Table
-- MAGIC * JSON file
-- MAGIC * MultiLine JSON
-- MAGIC * Multiple files