-- Databricks notebook source

CREATE DATABASE IF NOT EXISTS f1_raw

-- COMMAND ----------

USE f1_raw;

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
-- MAGIC ### Loading circuits file
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits
(circuitId INT,
circuit_Ref STRING,
name STRING,
location STRING,
country STRING,
latitude DOUBLE,
longitude DOUBLE,
altitude INT,
url STRING
)
USING csv
OPTIONS (path "abfss://raw@databrickscoursetj.dfs.core.windows.net/circuits.csv", header = True)

-- COMMAND ----------

-- MAGIC
-- MAGIC
-- MAGIC  %md
-- MAGIC ### Create Races table
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING)
USING csv
OPTIONS (path "abfss://raw@databrickscoursetj.dfs.core.windows.net/races.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

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

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
USING json
OPTIONS(path "abfss://raw@databrickscoursetj.dfs.core.windows.net/constructors.json")





-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create drivers table
-- MAGIC * Single Line JSON
-- MAGIC * Complex structure
-- MAGIC
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING)
USING json
OPTIONS (path "abfss://raw@databrickscoursetj.dfs.core.windows.net/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md ##### Create results table
-- MAGIC * Single Line JSON
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING json
OPTIONS(path "abfss://raw@databrickscoursetj.dfs.core.windows.net/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create pit stops table
-- MAGIC * Multi Line JSON
-- MAGIC * Simple structure
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
USING json
OPTIONS(path "abfss://raw@databrickscoursetj.dfs.core.windows.net/pit_stops.json", multiLine true)

-- COMMAND ----------

SELECT * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Lap Times Table
-- MAGIC * CSV file
-- MAGIC * Multiple files
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "abfss://raw@databrickscoursetj.dfs.core.windows.net/lap_times/")

-- COMMAND ----------

SELECT * from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Qualifying Table
-- MAGIC * JSON file
-- MAGIC * MultiLine JSON
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
OPTIONS (path "abfss://raw@databrickscoursetj.dfs.core.windows.net/qualifying/", multiLine true)

-- COMMAND ----------


SELECT * FROM f1_raw.qualifying

-- COMMAND ----------


DESC EXTENDED f1_raw.qualifying;