# Databricks notebook source
# MAGIC %md 
# MAGIC # Read CSV file

# COMMAND ----------

data = spark.read.csv("file:/Workspace/Users/tjavedan@hotmail.com/practice/employees.csv", 
                     header=True, inferSchema=True)



# COMMAND ----------

display(data)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Incremental Load

# COMMAND ----------


from pyspark.sql.functions import max

latest_date = data.select(max("last_updated")).first()[0]
print(latest_date)

# COMMAND ----------

new_records = data.filter(data["last_updated"] > latest_date)
display(new_records)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Append new records to existing data

# COMMAND ----------


data = data.union(new_records).distinct()


# COMMAND ----------

data = data.orderBy("id", ascending=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Transformations

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 1.Count of Employees per Department

# COMMAND ----------

max_employees = data.groupBy("department").count() \
    .withColumnRenamed("count", "Number of employees").orderBy("Number of employees", ascending=False)
display(max_employees)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2.Max Salary By Department

# COMMAND ----------

max_salary_per_department = data.groupBy("department") \
    .agg(max("salary").alias("max_salary")).orderBy("max_salary", ascending=False)

display(max_salary_per_department)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 3.Nnth Higest Salary
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import Window 
from pyspark.sql.functions import col, dense_rank


# COMMAND ----------

Window_salary = Window.partitionBy("department").orderBy(col("salary").desc())

max_salary_per_department = data.withColumn("RANK", dense_rank().over(Window_salary))

max_salary_per_department.select("name","department","salary","RANK")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Filtering Nth Salary 

# COMMAND ----------


max_salary_per_department.filter(col("RANK") == 2).display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Writing the data to a CSV file

# COMMAND ----------

write_data = data.coalesce(1).write.mode("overwrite").option("header", "true").csv("file:/Workspace/Users/tjavedan@hotmail.com/practice/employees_new.csv")
