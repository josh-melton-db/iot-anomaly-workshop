# Databricks notebook source
# MAGIC %md 
# MAGIC Exercises: for the rest of this notebook, find the ```#TODO```s and fill in the ```...``` with your answers </br></br>
# MAGIC Key highlights for this notebook:
# MAGIC - parse the data landed in your bronze delta table and stream it into a silver delta table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Parse/Transform the data from Bronze and load to Silver
# MAGIC 
# MAGIC <br/>
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/databricks-industry-solutions/iot-anomaly-detection/main/images/04_silver.jpg" width="50%">
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC This notebook will stream new events from the Bronze table, parse/transform them, and load them to a Delta table called "Silver".

# COMMAND ----------

# DBTITLE 1,Define configs that are consistent throughout the accelerator
# MAGIC %run ../util/notebook-config

# COMMAND ----------

# DBTITLE 1,Define config for this notebook 
source_table = bronze
target_table = silver
checkpoint_location_target = f"{checkpoint_path}/{target_table}"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Incrementally Read data from Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC `spark.readStream.format("delta")`

# COMMAND ----------

from pyspark.sql.functions import from_json, from_unixtime, col # NOTE: notice the spark functions we're importing - we'll use these soon!
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType

bronze_df = (
  ... # TODO 1: specify how to read from our source delta table as a stream using spark
  .table(f"{database}.{source_table}")
)

# # You can uncomment the line below to view the bronze data as it comes in - just don't forget to stop the stream when you're done!
# display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parse/Transform the Bronze data

# COMMAND ----------

# MAGIC %md
# MAGIC `from_json` </br>
# MAGIC `from_unixtime`

# COMMAND ----------

# Schema for the Payload column
json_schema = StructType([
  StructField("timestamp", IntegerType(), True),
  StructField("device_id", IntegerType(), True),
  StructField("device_model", StringType(), True),
  StructField("sensor_1", FloatType(), True),
  StructField("sensor_2", FloatType(), True),
  StructField("sensor_3", FloatType(), True),
  StructField("state", StringType(), True)
])

# Parse/Transform
transformed_df = (
  bronze_df
    .withColumn("struct_payload", ...(col("parsedValue"), schema = json_schema)) # TODO 2: Parse json and apply schema to payload
    .select("struct_payload.*", ...("struct_payload.timestamp").alias("datetime")) # TODO 3: Convert the timestamp column from timestamp to datetime type
    .drop('timestamp')
)

# # Uncomment to display the transformed data
# display(transformed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write transformed data to Silver

# COMMAND ----------

# MAGIC %md
# MAGIC `.option("checkpointLocation", checkpoint_location_target)` </br>
# MAGIC `.trigger(availableNow=True)`

# COMMAND ----------

(
  transformed_df
    .writeStream.format("delta")
    .outputMode("append")
    ...
    ...
    .table(f"{database}.{target_table}")
    .awaitTermination()
)

# COMMAND ----------

# MAGIC %md 
# MAGIC `spark.table("...").display()`

# COMMAND ----------

# Display Silver Table
spark.table(f"{database}.{target_table}")... # TODO 4: display the resulting table

# COMMAND ----------


