# Databricks notebook source
# MAGIC %md 
# MAGIC Exercises: for the rest of this notebook, find the ```#TODO```s and fill in the ```...``` with your answers </br></br>
# MAGIC Key highlights for this notebook:
# MAGIC - stream in JSON data and write to a delta table

# COMMAND ----------

# MAGIC %md 
# MAGIC %md 
# MAGIC # Real-time Monitoring and Anomaly Detection on Streaming IoT pipelines in Manufacturing 
# MAGIC 
# MAGIC Manufacturers today face challenges working with IoT data due to high investment costs, security, and connectivity outages. These challenges lead to more time and money being spent on trying to make things work rather than innovating on data products that drive business value.  The Databricks Lakehouse platform reduces these challenges with a reliable, secure platform capable of ingesting and transforming IoT data at massive scale, building analytics and AI assets on that data, and serving those assets where they are needed
# MAGIC 
# MAGIC In this solution accelerator, we show how to build a streaming pipeline for IoT data, train a machine learning model on that data, and use that model to make predictions on new IoT data.
# MAGIC 
# MAGIC The pattern shown consumes data from an Apache Kafka stream, although in this demo we simulate that by dropping json files into cloud storage and streaming them using <a href="https://docs.databricks.com/ingestion/auto-loader/index.html">autoloader</a>. Kafka is a distributed event streaming message bus that combines the best features of queuing and publish-subscribe technologies. [Kafka connectors for Spark\\({^T}{^M}\\) Structured Streaming](https://docs.databricks.com/structured-streaming/kafka.html) are packaged together within the Databricks runtime, making it easy to get started. Using these connectors, data from Kafka streams can easily be persisted into Delta Lakehouse. From there, advanced analytics or machine learning algorithms may be executed on the data.
# MAGIC 
# MAGIC <p></p>
# MAGIC 
# MAGIC <img src="https://github.com/databricks-industry-solutions/iot-anomaly-detection/blob/main/images/iot_streaming_lakehouse.png?raw=true" width=75%/>
# MAGIC 
# MAGIC <p></p>
# MAGIC 
# MAGIC ## Stream the Data from Kafka into a Bronze Delta Table
# MAGIC 
# MAGIC <p></p>
# MAGIC <center><img src="https://github.com/databricks-industry-solutions/iot-anomaly-detection/blob/main/images/03_bronze.jpg?raw=true" width="30%"></center>

# COMMAND ----------

# DBTITLE 1,Define configs that are consistent throughout the accelerator
# MAGIC %run ../util/notebook-config

# COMMAND ----------

# DBTITLE 1,Define config for this notebook 
# Define target table for this notebook
target_table = bronze
checkpoint_location_target = f"{checkpoint_path}/{target_table}"

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Write the streaming source data to Bronze Delta table
# MAGIC 
# MAGIC After generating artificial json data and writing it to cloud storage, we stream in the newly landed files and write them to delta lake, the efficient open source storage format built on parquet. For files landing in cloud storage we use <a href="https://docs.databricks.com/ingestion/auto-loader/index.html">autoloader</a> as a source that ingests the new files in a highly scalable, fault tolerant way. Autoloader also offers automatic schema inference, but in this instance we'll define the schema explicitly

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
expected_schema = StructType([StructField('parsedValue', StringType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC Example of reading as a stream, writing the results to delta, triggered as a "scheduled stream":
# MAGIC ```
# MAGIC (
# MAGIC   spark.readStream.format(...)
# MAGIC   ...
# MAGIC   .writeStream.format("delta")
# MAGIC   .trigger(availableNow=True)
# MAGIC )
# MAGIC ```

# COMMAND ----------

read_raw = (
  ...  # TODO 1: specify how to read as a stream using spark
  .format('cloudFiles')
  .option("cloudFiles.format", "json")
  .schema(expected_schema)
  .load(raw_path)
  ... # TODO 2: specify how to write a stream to a delta table using spark
  .option("checkpointLocation", checkpoint_path)
  ... # TODO 3: specify how the stream should be triggered to run
  .toTable(f"{database}.{target_table}")
  .awaitTermination()
)

# COMMAND ----------

# MAGIC %md 
# MAGIC Example of reading from a table and displaying:
# MAGIC ```
# MAGIC spark.table("...").display()
# MAGIC ```

# COMMAND ----------

# Display records from the Bronze table
...(f"{database}.{target_table}")... # TODO 4: display the results of reading the table

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
