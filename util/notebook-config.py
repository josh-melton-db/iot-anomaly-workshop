# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"])

# COMMAND ----------

# DBTITLE 1,Set database and streaming checkpoint
checkpoint_path = "/dbfs/tmp/josh_melton/iot-anomaly-detection/checkpoints" 
raw_path = "/dbfs/tmp/josh_melton/iot-anomaly-detection/raw"
database = "iot_anomaly_jlm"
bronze = "bronze_iot_anomaly"
silver = "silver_iot_anomaly"
feature = "feature_iot_anomaly"
gold = "gold_iot_anomaly"

# COMMAND ----------

if dbutils.widgets.get("reset_all_data") == "true":
  dbutils.fs.rm(checkpoint_path, True) 
  dbutils.fs.rm(raw_path, True) 
  spark.sql(f"drop database if exists {database} cascade") 

# COMMAND ----------

# DBTITLE 1,Database settings
spark.sql(f"create database if not exists {database}")
spark.sql(f"""create table if not exists {database}.{bronze} (
  parsedValue STRING
  )
""")
spark.sql(f"""create table if not exists {database}.{silver} (
  device_id INT,
  device_model STRING,
  datetime STRING,
  sensor_1 FLOAT, 
  sensor_2 FLOAT,
  sensor_3 FLOAT,
  state STRING
  )
""")
spark.sql(f"""create table if not exists {database}.{feature} (
  device_id INT,
  device_model STRING,
  datetime STRING,
  sensor_1 FLOAT, 
  sensor_2 FLOAT,
  sensor_3 FLOAT,
  state STRING,
  anamoly INT
  )
""")

# COMMAND ----------

# DBTITLE 1,mlflow settings
import mlflow
model_name = "iot_anomaly_detection"
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment('/Users/{}/iot_anomaly_detection'.format(username))

# COMMAND ----------


