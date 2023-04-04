# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"])

# COMMAND ----------

# DBTITLE 1,mlflow settings
import mlflow
model_name = "iot_anomaly_detection"
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment('/Users/{}/iot_anomaly_detection'.format(username))

# COMMAND ----------

user = username.split('@')[0]
checkpoint_path = f"/dbfs/tmp/{user}/iot-anomaly-detection/checkpoints" 
raw_path = f"/dbfs/tmp/josh_melton/iot-anomaly-detection/raw"
database = f"iot_anomaly"
bronze = f"bronze_iot_anomaly_{user}"
silver = f"silver_iot_anomaly_{user}"
feature = f"feature_iot_anomaly_{user}"
gold = f"gold_iot_anomaly_{user}"

# COMMAND ----------

if dbutils.widgets.get("reset_all_data") == "true":
  dbutils.fs.rm(checkpoint_path, True) 
  dbutils.fs.rm(raw_path, True) 
  spark.sql(f"drop database if exists {database} cascade") 

# COMMAND ----------

spark.sql(f"create database if not exists {database}")

# COMMAND ----------


