# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # IoT Data Generation
# MAGIC 
# MAGIC <img src="https://github.com/databricks-industry-solutions/iot-anomaly-detection/raw/main/resource/images/02_generate_iot_data.jpg">
# MAGIC 
# MAGIC In this notebook, we use `dbldatagen` to generate fictitious data and push into a Kafka topic.
# MAGIC 
# MAGIC We first generate data using [Databricks Labs Data Generator](https://databrickslabs.github.io/dbldatagen/public_docs/index.html) (`dbldatagen`). The data generator provides an easy way to generate large volumes of synthetic data within a Databricks notebook. The data that is generated is defined by a schema. The output is a PySpark dataframe.
# MAGIC 
# MAGIC The generated data consists of the following columns: 
# MAGIC - `device_id`
# MAGIC - `device_model`
# MAGIC - `timestamp`
# MAGIC - `sensor_1`
# MAGIC - `sensor_2`
# MAGIC - `sensor_3`
# MAGIC - `us_state`
# MAGIC 
# MAGIC where `sensor 1..3` are sensor values. 

# COMMAND ----------

# MAGIC %md
# MAGIC Generate the Data

# COMMAND ----------

# MAGIC %run ./notebook-config $reset_all_data=false

# COMMAND ----------

import dbldatagen as dg
import dbldatagen.distributions as dist
from pyspark.sql.types import IntegerType, FloatType, StringType, LongType
from pyspark.sql.functions import to_json, struct

states = [ 'AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
           'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
           'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM',
           'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX',
           'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY' ]

data_rows = 2000
df_spec = (
  dg.DataGenerator(
    spark,
    name="test_data_set1",
    rows=data_rows,
    partitions=4
  )
  .withIdOutput()
  .withColumn("device_id", IntegerType(), minValue=1, maxValue=1000)
  .withColumn(
    "device_model",
    StringType(),
    values=['mx2000', 'xft-255', 'db-1000', 'db-2000', 'mlr-120'],
    random=True
  )
  .withColumn("timestamp", LongType(), minValue=1577833200, maxValue=1673714337, random=True)
  .withColumn("sensor_1", IntegerType(), minValue=-10, maxValue=100, random=True, distribution=dist.Gamma(40.0,9.0))
  .withColumn("sensor_2", IntegerType(), minValue=0, maxValue=10, random=True)
  .withColumn("sensor_3", FloatType(), minValue=0.0001, maxValue=1.0001, random=True)
  .withColumn("state", StringType(), values=states, random=True)
)        
df = df_spec.build()

# COMMAND ----------

# queryStr = 'create_map(' + ', '.join(["lit('"+c+"'), col('"+c+"')" for c in df.columns]) + ')'
# print(queryStr)

# COMMAND ----------

df = df.withColumn('parsedValue', to_json(struct('*'))).select('parsedValue')

# COMMAND ----------

# MAGIC %md
# MAGIC Write to Raw, remove spark's extra folders

# COMMAND ----------

df.write.mode('append').format('json').save(raw_path)
for f in dbutils.fs.ls(raw_path):
  if f.path.split('/')[-1][0] == "_":
    dbutils.fs.rm(f.path, True)

# COMMAND ----------


