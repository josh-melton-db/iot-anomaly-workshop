# Databricks notebook source
# MAGIC %md You may find this series of notebooks at https://github.com/databricks-industry-solutions/iot-anomaly-detection. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Predict Anomalous Events
# MAGIC 
# MAGIC <br/>
# MAGIC 
# MAGIC <img src="https://github.com/databricks-industry-solutions/iot-anomaly-detection/blob/main/images/06_inference.jpg?raw=true" width="25%">
# MAGIC 
# MAGIC This notebook will use the trained model to identify anomalous events.

# COMMAND ----------

# DBTITLE 1,Define configs that are consistent throughout the accelerator
# MAGIC %run ../util/notebook-config

# COMMAND ----------

# DBTITLE 1,Define config for this notebook 
source_table = feature
target_table = gold
checkpoint_location_target = f"{checkpoint_path}/{target_table}"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Read Silver Feature Data

# COMMAND ----------

# Read Silver Data
silver_df = (
  spark.readStream
    .format("delta")
    .table(f"{database}.{source_table}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create a function to featurize and make the prediction

# COMMAND ----------

import mlflow

# Build a function that loads our model into a function and uses that function to make predictions
def predict_anomalies(data, epoch_id):
  # Load the model
  model = f'models:/{model_name}/production'
  predict = mlflow.pyfunc.spark_udf(spark, model_uri=model)

  # Make the prediction
  prediction_df = data.withColumn('prediction', predict(*data.drop('datetime', 'device_id').columns))
  
  # Clean up the output
  clean_pred_df = (prediction_df.select('device_id', 'datetime', 'sensor_1', 'sensor_2', 'sensor_3', 'prediction'))
  
  # Write the output to a Gold Delta table
  clean_pred_df.write.format('delta').mode('append').option("mergeSchema", "true").saveAsTable(f"{database}.{target_table}")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Stream the predicted results using the function

# COMMAND ----------

# Stream predicted outputs
(
  silver_df
    .writeStream
    .foreachBatch(predict_anomalies)
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------

# DBTITLE 1,Display our results
display(spark.table(f"{database}.{target_table}"))
