# Databricks notebook source
# MAGIC %md 
# MAGIC Exercises: for the rest of this notebook, find the ```#TODO```s and fill in the ```...``` with your answers </br></br>
# MAGIC Key highlights for this notebook:
# MAGIC - identify the optimal hyperparameters for our demand forecasting model on a single SKU using pandas and Hyperopt

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

# MAGIC %md
# MAGIC Example of retrieving the current production stage model, registering it as a spark function, and using it to make a prediction:
# MAGIC ```
# MAGIC model_version_uri = f"models:/{model_name}/production"
# MAGIC predict = mlflow.pyfunc.spark_udf(spark, model_version_uri)
# MAGIC predictions_df = input_df.withColumn("prediction", predict(input_df.columns))
# MAGIC ```

# COMMAND ----------

import mlflow

# Build a function that loads our model into a function and uses that function to make predictions
def predict_anomalies(data, epoch_id):
  # Load the model
  model_uri = f'...:/{model_name}/...' # TODO 1: define which stage of your model you'll want to use for predictions
  ...(spark, model_uri=model_uri) # TODO 2: create a function that you can use to make predictions 

  # Make the prediction
  prediction_df = data.withColumn('prediction', ...(*data.drop('datetime', 'device_id').columns)) # TODO 3: Use the function you created above to make the predictions on the input data
  
  # Clean up the output
  clean_pred_df = (prediction_df.select('device_id', 'datetime', 'sensor_1', 'sensor_2', 'sensor_3', 'prediction'))
  
  # Write the output to a Gold Delta table
  clean_pred_df.write.format('delta').mode('append').option("mergeSchema", "true").saveAsTable(f"{database}.{target_table}")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Stream the predicted results using the function

# COMMAND ----------

# Make predictions on the streaming data coming in
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

# COMMAND ----------


