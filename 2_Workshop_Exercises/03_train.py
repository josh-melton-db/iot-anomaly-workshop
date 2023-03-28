# Databricks notebook source
# MAGIC %md 
# MAGIC Exercises: for the rest of this notebook, find the ```#TODO```s and fill in the ```...``` with your answers </br></br>
# MAGIC Key highlights for this notebook:
# MAGIC - create test/train datasets
# MAGIC - build, register, and promote your model to production

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Build Test/Train Datasets and Train Model
# MAGIC 
# MAGIC <br/>
# MAGIC 
# MAGIC <img src="https://github.com/databricks-industry-solutions/iot-anomaly-detection/blob/main/images/05_train_model.jpg?raw=true" width="50%">
# MAGIC 
# MAGIC This notebook will label the Silver data, create training and test datasets from the labeled data, train a machine learning model, and deploy the model the MLflow model registry.

# COMMAND ----------

# DBTITLE 1,Define configs that are consistent throughout the accelerator
# MAGIC %run ../util/notebook-config

# COMMAND ----------

# DBTITLE 1,Define config for this notebook 
source_table = silver
target_table = feature
checkpoint_location_target = f"{checkpoint_path}/dataset"

# COMMAND ----------

# DBTITLE 1,Read and label the Silver data
from pyspark.sql import functions as F

# Read the Silver Data
silver_df = spark.table(f"{database}.{source_table}")

# # Uncomment to display silver_df
# display(silver_df)

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import *

# Label the Silver data with anomalies that we'll build a model to predict
labeled_df = (
  silver_df
    .withColumn("anomaly", when(col('sensor_1') > 80, 1).when(col('sensor_1') < 10, 1).when(col('sensor_1') > 65, round(rand(1))).when(col('sensor_1') < 25, round(rand(1))).otherwise(0))
)

# Display the labeled data
display(labeled_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Example of batch writing the results of a dataframe to a delta table in append mode: </br>
# MAGIC `features_df.write.saveAsTable("target_table", mode="append")`

# COMMAND ----------

# DBTITLE 1,Save Feature to Delta Table
features_df = labeled_df # here you could do more featurization based on domain knowledge
...option("mergeSchema", "true").saveAsTable(f"{database}.{target_table}", mode = "...") # TODO 1: write features_df to the target table in overwrite model

# COMMAND ----------

# DBTITLE 1,Create Training and Test Datasets
import pandas as pd
import numpy as np
import mlflow
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import *
mlflow.spark.autolog()
mlflow.sklearn.autolog()

# Read data
data = features_df.toPandas().drop(["device_id", "datetime"], axis=1)

train, test = train_test_split(data, test_size=0.30, random_state=206)
colLabel = 'anomaly'

# The predicted column is colLabel which is a scalar from [3, 9]
train_x = train.drop([colLabel], axis=1)
test_x = test.drop([colLabel], axis=1)
train_y = train[colLabel]
test_y = test[colLabel]

# COMMAND ----------

# DBTITLE 1,Design our ML pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline

def make_pipeline(max_depth, max_leaf_nodes):
  enc = OneHotEncoder(handle_unknown='ignore')
  model = DecisionTreeClassifier(max_depth=max_depth, max_leaf_nodes=max_leaf_nodes)
  pipeline = Pipeline(
    steps=[("preprocessor", enc), ("classifier", model)]
  )
  
  return pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC Example of starting an mlflow run in order to track the results in an mlflow experiment:
# MAGIC ```
# MAGIC mlflow.start_run(run_name="my_sklearn_model") as run:
# MAGIC   run_id = run.info.run_uuid
# MAGIC   model.fit(train_x, train_y)
# MAGIC   predictions = model.predict(text_x)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Initial Training Run
from sklearn.metrics import *
mlflow.spark.autolog()
mlflow.sklearn.autolog()

# Begin training run
max_depth = 1
max_leaf_nodes = 92

with ... as run: # TODO 2: start an MLflow run so that our trial gets tracked in an experiment
    run_id = run.info.run_uuid
    pipeline = make_pipeline(max_depth, max_leaf_nodes)
    pipeline.fit(train_x, train_y)
    predictions = pipeline.predict(test_x)
    
# You can look at the experiment logging including parameters, metrics, recall curves, etc. by clicking the "experiment" link below or the MLflow Experiments icon in the right navigation pane

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can look at the experiment logging including parameters, metrics, recall curves, etc. by clicking the "experiment" link above or the MLflow Experiments icon in the right navigation pane. You can try running with different parameters and use the experiment to track what produces the best results. Once we're done with experimentation we'll register the model to our MLflow model registry

# COMMAND ----------

# MAGIC %md
# MAGIC Example of registering a model:
# MAGIC ```
# MAGIC model_uri = f"runs:/{run_id}/{model_name}"
# MAGIC model_details = mlflow.register_model(model_uri=model_uri, name=f'{model_name}')
# MAGIC ```

# COMMAND ----------

import mlflow
from mlflow.tracking.client import MlflowClient

client = MlflowClient()
model_uri = f"runs:/{run_id}/model"

# Register the model
model_details = ...(..., model_name) # TODO 3: register the model to mlflow's model registry

# COMMAND ----------

# MAGIC %md
# MAGIC Example of transitioning a registered model to production
# MAGIC ```
# MAGIC from mlflow.tracking.client import MlflowClient
# MAGIC 
# MAGIC client = MlflowClient()
# MAGIC 
# MAGIC client.transition_model_version_stage(
# MAGIC   name=model_details.name,
# MAGIC   version=model_details.version,
# MAGIC   stage="Production"
# MAGIC )
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Transition the model to "Production" stage in the registry
...(  # TODO 4: use the mlflow client to transition the model version to the "Production" stage
  name = model_name,
  version = model_details.version,
  stage="...",   # TODO 5: promote to Production stage
  archive_existing_versions=True
)

# COMMAND ----------


