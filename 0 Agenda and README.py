# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Real-time monitoring and anomaly detection on streaming IoT pipelines in Manufacturing The Internet of Things (IoT) is a foundational technology that enables the delivery of Industry 4.0 objectives within manufacturing and connected products. IoT data provides critical insights into manufacturing processes and product performance, enabling companies to optimize operations, improve quality, and deliver innovative products and services to customers.
# MAGIC </br></br>
# MAGIC In this solution accelerator, we show how to build a streaming pipeline for IoT data, train a machine learning model on that data, and use that model to make predictions on new IoT data. The pattern is applicable across many use cases like
# MAGIC 
# MAGIC - predictive maintenance
# MAGIC - digital twins
# MAGIC - remote monitoring
# MAGIC - asset optimization
# MAGIC - energy consumption optimization
# MAGIC - workforce optimization and safety
# MAGIC - quality control 
# MAGIC </br></br>
# MAGIC Databricks Lakehouse overcomes the limitations of legacy platforms for dealing with IoT data. In this workshop you will:
# MAGIC - Ingest data from cloud storage 
# MAGIC - Load and transform IoT streaming data into Delta 
# MAGIC - Surface insights like anomalies on streamed signals in near real-time
# MAGIC </br></br>
# MAGIC <b>Getting started</b> </br>
# MAGIC Although specific solutions can be downloaded as .dbc archives from our websites, we recommend cloning these repositories onto your databricks environment. Not only will you get access to latest code, but you will be part of a community of experts driving industry best practices and re-usable solutions, influencing our respective industries. To start, simply clone solution accelerator repository in Databricks using Databricks Repos

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC | Time | Lesson &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 60m  | **Slides**                               | *Introductions, concepts overview* |
# MAGIC | 60m  | **Examples**    | Run through the examples of the concepts (in the 1_Practice_Exercises folder) |
# MAGIC | 40m  | **Workshop Exercises**    | Solve the exercises provided by filling in the TODOs in the 2_Workshop_Exercises folder|
# MAGIC | 20m  | **Workshop Solutions**    | Review the solutions to the exercises|

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
