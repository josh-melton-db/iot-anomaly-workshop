# Databricks notebook source
# MAGIC %md 
# MAGIC ## Demo bundle configuration
# MAGIC Please ignore / do not delete, only used to prep and bundle the demo

# COMMAND ----------

{
  "name": "delta-lake",
  "category": "data-engineering",
  "title": "Delta Lake",
  "description": "Store your table with Delta Lake & discover how Delta Lake can simplify your Data Pipelines.",
  "fullDescription": "Delta Lake is an open format storage layer that delivers reliability, security and performance on your data lake — for both streaming and batch operations. By replacing data silos with a single home for structured, semi-structured and unstructured data, Delta Lake is the foundation of a cost-effective, highly scalable lakehouse.<br /> In this demo, we'll show you how Delta Lake is working and its main capabilities: <ul><li>ACID transactions</li><li>Support for DELETE/UPDATE/MERGE</li><li>Unify batch & streaming</li><li>Time Travel</li><li>Clone zero copy</li><li>Generated partitions</li><li>CDF - Change Data Flow (DBR runtime)</li><li>Blazing-fast queries</li></ul>",
  "bundle": True,
  "tags": [{"delta": "Delta Lake"}],
  "notebooks": [
    {
      "path": "_resources/00-setup", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Load data", 
      "description": "Init load data"
    },
    {
      "path": "_resources/01-load-data", 
      "pre_run": False, 
      "publish_on_website": False, 
      "add_cluster_setup_cell": False, 
      "title":  "Load data", 
      "description": "Init load data"
    },
    {
      "path": "00-Delta-Lake-Introduction", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Introduction to Delta Lake", 
      "description": "Create your first table, DML operation, time travel, RESTORE, CLONE and more.",
      "parameters": {"raw_data_location": "/Users/quentin.ambard@databricks.com/demos/retail/delta"}
    },
    {
      "path": "01-Delta-Lake-Performance", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Delta Lake Performance & operation", 
      "description": "Faster queries with OPTIMIZE, ZORDERS and partitions."
    },
    {
      "path": "02-Delta-Lake-CDF", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Change Data Flow (CDF)", 
      "description": "Capture and propagate table changes."
    },
    {
      "path": "03-Advanced-Delta-Lake-Internal", 
      "pre_run": True, 
      "publish_on_website": True, 
      "add_cluster_setup_cell": True, 
      "title":  "Delta Lake Internals", 
      "description": "Deep dive in Delta Lake file format."
    }    
  ]
}
