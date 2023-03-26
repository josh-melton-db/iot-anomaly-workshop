# Databricks notebook source
# MAGIC %md
# MAGIC # Structured Streaming Concepts
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Describe the programming model used by Spark Structured Streaming
# MAGIC * Configure required options to perform a streaming read on a source
# MAGIC * Describe the requirements for end-to-end fault tolerance
# MAGIC * Configure required options to perform a streaming write to a sink
# MAGIC * Interact with streaming queries and stop active streams
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC The source contains smartphone accelerometer samples from devices and users with the following columns:
# MAGIC 
# MAGIC | Field          | Description |
# MAGIC | ------------- | ----------- |
# MAGIC | Arrival_Time | time data was received |
# MAGIC | Creation_Time | event time |
# MAGIC | Device | type of Model |
# MAGIC | Index | unique identifier of event |
# MAGIC | Model | i.e Nexus4  |
# MAGIC | User | unique user identifier |
# MAGIC | geolocation | city & country |
# MAGIC | gt | transportation mode |
# MAGIC | id | unused null field |
# MAGIC | x | acceleration in x-dir |
# MAGIC | y | acceleration in y-dir |
# MAGIC | z | acceleration in z-dir |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our "classroom."

# COMMAND ----------

# MAGIC %run ../util/classic-setup $mode="reset"

# COMMAND ----------

# MAGIC %md
# MAGIC ## End-to-end Fault Tolerance
# MAGIC 
# MAGIC Structured Streaming ensures end-to-end exactly-once fault-tolerance guarantees through _checkpointing_ (discussed below) and <a href="https://en.wikipedia.org/wiki/Write-ahead_logging" target="_blank">Write Ahead Logs</a>.
# MAGIC 
# MAGIC Structured Streaming sources, sinks, and the underlying execution engine work together to track the progress of stream processing. If a failure occurs, the streaming engine attempts to restart and/or reprocess the data.
# MAGIC For best practices on recovering from a failed streaming query see <a href="">docs</a>.
# MAGIC 
# MAGIC This approach _only_ works if the streaming source is replayable; replayable sources include cloud-based object storage and pub/sub messaging services.
# MAGIC 
# MAGIC At a high level, the underlying streaming mechanism relies on a couple approaches:
# MAGIC 
# MAGIC * First, Structured Streaming uses checkpointing and write-ahead logs to record the offset range of data being processed during each trigger interval.
# MAGIC * Next, the streaming sinks are designed to be _idempotent_—that is, multiple writes of the same data (as identified by the offset) do _not_ result in duplicates being written to the sink.
# MAGIC 
# MAGIC Taken together, replayable data sources and idempotent sinks allow Structured Streaming to ensure **end-to-end, exactly-once semantics** under any failure condition.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Reading a Stream
# MAGIC 
# MAGIC The `readStream` method returns a `DataStreamReader` used to configure the stream.
# MAGIC 
# MAGIC Configuring a streaming read on a source requires:
# MAGIC * The schema of the data
# MAGIC * The `format` of the source [(file format or named connector)](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/data-sources)
# MAGIC * Configurations specific to the source:
# MAGIC   * [Kafka](https://docs.databricks.com/spark/latest/structured-streaming/kafka.html)
# MAGIC   * [Event Hubs](https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### The Schema
# MAGIC 
# MAGIC Every streaming DataFrame must have a schema. When connecting to pub/sub systems like Kafka and Event Hubs, the schema will be automatically provided by the source.
# MAGIC 
# MAGIC For other streaming sources, the schema must be user-defined. It is not safe to infer schema from files, as the assumption is that the source is growing indefinitely from zero records.

# COMMAND ----------

schema = "Arrival_Time BIGINT, Creation_Time BIGINT, Device STRING, Index BIGINT, Model STRING, User STRING, geolocation STRUCT<city: STRING, country: STRING>, gt STRING, id BIGINT, x DOUBLE, y DOUBLE, z DOUBLE"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Differences between Static and Streaming Reads
# MAGIC 
# MAGIC In the cell below, a static and streaming read are each defined against the same source (files in a directory on a cloud object store). Note that the syntax is identical except that the streaming query uses `readStream` instead of `read`.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> While `maxFilesPerTrigger` will be used throughout the material in this course to limit how quickly source files are consumed, this is optional and for demonstration purposes. This option allows control over how much data will be processed in each micro-batch.

# COMMAND ----------

dataPath = "/mnt/training/definitive-guide/data/activity-json/streaming"

staticDF = (spark
  .read
  .format("json")
  .schema(schema)
  .load(dataPath)
)

streamingDF = (spark
  .readStream
  .format("json")
  .schema(schema)
  .option("maxFilesPerTrigger", 1)     # Optional; force processing of only 1 file per trigger 
  .load(dataPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Just like with static DataFrames, data is not processed and jobs are not triggered until an action is called.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Writing a Stream
# MAGIC 
# MAGIC The method `DataFrame.writeStream` returns a `DataStreamWriter` used to configure the output of the stream.
# MAGIC 
# MAGIC There are a number of required parameters to configure a streaming write:
# MAGIC * The `format` of the **output sink** (see [documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks))
# MAGIC * The location of the **checkpoint directory**
# MAGIC * The **output mode**
# MAGIC * Configurations specific to the output sink, such as:
# MAGIC   * [Kafka](https://docs.databricks.com/spark/latest/structured-streaming/kafka.html)
# MAGIC   * [Event Hubs](https://docs.databricks.com/spark/latest/structured-streaming/streaming-event-hubs.html)
# MAGIC   * A <a href="https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=foreach#pyspark.sql.streaming.DataStreamWriter.foreach"target="_blank">custom sink</a> via `writeStream.foreach(...)`
# MAGIC 
# MAGIC Once the configuration is completed, trigger the job with a call to `.start()`. When writing to files, use `.start(filePath)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpointing
# MAGIC 
# MAGIC Databricks creates checkpoints by storing the current state of your streaming job to Azure Blob Storage or ADLS.
# MAGIC 
# MAGIC Checkpointing combines with write ahead logs to allow a terminated stream to be restarted and continue from where it left off.
# MAGIC 
# MAGIC Checkpoints cannot be shared between separate streams.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Output Modes
# MAGIC 
# MAGIC Streaming jobs have output modes similar to static/batch workloads. [More details here](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes).
# MAGIC 
# MAGIC | Mode   | Example | Notes |
# MAGIC | ------------- | ----------- |
# MAGIC | **Append** | `.outputMode("append")`     | _DEFAULT_ - Only the new rows appended to the Result Table since the last trigger are written to the sink. |
# MAGIC | **Complete** | `.outputMode("complete")` | The entire updated Result Table is written to the sink. The individual sink implementation decides how to handle writing the entire table. |
# MAGIC | **Update** | `.outputMode("update")`     | Only the rows in the Result Table that were updated since the last trigger will be outputted to the sink. Since Spark 2.1.1 |
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Not all sinks will support `update` mode.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Differences between Static and Streaming Writes
# MAGIC 
# MAGIC The following cell demonstrates batch logic to append data from a static read.

# COMMAND ----------

outputPath = userhome + "/static-write"

dbutils.fs.rm(outputPath, True)    # clear this directory in case lesson has been run previously

(staticDF                                
  .write                                               
  .format("delta")                                          
  .mode("append")                                       
  .save(outputPath))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Note that there are only minor syntax differences when writing a stream instead:
# MAGIC - `writeStream` instead of `write`
# MAGIC - The path for the checkpoint is provided to the option `checkpointLocation`
# MAGIC - `outputMode` instead of `mode` (note that streaming uses `complete` instead of `overwrite` for similar functionality here)
# MAGIC - `start` instead of `save`
# MAGIC 
# MAGIC The following cell demonstrates a streaming write to Delta files.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Assigning a variable name when writing to a sink provides programmatic access to a `StreamingQuery` object. This will be discussed below.

# COMMAND ----------

outputPath = userhome + "/streaming-concepts"
checkpointPath = outputPath + "/checkpoint"

dbutils.fs.rm(outputPath, True)    # clear this directory in case lesson has been run previously

streamingQuery = (streamingDF                                
  .writeStream                                                
  .format("delta")                                          
  .option("checkpointLocation", checkpointPath)               
  .outputMode("append")
#   .queryName("my_stream")        # optional argument to register stream to Spark catalog
  .start(outputPath)                                       
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming with Delta Lake
# MAGIC 
# MAGIC In the logic defined above, data is read from JSON files and then saved out in the Delta Lake format. Note that because Delta creates a new version for each transaction, when working with streaming data this will mean that the Delta table creates a new version for each trigger interval in which new data is processed. [More info on streaming with Delta](https://docs.databricks.com/delta/delta-streaming.html#table-streaming-reads-and-writes).

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Defining the Trigger Interval
# MAGIC 
# MAGIC When defining a streaming write, the `trigger` method specifies when the system should process the next set of data. The example above uses the default, which is the same as `.trigger(Trigger.ProcessingTime("500 ms"))`.
# MAGIC 
# MAGIC | Trigger Type                           | Example | Notes |
# MAGIC |----------------------------------------|-----------|-------------|
# MAGIC | Unspecified                            |  | _DEFAULT_ - The query will be executed as soon as the system has completed processing the previous query |
# MAGIC | Fixed interval micro-batches           | `.trigger(Trigger.ProcessingTime("2 minutes"))` | The query will be executed in micro-batches and kicked off at the user-specified intervals |
# MAGIC | One-time micro-batch                   | `.trigger(Trigger.Once())` | The query will execute _only one_ micro-batch to process all the available data and then stop on its own |
# MAGIC | Continuous w/fixed checkpoint interval | `.trigger(Trigger.Continuous("1 second"))` | The query will be executed in a low-latency, <a href="http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#continuous-processing" target = "_blank">continuous processing mode</a>. _EXPERIMENTAL_ in 2.3.2 |
# MAGIC 
# MAGIC Note that triggers are specified when defining how data will be written to a sink and control the frequency of micro-batches. By default, Spark will automatically detect and process all data in the source that has been added since the last trigger; some sources allow configuration to limit the size of each micro-batch.
# MAGIC 
# MAGIC BEST_PRACTICE: Read [this blog post](https://databricks.com/blog/2017/05/22/running-streaming-jobs-day-10x-cost-savings.html) to learn more about using `Trigger.Once` to simplify CDC with a hybrid streaming/batch design.

# COMMAND ----------

# MAGIC %md
# MAGIC The code below stops the `streamingQuery` defined above and introduces `awaitTermination()`
# MAGIC 
# MAGIC `awaitTermination()` will block the current thread
# MAGIC * Until the stream stops or
# MAGIC * Until the specified timeout elapses

# COMMAND ----------

streamingQuery.awaitTermination(5)      # Stream for another 5 seconds while the current thread blocks
streamingQuery.stop()                   # Stop the stream

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## The Display function
# MAGIC 
# MAGIC Within the Databricks notebooks, we can use the `display()` function to render a live plot. This stream is written to memory; **generally speaking this is most useful for debugging purposes**.
# MAGIC 
# MAGIC When you pass a "streaming" `DataFrame` to `display()`:
# MAGIC * A "memory" sink is being used
# MAGIC * The output mode is complete
# MAGIC * *OPTIONAL* - The query name is specified with the `streamName` parameter
# MAGIC * *OPTIONAL* - The trigger is specified with the `trigger` parameter
# MAGIC * *OPTIONAL* - The checkpointing location is specified with the `checkpointLocation`
# MAGIC 
# MAGIC `display(myDF, streamName = "myQuery")`
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The previous cell programmatically stopped only active streaming query. In the cell below, `display` will start a new streaming query against the source defined in `streamingDF`.  We are passing `streaming_display` as the name for this newly started stream.

# COMMAND ----------

display(streamingDF, streamName = "streaming_display")

# COMMAND ----------

# MAGIC %md
# MAGIC Since the `streamName` gets registered as a temporary table pointing to the memory sink, we can use SQL to query the sink.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM streaming_display WHERE gt = "stand"

# COMMAND ----------

# MAGIC %md
# MAGIC Stop all remaining streams.

# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Summary</h2>
# MAGIC 
# MAGIC We use `readStream` to read streaming input from a variety of input sources and create a DataFrame.
# MAGIC 
# MAGIC Nothing happens until we invoke `writeStream` or `display`.
# MAGIC 
# MAGIC Using `writeStream` we can write to a variety of output sinks. Using `display` we draw LIVE bar graphs, charts and other plot types in the notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Additional Topics &amp; Resources</h2>
# MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#" target="_blank">Structured Streaming Programming Guide</a></br>
# MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a> by Tathagata Das. This is an excellent video describing how Structured Streaming works.</br>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
