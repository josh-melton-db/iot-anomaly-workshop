# Databricks notebook source
# MAGIC %md 
# MAGIC ## Introduction to Apache Spark on Databricks
# MAGIC 
# MAGIC Cells can be executed by hitting `Shift+Enter` while the cell is selected.

# COMMAND ----------

# MAGIC %md Spark allows two distinct kinds of operations by the user. There are **transformations** and there are **actions**.
# MAGIC 
# MAGIC ### Transformations
# MAGIC 
# MAGIC Transformations are operations that will not be completed at the time you write and execute the code in a cell - they will only get executed once you have called a **action**. An example of a transformation might be to convert an integer into a float or to filter a set of values.
# MAGIC 
# MAGIC ### Actions
# MAGIC 
# MAGIC Actions are commands that are computed by Spark right at the time of their execution. They consist of running all of the previous transformations in order to get back an actual result. An action is composed of one or more jobs which consists of tasks that will be executed by the workers in parallel where possible

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Spark consists of actions and transformations - let's talk about why that's the case. The reason for this is that it gives a simple way to optimize the entire pipeline of computations as opposed to the individual pieces. This makes it exceptionally fast for certain types of computation because it can perform all relevant computations at once. Technically speaking, Spark `pipelines` this computation which we can see in the image below. This means that certain computations can all be performed at once (like a map and a filter) rather than having to do one operation for all pieces of data then the following operation.
# MAGIC 
# MAGIC Apache Spark can also keep results in memory as opposed to other frameworks that immediately write to disk after each task.
# MAGIC 
# MAGIC ## Apache Spark Architecture
# MAGIC 
# MAGIC Before proceeding with our example, let's see an overview of the Apache Spark architecture. As mentioned before, Apache Spark allows you to treat many machines as one machine and this is done via a master-worker type architecture where there is a `driver` or master node in the cluster, accompanied by `worker` nodes. The master sends work to the workers and either instructs them to pull to data from memory or from disk (or from another data source like S3 or Redshift).
# MAGIC 
# MAGIC 
# MAGIC You can view the details of your Apache Spark application in the Apache Spark web UI.  The web UI is accessible in Databricks by going to "Clusters" and then clicking on the "View Spark UI" link for your cluster, it is also available by clicking at the top left of this notebook where you would select the cluster to attach this notebook to. In this option will be a link to the Apache Spark Web UI.
# MAGIC 
# MAGIC At a high level, every Apache Spark application consists of a driver program that launches various parallel operations on executor Java Virtual Machines (JVMs) running either in a cluster or locally on the same machine. In Databricks, the notebook interface is the driver program.  This driver program contains the main loop for the program and creates distributed datasets on the cluster, then applies operations (transformations & actions) to those datasets.
# MAGIC Driver programs access Apache Spark through a `SparkSession` object regardless of deployment location.
# MAGIC 
# MAGIC ## A Worked Example of Transformations and Actions
# MAGIC 
# MAGIC To illustrate all of these architectural and most relevantly **transformations** and **actions** - let's go through a more thorough example, this time using `DataFrames` and a csv file. 
# MAGIC 
# MAGIC The DataFrame and SparkSQL work almost exactly as we have described above, we're going to build up a plan for how we're going to access the data and then finally execute that plan with an action. We go through a process of analyzing the query, building up a plan, comparing them and then finally executing it.
# MAGIC 
# MAGIC While we won't go too deep into the details for how this process works, you can read a lot more about this process on the [Databricks blog](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html). For those that want a more information about how Apache Spark goes through this process, I would definitely recommend that post!
# MAGIC 
# MAGIC Going forward, we're going to access a set of public datasets that Databricks makes available. Databricks datasets are a small curated group that we've pulled together from across the web. We make these available using the [Databricks File System](https://docs.databricks.com/data/databricks-file-system.html). Let's load the popular diamonds dataset in as a spark  `DataFrame`. Now let's go through the dataset that we'll be working with.

# COMMAND ----------

dataPath = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"

diamonds = spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load(dataPath)
  
# this can be done by wrapping in parenthesis instead of using backslashes on each line to attain the same result
diamonds = (
  spark.read.format("csv")   
  .option("header","true")
  .option("inferSchema", "true")
  .load(dataPath)
)

# COMMAND ----------

# MAGIC %md Now that we've loaded in the data, we're going to perform computations on it. This provides us a convenient tour of some of the basic functionality and some of the nice features that makes running Spark on Databricks the simplest! In order to be able to perform our computations, we need to understand more about the data. We can do this with the `display` function.

# COMMAND ----------

diamonds.display()   # can also use display(diamonds)

# COMMAND ----------

# MAGIC %md what makes `display` exceptional is the fact that we can very easily create some more sophisticated graphs by clicking the graphing icon that you can see below. Here's a plot that allows us to compare price, color, and cut.

# COMMAND ----------

display(diamonds)

# COMMAND ----------

# MAGIC %md Now that we've explored the data, let's return to understanding **transformations** and **actions**. I'm going to create several transformations and then an action. After that we will inspect exactly what's happening under the hood.
# MAGIC 
# MAGIC These transformations are simple, first we group by two variables, cut and color and then compute the average price. Then we're going to inner join that to the original dataset on the column `color`. Then we'll select the average price as well as the carat from that new dataset. Finally, we show how to filter rows from our result; select() specifies columns, where() or filter() remove rows
# MAGIC 
# MAGIC With df1, we answer the question "what is the average price for each combination of cut and color?" <br>
# MAGIC With df2, we join in the rest of the dataset to ensure we have the carat columns and select only the desired columns <br>
# MAGIC With fancy_diamonds_df, we answer the question "what are the average prices for each combination of cut and color, where carat is greater than 2?"

# COMMAND ----------

# MAGIC %md
# MAGIC For more information about transformations in Spark, check the <a href="https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html">PySpark function documentation</a>

# COMMAND ----------

from pyspark.sql.functions import *   # import the pyspark functions we need to do transformations

# COMMAND ----------

df1 = diamonds.groupBy("cut", "color").avg("price") # a simple grouping

# COMMAND ----------

df2 = (
  df1.join(diamonds, 'color', 'inner')         # an inner join, selecting some columns and defining others
  .select("`avg(price)`", "carat", "color")
  .withColumnRenamed('avg(price)', 'average_price')
  .withColumn('is_fancy', col('carat') > lit(3))
)

# COMMAND ----------

fancy_diamonds_df = df2.where(col('is_fancy'))  # could also replace where() with filter()

# COMMAND ----------

fancy_diamonds_df.where(~col('is_fancy')).display()   # ~ is negation, read "is_fancy" is not true

# COMMAND ----------

fancy_diamonds_df.where(col('is_fancy')).display()

# COMMAND ----------

# MAGIC %md These transformations are now complete in a sense but nothing has happened. As you'll see above we don't get any results back! 
# MAGIC 
# MAGIC The reason for that is these computations are *lazy* in order to build up the entire flow of data from start to finish required by the user. This is a intelligent optimization for two key reasons. Any calculation can be recomputed from the very source data allowing Apache Spark to handle any failures that occur along the way, successfully handle stragglers. Secondly, Apache Spark can optimize computation so that data and computation can be `pipelined` as we mentioned above. Therefore, with each transformation Apache Spark creates a plan for how it will perform this work.
# MAGIC 
# MAGIC To get a sense for what this plan consists of, we can use the `explain` method. Remember that none of our computations have been executed yet, so all this explain method does is tell us the lineage for how to compute this exact dataset.

# COMMAND ----------

df2.explain()

# COMMAND ----------

fancy_diamonds_df.write.format("delta").mode("overwrite").saveAsTable("fancy_diamonds")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from fancy_diamonds

# COMMAND ----------

# MAGIC %md ## Conclusion
# MAGIC 
# MAGIC In this notebook we've covered a ton of material! But you're now well on your way to understanding Spark and Databricks! Now that you've completed this notebook, you should hopefully be more familiar with the core concepts of Spark on Databricks.
