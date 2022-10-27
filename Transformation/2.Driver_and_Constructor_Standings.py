# Databricks notebook source
# MAGIC %run "../Config/config_folder_name"

# COMMAND ----------

ip_folder_name
op_folder_name

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df=spark.read.format('parquet').load('dbfs:/mnt/etlprojects/transformed/race_result')

# COMMAND ----------

display(df)

# COMMAND ----------

drivers_standings=df.groupBy('race_year','driver_name','driver_nationality','team').agg(sum(col('points')).alias('Points'),count(when(col('position')==1,True)).alias('Win')).orderBy(col('race_year').desc(),col('Win').desc())

# COMMAND ----------

dr_window=Window.partitionBy('race_year').orderBy(desc('Win'),desc('Points'))
drivers_standings_ranks=drivers_standings.withColumn('Rank',rank().over(dr_window))
display(drivers_standings_ranks.filter('race_year==2020'))
drivers_standings_ranks.write.mode('overwrite').parquet('dbfs:/mnt/etlprojects/transformed/Driver_standings')

# COMMAND ----------

window_cntr=Window.partitionBy('race_year').orderBy(desc('Win'),desc('Points'))

constructor_standing=df.groupBy('team','race_year').agg(sum(col('points')).alias('Points'),count(when(col('position')==1,True)).alias('Win')).orderBy(col('race_year').desc(),col('Win').desc())
constructor_standing_final=constructor_standing.withColumn('Rank',rank().over(window_cntr))

display(constructor_standing_final.filter('race_year==2020'))

constructor_standing_final.write.mode('overwrite').parquet('dbfs:/mnt/etlprojects/transformed/Constructor_standings')

# COMMAND ----------


