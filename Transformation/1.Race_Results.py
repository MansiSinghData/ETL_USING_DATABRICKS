# Databricks notebook source
# MAGIC %run "../Config/config_folder_name"

# COMMAND ----------

ip_folder_name
op_folder_name

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_ckts=spark.read.format('parquet').load(f'{op_folder_name}/circuits/').withColumnRenamed('location','circuit_location')
df_races=spark.read.format('parquet').load(f'{op_folder_name}/race/').withColumnRenamed('name','race_name').withColumnRenamed('race_timestamp','race_date')
df_ckt_races=df_ckts.join(df_races,df_ckts.circuit_id==df_races.circuit_id).select('race_year','race_name','race_date','circuit_location','race_id')
display(df_ckt_races)

# COMMAND ----------

df_result=spark.read.format('parquet').load(f'{op_folder_name}/Results/').withColumnRenamed('time','race_time').select('race_Id','driver_Id','constructor_Id','grid','fastest_Lap','race_time','points','position')
display(df_result)

# COMMAND ----------

df_drivers=spark.read.format('parquet').load(f'{op_folder_name}/drivers/').withColumnRenamed('Name','driver_name').withColumnRenamed('number','driver_number').withColumnRenamed('nationality','driver_nationality').select('Driver_Id','driver_name','driver_number','driver_nationality')
display(df_drivers)

# COMMAND ----------

df_constructor=spark.read.format('parquet').load(f'{op_folder_name}/constructors/').withColumnRenamed('name','team').select('Constructor_Id','team')
display(df_constructor)

# COMMAND ----------

df_final=df_result.join(df_ckt_races,df_result.race_Id==df_ckt_races.race_id).join(df_drivers,df_result.driver_Id==df_drivers.Driver_Id).join(df_constructor,df_result.constructor_Id==df_constructor.Constructor_Id).drop(df_result.race_Id).drop(df_ckt_races.race_id).drop(df_result.driver_Id).drop(df_drivers.Driver_Id).drop(df_result.constructor_Id).drop(df_constructor.Constructor_Id).withColumn('Ingested_datetime',current_timestamp())
display(df_final)

# COMMAND ----------

df_final.write.mode('overwrite').parquet('dbfs:/mnt/etlprojects/transformed/race_result')

# COMMAND ----------

df_abu_dhabi=df_final.filter(col('race_name').like('Abu Dhabi Grand Prix%')).filter('race_year==2020').orderBy(col('points').desc())
display(df_abu_dhabi)
