# Databricks notebook source
# MAGIC %md 
# MAGIC #Ingestion of Results file(Json) and Pits Stops file(Multiline Json)

# COMMAND ----------

dbutils.widgets.text('Source_name',"")
a=dbutils.widgets.get('Source_name')
a

# COMMAND ----------

# MAGIC %run "../Config/config_folder_name"

# COMMAND ----------

# MAGIC %run "../Config/common_func"

# COMMAND ----------

ip_folder_name
op_folder_name

# COMMAND ----------

from pyspark.sql.types import StringType,DoubleType,IntegerType,StructType,StructField
from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

df_schema=StructType([StructField('constructorId',IntegerType(),True),
                      StructField('driverId',IntegerType(),True),
                      StructField('fastestLap',IntegerType(),True),
                      StructField('fastestLapSpeed',DoubleType(),True),
                      StructField('fastestLapTime',StringType(),True),
                      StructField('grid',IntegerType(),True),
                      StructField('laps',IntegerType(),True),
                      StructField('milliseconds',IntegerType(),True),
                      StructField('number',IntegerType(),True),
                      StructField('points',DoubleType(),True),
                      StructField('position',IntegerType(),True),
                      StructField('positionOrder',IntegerType(),True),
                      StructField('positionText',StringType(),True),
                      StructField('raceId',IntegerType(),True),
                      StructField('rank',IntegerType(),True),
                      StructField('resultId',IntegerType(),False),
                      StructField('statusId',IntegerType(),True),
                      StructField('time',StringType(),True)
                     ])

df=spark.read.format('json').schema(df_schema).option('header',True).load(f'{ip_folder_name}/results.json')
df_001=df.withColumnRenamed('constructorId','constructor_Id').withColumnRenamed('driverId','driver_Id').withColumnRenamed('fastestLap','fastest_Lap').withColumnRenamed('fastestLapTime','fastest_Lap_Time').withColumnRenamed('fastestLapSpeed','fastest_Lap_Speed').withColumnRenamed('positionOrder','position_Order').withColumnRenamed('positionText','position_Text').withColumnRenamed('raceId','race_Id').withColumnRenamed('resultId','result_Id').drop('statusId').withColumn('Source',lit(a))
df_002=ingestion_date(df_001)
display(df_002)
df_002.write.mode('overwrite').partitionBy('race_Id').parquet(f'{op_folder_name}/Results')

# COMMAND ----------

schema_pits=StructType([StructField('raceId',IntegerType(),False),
                      StructField('driverId',IntegerType(),True),
                      StructField('stop',StringType(),True),
                      StructField('lap',IntegerType(),True),
                      StructField('time',StringType(),True),
                       StructField('duration',StringType(),True), 
                      StructField('milliseconds',IntegerType(),True)
                     ])

df_pits=spark.read.format('json').option('multiline',True).schema(schema_pits).option('header',True).load(f'{ip_folder_name}/pit_stops.json')
df_pits_001=df_pits.withColumnRenamed('raceId','race_Id').withColumnRenamed('driverId','driver_Id').withColumn('Source',lit(a))
df_pits_002=ingestion_date(df_pits_001)
display(df_pits_002)
df_pits_001.write.mode('overwrite').parquet('dbfs:/mnt/etlprojects/processed/pitsstop')

# COMMAND ----------

dbutils.notebook.exit('Success!!')
