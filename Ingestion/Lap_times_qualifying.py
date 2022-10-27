# Databricks notebook source
# MAGIC %md 
# MAGIC #Ingestion of lap_times(CSV) and Qualifying Folder(Multiline Json)

# COMMAND ----------

dbutils.widgets.text('Source_name',"")
a=dbutils.widgets.get('Source_name')
a

# COMMAND ----------

# MAGIC %run "../Config/config_folder_name"

# COMMAND ----------

# MAGIC %run "../Config/common_func"

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,StructType,StructField
from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

struct_lap=StructType([StructField('raceId',IntegerType(),False),
                       StructField('driverId',IntegerType(),True),
                       StructField('lap',IntegerType(),True),
                       StructField('position',IntegerType(),True),
                       StructField('time',StringType(),True),
                       StructField('milliseconds',IntegerType(),True)
                       ])
df_lap=spark.read.format('csv').schema(struct_lap).load(f'{ip_folder_name}/lap_times/')
df_lap_001=df_lap.withColumnRenamed('raceId','race_Id').withColumnRenamed('driverId','driver_Id').withColumn('Source',lit(a))
df_lap_002=ingestion_date(df_lap_001)
display(df_lap_002)
df_lap_001.write.mode('overwrite').parquet(f'{op_folder_name}/laps')


# COMMAND ----------

df_q_schema=StructType([StructField('constructorId',IntegerType(),True),
                        StructField('driverId',IntegerType(),True),
                        StructField('number',IntegerType(),True),
                        StructField('position',IntegerType(),True),
                        StructField('q1',StringType(),True),
                        StructField('q2',StringType(),True),
                        StructField('q3',StringType(),True),
                        StructField('qualifyId',IntegerType(),False),
                        StructField('raceId',IntegerType(),True) ])

df_q=spark.read.format('json').schema(df_q_schema).option('multiline',True).load(f'{ip_folder_name}/qualifying/')
df_q_001=df_q.withColumnRenamed('constructorId','constructor_Id').withColumnRenamed('driverId','driver_Id').withColumnRenamed('qualifyId','qualify_Id').withColumnRenamed('raceId','race_Id').withColumn('Source',lit(a))
df_q_002=ingestion_date(df_q_001)
display(df_q_002)
df_q_001.write.mode('overwrite').parquet(f'{op_folder_name}/qualifying')

# COMMAND ----------

dbutils.notebook.exit('Success!!')
