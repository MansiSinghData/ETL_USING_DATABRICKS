# Databricks notebook source
# MAGIC %md
# MAGIC #Ingestion of Circuits file and Race File(CSV files)

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

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,TimestampType,DateType
from pyspark.sql.functions import *

# COMMAND ----------

struct=StructType([StructField('circuitId',IntegerType(),False),
                  StructField('circuitRef',StringType(),True),
                  StructField('name',StringType(),True),
                   StructField('location',StringType(),True),
                   StructField('country',StringType(),True),
                   StructField('lat',DoubleType(),True),
                   StructField('lng',DoubleType(),True),
                   StructField('alt',DoubleType(),True),
                   StructField('url',StringType(),True)
                  ])

df=spark.read.format('csv').schema(struct).option('header',True).load(f'{ip_folder_name}/circuits.csv')
df_filtered=df.select(df.circuitId.alias('circuit_id'),
                      df.circuitRef.alias('circuit_Ref'),'name','location','country',
                      df.lat.alias('latitude'),df.lng.alias('longitude'),df.alt.alias('Altitude'),'url')

df_filtered_new=df_filtered.drop('url')
df_002=df_filtered_new.withColumn('Source',lit(a))
df_002=ingestion_date(df_002)
display(df_002)
df_002.write.mode('overwrite').parquet(f"{op_folder_name}/circuits")


# COMMAND ----------

schema_race=StructType([StructField('raceId',IntegerType(),False),
                       StructField('year',IntegerType(),True),
                        StructField('round',IntegerType(),True),
                        StructField('circuitId',IntegerType(),True),
                        StructField('name',StringType(),True),
                        StructField('date',DateType(),True),
                        StructField('time',StringType(),True),
                        StructField('url',StringType(),True)
                       ])
df_race=spark.read.format('csv').schema(schema_race).option('header',True).load(f'{ip_folder_name}/races.csv')
df_race_002=df_race.withColumn('race_timestamp',to_timestamp(lit(concat(col('date'),lit(' '),col('time'))),'yyyy-MM-dd HH:mm:ss')).withColumn('Source',lit(a))

df_race_0022=ingestion_date(df_race_002)
df_race_003=df_race_0022.select(df_race_002.raceId.alias('race_id'),df_race_002.year.alias('race_year'),df_race_002.round,df_race_002.circuitId.alias('circuit_id'),'name','race_timestamp','Ingested_datetime')
display(df_race_003)
df_race_003.write.mode('overwrite').parquet(f"{op_folder_name}/race")


# COMMAND ----------

dbutils.notebook.exit('Success!!')
