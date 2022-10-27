# Databricks notebook source
# MAGIC %md 
# MAGIC #Ingestion of Constructor file(Json) and Drivers file(Nested Json)

# COMMAND ----------

dbutils.widgets.text('Source_name',"")
a=dbutils.widgets.get('Source_name')
a

# COMMAND ----------

# MAGIC %run "../Config/config_folder_name"

# COMMAND ----------

# MAGIC %run "../Config/common_func"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType
from pyspark.sql.functions import col,lit,concat

# COMMAND ----------

schemas='constructorId Int,constructorRef string,name String,nationality String,url String'
df=spark.read.format('json').schema(schemas).option('header',True).load(f'{ip_folder_name}/constructors.json')
df_001=df.select(df.constructorId.alias('Constructor_Id'),df.constructorRef.alias('Constructor_ref'),'name','nationality').withColumn('Source',lit(a))
df_002=ingestion_date(df_001)
df_002.display()
#df_001.write.mode('overwrite').parquet(f'{op_folder_name}/constructors')

# COMMAND ----------

name_struct=StructType([StructField('forename',StringType(),True),
                        StructField('surname',StringType(),True)
])

driver_struct=StructType([StructField('code',StringType(),True),
                        StructField('dob',DateType(),True),
                          StructField('driverId',IntegerType(),False),
                        StructField('driverRef',StringType(),True),
                          StructField('name',name_struct),
                        StructField('nationality',StringType(),True),
                          StructField('number',StringType(),True),
                        StructField('url',StringType(),True)
])
df_driver=spark.read.format('json').schema(driver_struct).option('header',True).load(f'{ip_folder_name}/drivers.json')
df_driver_new=df_driver.withColumnRenamed('driverId','Driver_Id').withColumnRenamed('driverRef','Driver_Ref').withColumn('Name',concat(col('name.forename'),lit(' '),col('name.surname'))).withColumn('Source',lit(a)).drop('url')
df_driver_neww=ingestion_date(df_driver_new)
display(df_driver_neww)
#df_driver_neww.write.mode('overwrite').parquet(f'{op_folder_name}/drivers')

# COMMAND ----------

dbutils.notebook.exit('Success!!')
