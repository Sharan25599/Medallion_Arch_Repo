# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType,BooleanType
from pyspark.sql.functions import to_date, col, date_format,when,concat_ws
from pyspark.sql import SparkSession

# COMMAND ----------

def mounting_from_adf(source,mountpoint,key,value):
    dbutils.fs.mount(
    source=source,
    mount_point= mountpoint,
    extra_configs={key:value})

# COMMAND ----------

def read_file(file_format, file_path,custom_schema, **options):
   return spark.read.format(file_format).schema(custom_schema).options(**options).load(file_path)

# COMMAND ----------

def change_date_format(df, column_name, current_format, new_format):
    df = df.withColumn(column_name, to_date(col(column_name), current_format))
    df = df.withColumn(column_name, date_format(col(column_name), new_format))
    return df

# COMMAND ----------

def change_date_type(df, date_column):
    return df.withColumn(date_column, to_date(col(date_column)))

# COMMAND ----------

def map_gender(df,column_name):
    return df.withColumn(column_name,when(col(column_name) == 'M','Male')\
                              .when(col(column_name) == 'F','Female')\
                               .otherwise('null'))


# COMMAND ----------

def concat_columns(df,column_name,column1,column2,column3):
    return df.withColumn(column_name, concat_ws(",",column1,column2,column3))


# COMMAND ----------

def drop_columns(df, cols):
    return df.drop(*cols)

# COMMAND ----------

def fill_na_with_default(student_df, default_numeric=0, default_string="N/A"):
    df_filled = student_df.fillna(default_numeric).fillna(default_string)
    return df_filled

# COMMAND ----------

def write_delta(df,file_format,output_path,database_name,table_name):
    df.write.format(file_format).mode("overwrite").option("path", output_path).saveAsTable(f"{database_name}.{table_name}")