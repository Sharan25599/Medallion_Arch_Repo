# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

student_schema=StructType([
    StructField("id", IntegerType(), True),
    StructField("first_name", StringType(), True), 
    StructField("last_name", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("ethnicity", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("status", StringType(), True),
    StructField("entry_academic_period", StringType(), True),
    StructField("exclusion_type", StringType(), True),
    StructField("act_composite", IntegerType(), True),
    StructField("act_math", IntegerType(), True), 
    StructField("act_english", IntegerType(), True),
    StructField("act_reading", IntegerType(), True),
    StructField("sat_combined", IntegerType(), True),
    StructField("sat_math", IntegerType(), True),
    StructField("sat_verbal", IntegerType(), True),
    StructField("sat_reading", IntegerType(), True),
    StructField("hs_gpa", DoubleType(), True),
    StructField("hs_city", StringType(), True),
    StructField("hs_state", StringType(), True),
    StructField("hs_zip", StringType(), True),
    StructField("email", StringType(), True),
    StructField("entry_age", DoubleType(), True),
    StructField("ged", BooleanType(), True),
    StructField("english_2nd_language", StringType(), True),
    StructField("first_generation", StringType(), True)
])