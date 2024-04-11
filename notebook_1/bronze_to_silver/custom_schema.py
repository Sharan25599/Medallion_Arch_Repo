# Databricks notebook source
from pyspark.sql.types import *


# COMMAND ----------

schema = StructType([
    StructField("Customer ID", StringType(), True),
    StructField("Order ID", StringType(), True),
    StructField("Product Name", StringType(), True),
    StructField("Price", StringType(), True),
    StructField("Quantity", StringType(), True),
    StructField("Order Date", StringType(), True),
    StructField("Delivery Date", StringType(), True),
    StructField("Payment Method", StringType(), True),
    StructField("Country", StringType(), True)
    ])