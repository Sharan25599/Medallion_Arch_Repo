# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

def read(format, table_path):
   return spark.read.format(format).load(table_path)

# COMMAND ----------

def month_column(df, actual_column, new_column):
    return df.withColumn(new_column, month(col(actual_column)))

# COMMAND ----------

def total_revenue(df,new_column, price_col, quantity_col, month_col, country_col, aggregation_column, alias_column):
    revenue = df.withColumn(new_column, col(price_col) * col(quantity_col))
    return revenue.groupBy(col(month_col), col(country_col)).agg(sum(new_column).alias(alias_column))

# COMMAND ----------

def customer_base_country(df,group_by_column, aggregate_column, alias_column):
    return df.groupBy(group_by_column).agg(countDistinct(aggregate_column).alias(alias_column))


# COMMAND ----------

def valuable_customer(df, groupby_column, aggregate_column, alias_column, value):
    return df.groupBy(groupby_column).agg(avg(aggregate_column).alias(alias_column)).filter(col(alias_column)>value)

# COMMAND ----------

def avg_delivery_time(df, delivery_time_column, delivery_date_column, order_date_column):
    df = df.withColumn(delivery_time_column, datediff(col(delivery_date_column), col(order_date_column)))
    return df.select(avg(col(delivery_time_column)).alias("avg_delivery_time"))

# COMMAND ----------

def type_cast(df, column_name, time_format):
    return df.withColumn(column_name, to_date(col(column_name), time_format))

# COMMAND ----------

def popular_payment(df,groupby_column):
    payement=df.groupBy(groupby_column).count().orderBy(col("count").desc())
    return payement.limit(1)

# COMMAND ----------

def avg_order_price(df,groupby_column,agg_column, alias_column):
    return df.groupBy(groupby_column).agg(avg(agg_column).alias(alias_column))

# COMMAND ----------

def write(df,file_format,mode,path):
    df.write.format(file_format).mode(mode).save(path)