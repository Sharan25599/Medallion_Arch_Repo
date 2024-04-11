# Databricks notebook source
def read_delta(spark,file_format,table_location):
    return spark.read.format(file_format).load(table_location)

# COMMAND ----------

def group_by_avg(df, group_col, avg_col):
    result_df = df.groupBy(group_col).avg(avg_col)
    return result_df

# COMMAND ----------

def group_by_min(df, group_colunm,min_column):
    return df.groupBy(group_colunm).min(min_column)
    

# COMMAND ----------

def total_gpa(df, col1,col2,col3):
    return df.groupBy(col1,col2).sum(col3)

# COMMAND ----------

def rename_column(df, old_name, new_name):
    return df.withColumnRenamed(old_name, new_name)

# COMMAND ----------

def write_delta(df,file_format,output_path,database_name,table_name):
    df.write.format(file_format).mode("overwrite").option("path", output_path).saveAsTable(f"{database_name}.{table_name}")