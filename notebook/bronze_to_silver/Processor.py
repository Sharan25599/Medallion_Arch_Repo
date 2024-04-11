# Databricks notebook source
# MAGIC %run /Workspace/Notebook/bronze_to_silver/common_functions

# COMMAND ----------

# MAGIC %run /Workspace/Notebook/bronze_to_silver/custom_schema

# COMMAND ----------

source='wasbs://demo@sampleadls2torage.blob.core.windows.net/'
mountpoint = '/mnt/mountpointstorageadf'
key='fs.azure.account.key.sampleadls2torage.blob.core.windows.net'
value='mMDQ352rBPUuwsyL6fciXquDNrHiUJo0o+Ydaos2VUBz5nnQ8E1aVvf3rSX/7xrX0IQpUiKd02ej+AStq+4lw=='
mounting_from_adf(source,mountpoint,key,value)

# COMMAND ----------

file_format='csv'
file_path = "dbfs:/mnt/mountpointstorageadf/Bronze/Student_sheet.csv"
options={'header':True}
custom_schema=student_schema
student_df=read_file(file_format, file_path,custom_schema, **options)
student_df.display()

# COMMAND ----------

date_format_df=change_date_format(filled_null_value_df,'date_of_birth','dd-MM-yyyy','yyyy-MM-dd')
date_format_df.display()

# COMMAND ----------

date_type_df = change_date_type(date_format_df, "date_of_birth")
date_type_df.display()

# COMMAND ----------

mapping_gender_df=map_gender(date_type_df,'gender')
# mapping_gender_df.display()

# COMMAND ----------

concat_column_df=concat_columns(mapping_gender_df,'address','hs_city','hs_state','hs_zip')
# concat_column_df.display()

# COMMAND ----------

cols_to_drop = 'hs_city', 'hs_state', 'hs_zip','exclusion_type'
drop_df = drop_columns(concat_column_df, cols_to_drop)
# drop_df.display()

# COMMAND ----------

filled_null_value_df = fill_na_with_default(drop_df)
# filled_null_value_df.display()

# COMMAND ----------

drop_duplicate_df=filled_null_value_df.dropDuplicates()
# drop_duplicate_df.display()

# COMMAND ----------

drop_duplicate_df.count()

# COMMAND ----------

file_format='delta'
output_path="dbfs:/mnt/mountpointstorageadf/Silver"
database_name="silver"
table_name="Student_table"
write_delta(drop_duplicate_df,file_format,output_path,database_name,table_name)

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS silver")

# COMMAND ----------

