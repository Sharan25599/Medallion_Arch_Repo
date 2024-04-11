# Databricks notebook source
# MAGIC %run /Workspace/Notebook/silver_to_gold/common_functions

# COMMAND ----------

table_location = 'dbfs:/mnt/mountpointstorageadf/Silver'
student_df = spark.read.format('delta').load(table_location)
student_df.display()

# COMMAND ----------

avg_gpa_df = group_by_avg(student_df, 'ethnicity', 'hs_gpa')
avg_gpa_df.display()

# COMMAND ----------

min_entry_age_df=group_by_min(student_df, 'entry_academic_period','entry_age')
min_entry_age_df.display()


# COMMAND ----------

total_gpa_df=total_gpa(student_df, 'gender','status','hs_gpa')
total_gpa_df.display()

# COMMAND ----------

rename_column_df=rename_column(total_gpa_df, 'sum(hs_gpa)', 'total_gpa')
rename_column_df.display()


# COMMAND ----------

file_format='delta'
output_path="dbfs:/mnt/mountpointstorageadf/Gold"
database_name="gold"
table_name="Student_table"
write_delta(rename_column_df,file_format,output_path,database_name,table_name)