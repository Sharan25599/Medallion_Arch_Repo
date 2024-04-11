# Databricks notebook source
# MAGIC %run /Workspace/NoteBook1/bronze_to_silver/common_function

# COMMAND ----------

# MAGIC %run /Workspace/NoteBook1/bronze_to_silver/custom_schema

# COMMAND ----------

source='wasbs://demofiles@medallionarchdemo.blob.core.windows.net/'
mountpoint = '/mnt/mountpointstorageadfjson'
key='fs.azure.account.key.medallionarchdemo.blob.core.windows.net'
value='8N1zwO3mnnKVql/fQENEqWs4tiAKhWFLtYG4VDDJKTpxOTyb4pRjrpiHifhG5YOVOMdIUqqsogXE+AStASb3h=='
mounting_from_adf(source,mountpoint,key,value)

# COMMAND ----------

file_format='json'
file_path = "dbfs:/mnt/mountpointstorageadfjson/Bronze/sample-json.json"
options={'multiLine':True}
custom_schema=schema
customer_df=read_file(file_format, file_path,custom_schema, **options)
customer_df.display()

# COMMAND ----------

customer_df=type_cast(customer_df, "Customer ID", 'int')
customer_df=type_cast(customer_df, "Delivery Date", 'date')
customer_df=type_cast(customer_df, "Order Date", 'date')
customer_df=type_cast(customer_df, "Order ID", 'int')
customer_df=type_cast(customer_df, "Price", 'float')
customer_df=type_cast(customer_df, "Quantity", 'int')
# customer_df.printSchema()

# COMMAND ----------

def snake_case_to_lower(x):
    a = x.lower()
    return a.replace(' ','_')

# COMMAND ----------

a=udf(snake_case_to_lower,StringType())
lst = list(map(lambda x:snake_case_to_lower(x),customer_df.columns))
to_snake_case_df = customer_df.toDF(*lst)
# to_snake_case_df.display()

# COMMAND ----------

drop_nulls_df=drop_null_values(to_snake_case_df)
drop_nulls_df.display()

# COMMAND ----------

drop_duplicates_df=drop_duplicates(drop_nulls_df)
drop_duplicates_df.display()

# COMMAND ----------

table_format="delta"
mode="overwrite"
path="dbfs:/mnt/mountpointstorageadfjson/Silver"
write_to_delta(drop_duplicates_df, table_format, mode, path)
    