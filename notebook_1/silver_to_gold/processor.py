# Databricks notebook source
# MAGIC %run /Workspace/NoteBook1/silver_to_gold/common_functions

# COMMAND ----------

table_path="dbfs:/mnt/mountpointstorageadfjson/Silver"
customer_df = spark.read.format("delta").load(table_path)
customer_df.display()

# COMMAND ----------

# DBTITLE 1,Separating the month column from order_date column
customer_df=month_column(customer_df, "order_date", "month")
customer_df.display()

# COMMAND ----------

# DBTITLE 1,Revenue by Month and Country (Price*Quantity)
revenue_df=total_revenue(customer_df, "revenue", "price", "quantity", "month", "country", "revenue", "total_revenue" )
revenue_df.display()

# COMMAND ----------

# DBTITLE 1,Customer Base on Country Level i.e count distinct
customer_base_country_df=customer_base_country(customer_df,"country","customer_id","count_of_customer")
customer_base_country_df.display()

# COMMAND ----------

# DBTITLE 1,Highly valuable customers, avg spending is more than 800
groupby_column="customer_id"
aggregate_column="price"
alias_column="avg_price"
value=800
valuable_customer_df=valuable_customer(customer_df, groupby_column, aggregate_column, alias_column, value)
valuable_customer_df.display()

# COMMAND ----------

delivery_date_column="delivery_date"
order_date_column="order_date"
df1=customer_df.withColumn(delivery_time_column, datediff(col(delivery_date_column), col(order_date_column)))
df1.display()

# COMMAND ----------

# DBTITLE 1,Average Delivery Time
delivery_time_column="delivery_time"
delivery_date_column="delivery_date"
order_date_column="order_date"
average_delivery_time_df=avg_delivery_time(customer_df, delivery_time_column, delivery_date_column, order_date_column)
average_delivery_time_df.display()

# COMMAND ----------

# DBTITLE 1,Normalize all date fields to yyyy-MM-dd format
date_fields_df=type_cast(customer_df, "delivery_date", 'yyyy-MM-dd')
date_fields_df=type_cast(date_fields_df, "order_date", 'yyyy-MM-dd')
date_fields_df.display()

# COMMAND ----------

# DBTITLE 1,Average Order Price
groupby_column="order_id"
agg_column="price"
alias_column="average_price"
average_order_price_df=avg_order_price(customer_df,groupby_column,agg_column, alias_column)
average_order_price_df.display()

# COMMAND ----------

# DBTITLE 1,Most Popular Payment Method
groupby="payment_method"
popular_payment_df=popular_payment(customer_df,groupby)
popular_payment_df.display()

# COMMAND ----------

write(df=revenue_df,file_format="delta",mode="overwrite",path="dbfs:/mnt/mountpointstorageadfjson/Gold/revenue_df")

# COMMAND ----------

write(df=customer_base_country_df,file_format="delta",mode="overwrite",path="dbfs:/mnt/mountpointstorageadfjson/Gold/customer_base_country_df")

# COMMAND ----------

write(df=valuable_customer_df,file_format="delta",mode="overwrite",path="dbfs:/mnt/mountpointstorageadfjson/Gold/valuable_customer_df")

# COMMAND ----------

write(df=average_delivery_time_df,file_format="delta",mode="overwrite",path="dbfs:/mnt/mountpointstorageadfjson/Gold/average_delivery_time_df")

# COMMAND ----------

write(df=date_fields_df,file_format="delta",mode="overwrite",path="dbfs:/mnt/mountpointstorageadfjson/Gold/date_fields_df")

# COMMAND ----------

write(df=average_order_price_df,file_format="delta",mode="overwrite",path="dbfs:/mnt/mountpointstorageadfjson/Gold/average_order_price_df")

# COMMAND ----------

write(df=popular_payment_df,file_format="delta",mode="overwrite",path="dbfs:/mnt/mountpointstorageadfjson/Gold/popular_payment_df")