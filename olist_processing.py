# Databricks notebook source
#Load, Transform, Persist Pipeline

#1-mount the data lakes
#2-loads csvs from landing data lake
#3-convert csvs to parquet and move then to processing data lake
#4-create sql database
#5-create tables based on parquet format files
#6-specific analysis wil be moved to curated data lake and then loaded into sql tables
#7-powerbi application reads directly from sql tables at databricks rest api service


# COMMAND ----------

# MAGIC %md
# MAGIC # Mounting Data lakes

# COMMAND ----------


 #dbutils.fs.unmount("/mnt/landing") 


# COMMAND ----------

#  configs = {"fs.azure.account.auth.type": "OAuth",
#            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#            "fs.azure.account.oauth2.client.id": "ebf68edb-3221-491c-b8b2-5d7c487596b9",
#            "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="olist_scope1",key="olist-secret"),
#            "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/8f8d6011-959e-4775-ab1c-02abdf5ce693/oauth2/token"}

# # # Optionally, you can add <directory-name> to the source URI of your mount point.
#  dbutils.fs.mount(
#    source = "abfss://landing@oliststorageaccount2.dfs.core.windows.net/", 
#    mount_point = "/mnt/landing",
#    extra_configs = configs)


# COMMAND ----------

 dbutils.fs.ls("/mnt/landing/")



# COMMAND ----------

#Example: error device already monted
# dbutils.fs.unmount("/mnt/processing")


# COMMAND ----------

# dbutils.fs.mount(
#   source = "abfss://processing@oliststorageaccount2.dfs.core.windows.net/",
#   mount_point = "/mnt/processing",
#   extra_configs = configs)



# COMMAND ----------

# dbutils.fs.ls("/mnt/processing")



# COMMAND ----------

# dbutils.fs.unmount("/mnt/curated")



# COMMAND ----------


# dbutils.fs.mount(
#   source = "abfss://curated@oliststorageaccount2.dfs.core.windows.net/",
#   mount_point = "/mnt/curated",
#   extra_configs = configs)



# COMMAND ----------

# dbutils.fs.ls("/mnt/curated")



# COMMAND ----------

# MAGIC %md
# MAGIC # Readings CSVs in Landing Data Lake to DataFrames

# COMMAND ----------

#read customer csv to dataframe and test it
df_customers = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/landing/dbo.olist_customers_dataset.csv")

 
#display the dataframe
display(df_customers)

# COMMAND ----------

df_customers.printSchema()

# COMMAND ----------

#read the rest of csv files to the respectives dataframes

df_geolocation = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/landing/dbo.olist_geolocation_dataset.csv")
df_order_items = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/landing/dbo.olist_order_items_dataset.csv")
df_order_payments = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/landing/dbo.olist_order_payments_dataset.csv")
df_order_reviews = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/landing/dbo.olist_order_reviews_dataset.csv")
df_orders = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/landing/dbo.olist_orders_dataset.csv")
df_sellers = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/landing/dbo.olist_sellers_dataset.csv")
df_product_category_name_translation = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/landing/dbo.product_category_name_translation.csv")



# COMMAND ----------

# spark.read.format('csv').option('inferSchema', 'true').option('header',"true").load('/mnt/landing/dbo.product_category_name_translation.csv').display()
 

# COMMAND ----------

# MAGIC %md
# MAGIC # Create SQL Temp Views

# COMMAND ----------

df_customers.createOrReplaceTempView("customers_tempview")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM customers_tempview

# COMMAND ----------

# MAGIC %md
# MAGIC Create SQL Database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS customers_db

# COMMAND ----------

# MAGIC %md
# MAGIC # Create SQL Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customers_db.customers
# MAGIC
# MAGIC  -- geolocation
# MAGIC  -- order_items
# MAGIC  -- order_payments
# MAGIC  -- order_reviews
# MAGIC  -- orders
# MAGIC  -- sellers
# MAGIC  -- product_category_name_translation

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customers_db.geolocation
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customers_db.order_items
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customers_db.order_payments
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customers_db.order_reviews
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customers_db.orders
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customers_db.sellers
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customers_db.product_category_name_translation
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.customers 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/landing/dbo.olist_customers_dataset.csv'
# MAGIC OPTIONS (header "true", inferSchema "true")
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# %sql
# SELECT COUNT(customer) FROM customers_db.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.geolocation 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/landing/dbo.olist_geolocation_dataset.csv'
# MAGIC OPTIONS (header "true", inferSchema "true")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.order_items 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/landing/dbo.olist_order_items_dataset.csv'
# MAGIC OPTIONS (header "true", inferSchema "true")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.order_payments 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/landing/dbo.olist_order_payments_dataset.csv'
# MAGIC OPTIONS (header "true", inferSchema "true")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.order_reviews 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/landing/dbo.olist_order_reviews_dataset.csv'
# MAGIC OPTIONS (header "true", inferSchema "true")
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.orders 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/landing/dbo.olist_orders_dataset.csv'
# MAGIC OPTIONS (header "true", inferSchema "true")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.sellers 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/landing/dbo.olist_sellers_dataset.csv'
# MAGIC OPTIONS (header "true", inferSchema "true")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.product_category_name_translation 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/landing/dbo.product_category_name_translation.csv'
# MAGIC OPTIONS (header "true", inferSchema "true")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM customers_db.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE customers_db.customers

# COMMAND ----------

df_customers_SQL = spark.table('customers_db.customers')
display(df_customers_SQL)

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtering the DataSet

# COMMAND ----------

df_customers_SQL.select('customer_state').distinct().show()

# COMMAND ----------

from pyspark.sql.functions import col
df_customers_SQL = df_customers_SQL.filter(col("customer_state") == "RJ")

# COMMAND ----------

display(df_customers_SQL)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Full Parquet Datasets to Processing Data lake

# COMMAND ----------

df_customers.write.mode("overwrite").parquet("/mnt/processing/customers.parquet")


# COMMAND ----------


df_geolocation.write.mode("overwrite").parquet("/mnt/processing/geolocation.parquet")
df_order_items.write.mode("overwrite").parquet("/mnt/processing/order_items.parquet")
df_order_payments.write.mode("overwrite").parquet("/mnt/processing/order_payments.parquet")
df_order_reviews.write.mode("overwrite").parquet("/mnt/processing/order_reviews.parquet")
df_orders.write.mode("overwrite").parquet("/mnt/processing/orders.parquet")
df_sellers.write.mode("overwrite").parquet("/mnt/processing/sellers.parquet")
df_product_category_name_translation.write.mode("overwrite").parquet("/mnt/processing/product_category_name_translation.parquet")


# COMMAND ----------

# MAGIC %md
# MAGIC # Write Filtered Parquet to Processing Data Lake

# COMMAND ----------

df_customers_SQL.write.mode("overwrite").parquet("/mnt/processing/customers_RJ.parquet")


# COMMAND ----------

df_customers_parq = spark.read.parquet("/mnt/processing/customers_RJ.parquet")
display(df_customers_parq)

# COMMAND ----------

df_customers_parq.createOrReplaceTempView("CustomersParquetTable")
custparkSQL = spark.sql("select * from CustomersParquetTable where customer_state = 'RJ'")
display(custparkSQL)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create SQL Tables based on Parquet files at Processing Data Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full parquet
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.customers_pqt USING PARQUET OPTIONS (path "/mnt/processing/customers.parquet", header "true", inferSchema "true")
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_db.customers_pqt

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Full parquet
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.geolocation_pqt USING PARQUET OPTIONS (path "/mnt/processing/geolocation.parquet", header "true", inferSchema "true")
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Full parquet
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.order_items_pqt USING PARQUET OPTIONS (path "/mnt/processing/order_items.parquet", header "true", inferSchema "true")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Full parquet
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.order_payments_pqt USING PARQUET OPTIONS (path "/mnt/processing/order_payments.parquet", header "true", inferSchema "true")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Full parquet
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.order_reviews_pqt USING PARQUET OPTIONS (path "/mnt/processing/order_reviews.parquet", header "true", inferSchema "true")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Full parquet
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.orders_pqt USING PARQUET OPTIONS (path "/mnt/processing/orders.parquet", header "true", inferSchema "true")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Full parquet
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.sellers_pqt USING PARQUET OPTIONS (path "/mnt/processing/sellers.parquet", header "true", inferSchema "true")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Full parquet
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.product_category_name_translation_pqt USING PARQUET OPTIONS (path "/mnt/processing/product_category_name_translation.parquet", header "true", inferSchema "true")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Filtered parquet
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.customers_RJ_pqt USING PARQUET OPTIONS (path "/mnt/processing/customers_RJ.parquet", header "true", inferSchema "true")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE customers_db.customers_RJ_pqt

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from customers_db.customers_RJ_pqt

# COMMAND ----------

df_customers_parq = spark.read.parquet("/mnt/processing/customers_RJ.parquet")
df_customers_parq.createOrReplaceTempView("CustomersParquetTableByState")
df_customers_by_state_parq = spark.sql("select * from CustomersParquetTableByState where customer_state='RJ'")
display(df_customers_by_state_parq)

# COMMAND ----------

display(df_customers_parq)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write processed CSVs to Curated Data Lake

# COMMAND ----------

df_customers_parq.write.option("header",True).option("delimiter",",").mode("overwrite").csv("/mnt/curated/customers_RJ.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC # Test Reading CSV file located at Curated Data Lake

# COMMAND ----------

#read in the data to dataframe df
df_RJ = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/curated/customers_RJ.csv")
 
#display the dataframe
display(df_RJ)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Filtered Curated CSV
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.customers_RJ_csv 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/curated/customers_RJ.csv'
# MAGIC OPTIONS (header "true", inferSchema "true")
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE customers_db.customers_RJ_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from customers_db.customers_RJ_csv 
