# Databricks notebook source
# location where the superstore file is stored -> /FileStore/tables/superstore.csv

# COMMAND ----------

dbutils.widgets.dropdown("time_period",'Weekly',['Weekly','Monthly'])

# COMMAND ----------

# code to find start date and end date
from datetime import date,timedelta,datetime
from pyspark.sql.functions import * 

time_period= dbutils.widgets.get("time_period")
print(time_period)
today = date.today()

if time_period=='Weekly':
    start_date=today-timedelta(days=today.weekday(),weeks=1)-timedelta(days=1)
    end_date=start_date+timedelta(days=6)

else:
    first=today.replace(day=1)
    end_date=first-timedelta(days=1)
    start_date=first-timedelta(days=end_date.day)

print(start_date,end_date)

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/superstore.csv", header=True, inferSchema=True)
df.display()

# COMMAND ----------

# 1. Total Number of customers

df.select("Customer_id").distinct().count()


# COMMAND ----------

df.createOrReplaceTempView("Superstore") # creates a temporary table to work on sql

# COMMAND ----------

# DBTITLE 1,Total number of customers using SQL
# MAGIC
# MAGIC %sql 
# MAGIC Select Count(distinct Customer_id ) from Superstore
# MAGIC

# COMMAND ----------

# DBTITLE 1,using sql binding
total_customers = spark.sql("Select Count (Distinct Customer_id) from {Superstore}", Superstore=df)
total_customers.display

# COMMAND ----------

# DBTITLE 1,Weekly and monthly sales calculation
display(spark.sql(f"""select count (Distinct Customer_id) from Superstore where order_date between '{start_date}' and '{end_date}'"""))

# COMMAND ----------

# DBTITLE 1,Total number of orders
display(spark.sql("Select count(distinct order_id ) from Superstore" ))

# COMMAND ----------

# DBTITLE 1,Total number of sales
display(spark.sql("Select SUM(Sales) from Superstore"))

# COMMAND ----------

# DBTITLE 1,Total number of profits
display(spark.sql("Select SUM(Profit) from Superstore"))

# COMMAND ----------

# DBTITLE 1,Total sales by country
display(spark.sql("Select Sum(Sales) as Total_sales, Country from Superstore group by country"))

# COMMAND ----------

# DBTITLE 1,Most profitable region
# MAGIC %sql select Sum(Sales) as top_sales, region from Superstore group by region ORDER BY top_sales DESC
# MAGIC
# MAGIC /* most sales is coming from West region*/

# COMMAND ----------

# DBTITLE 1,Top sales based on category
# MAGIC %sql select Sum(Sales) as top_sales, category from Superstore group by category ORDER BY top_sales DESC
# MAGIC /* Maximum sales is from the products that belong to technology category*/

# COMMAND ----------

# DBTITLE 1,Top 10 sales based on subcategory
# MAGIC %sql select Sum(Sales) as top_sales,Sub_category from Superstore group by 2 ORDER BY 1 DESC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Most ordered product quantity
# MAGIC %sql select SUM(Quantity) as Max_quantity,Product_name from Superstore group by 2 ORDER BY 1 DESC

# COMMAND ----------

# DBTITLE 1,Sales done by each customer and city
# MAGIC %sql select SUM(Sales) as Sales, Customer_name, city from Superstore group by 2,3  ORDER BY 1 DESC