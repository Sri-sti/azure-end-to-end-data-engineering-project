# Databricks notebook source


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Silver Layer Script**

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.auth.type.staz104shrishti.dfs.core.windows.net",
    "SAS"
)

spark.conf.set(
    "fs.azure.sas.token.provider.type.staz104shrishti.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
)

spark.conf.set(
    "fs.azure.sas.fixed.token.staz104shrishti.dfs.core.windows.net",
    "sv=2026-02-06&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2026-11-02T14:59:43Z&st=2026-10-31T06:44:43Z&spr=https&sig=a4Eq1LfphkgmJB%2FrlC%2FKvgFqf%2BHLcATmnWmFmNX2psI%3D"
)

# COMMAND ----------

df=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronz@staz104shrishti.dfs.core.windows.net/AdventureWorks_Calendar/AdventureWorks_Calendar.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Transformation

# COMMAND ----------


df=df.withColumn("Month",month(col("Date")))\
    .withColumn("Year",month(col("Date")))
display(df)

# COMMAND ----------

path="abfss://silver@staz104shrishti.dfs.core.windows.net/AdventureWorks_Calendar"
df.write.format("delta")\
    .mode("overwrite")\
    .option("path",path)\
    .save()

# COMMAND ----------

path="abfss://bronz@staz104shrishti.dfs.core.windows.net/AdventureWorks_Customers/AdventureWorks_Customers.csv"
df_cus=spark.read.format("csv")\
    .option("header",True)\
        .option("inferSchema",True)\
            .load("abfss://bronz@staz104shrishti.dfs.core.windows.net/AdventureWorks_Customers/AdventureWorks_Customers.csv")
display(df_cus)

# COMMAND ----------

df_cus.withColumn("FullName",concat(col("Prefix"),lit(" "),col("FirstName"),lit(" "),col("LastName"))).show()

# COMMAND ----------

df_cus=df_cus.withColumn("fullName",concat(col("Prefix"),lit(" "),col("FirstName"),lit(" "),col("LastName")))
display(df_cus)

# COMMAND ----------

path="abfss://silver@staz104shrishti.dfs.core.windows.net/AdventureWorks_Customers"
df_cus.write.format("parquet")\
    .mode("overwrite")\
    .option("path",path)\
    .save()

# COMMAND ----------

path="abfss://bronz@staz104shrishti.dfs.core.windows.net/AdventureWorks_Product_Categories/AdventureWorks_Product_Categories.csv"
df_procat=spark.read.format("csv")\
    .option("header",True)\
        .option("inferSchema",True)\
            .load(path)
display(df_procat)

# COMMAND ----------

path="abfss://bronz@staz104shrishti.dfs.core.windows.net/AdventureWorks_Products/AdventureWorks_Products.csv"
df_pro=spark.read.format("csv")\
    .option("header",True)\
        .option("inferSchema",True)\
            .load(path)
display(df_pro)

# COMMAND ----------

df_pro=df_pro.withColumn("ProductSKU",split(col("ProductSKU"),"-")[0])\
    .withColumn("ProductName",split(col("ProductName")," ")[1])

# COMMAND ----------

path="abfss://silver@staz104shrishti.dfs.core.windows.net/AdventureWorks_Products"
df_pro.write.format("parquet")\
    .mode("overwrite")\
    .option("path",path)\
    .save()

# COMMAND ----------

path="abfss://bronz@staz104shrishti.dfs.core.windows.net/AdventureWorks_Returns/AdventureWorks_Returns.csv"
df_ret=spark.read.format("csv")\
    .option("header",True)\
        .option("inferSchema",True)\
            .load(path)
display(df_ret)

# COMMAND ----------

path="abfss://silver@staz104shrishti.dfs.core.windows.net/AdventureWorks_Returns"
df_ret.write.format("parquet")\
    .mode("overwrite")\
    .option("path",path)\
    .save()

# COMMAND ----------

path="abfss://bronz@staz104shrishti.dfs.core.windows.net/AdventureWorks_Sales*"
df_sales=spark.read.format("csv")\
    .option("header",True)\
        .option("inferSchema",True)\
            .load(path)
display(df_sales)

# COMMAND ----------

df_sales=df_sales.withColumn("StockDate",to_timestamp(col("StockDate")))

# COMMAND ----------

df_sales=df_sales.withColumn("OrderNumber",regexp_replace(col("OrderNumber"),"S","M"))

# COMMAND ----------

df_sales=df_sales.withColumn("Multiply",col("OrderLineItem")*col("OrderQuantity"))

# COMMAND ----------

path="abfss://silver@staz104shrishti.dfs.core.windows.net/AdventureWorks_Sales"
df_ret.write.format("delta")\
    .mode("overwrite")\
    .option("path",path)\
    .save()

# COMMAND ----------

display(df_sales.groupBy("OrderDate").agg(count("OrderNumber").alias("total_order_number")))

# COMMAND ----------

path="abfss://bronz@staz104shrishti.dfs.core.windows.net/AdventureWorks_Territories/AdventureWorks_Territories.csv"
df_ter=spark.read.format("csv")\
    .option("header",True)\
        .option("inferSchema",True)\
            .load(path)
display(df_ter)

# COMMAND ----------

path="abfss://silver@staz104shrishti.dfs.core.windows.net/AdventureWorks_Territories"
df_ter.write.format("parquet")\
    .mode("overwrite")\
    .option("path",path)\
    .save()

# COMMAND ----------

path="abfss://bronz@staz104shrishti.dfs.core.windows.net/AdventureWorks_Product_Subcategories/AdventureWorks_Product_Subcategories.csv"
df_subcat=spark.read.format("csv")\
    .option("header",True)\
        .option("inferSchema",True)\
            .load(path)
display(df_subcat)

# COMMAND ----------

path="abfss://silver@staz104shrishti.dfs.core.windows.net/AdventureWorks_Product_Subcategories"
df_subcat.write.format("parquet")\
    .mode("overwrite")\
    .option("path",path)\
    .save()

# COMMAND ----------

spark.conf.unset("fs.azure.account.key.staz104shrishti.dfs.core.windows.net")

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

spark.range(5).show()

# COMMAND ----------

dbutils.fs.ls("abfss://bronz@staz104shrishti.dfs.core.windows.net/")