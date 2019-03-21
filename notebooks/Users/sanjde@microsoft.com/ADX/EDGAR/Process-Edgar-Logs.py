# Databricks notebook source
bigdatacatalog_key = dbutils.preview.secret.get(scope = "bigdatacatalog", key = "key1")
dbutils.fs.mount(
  source = "wasbs://edgar-logs@bigdatacatalog.blob.core.windows.net",
  mount_point = "/mnt/edgar-logs",
  extra_configs = {"fs.azure.account.key.bigdatacatalog.blob.core.windows.net":bigdatacatalog_key})

# COMMAND ----------

dbutils.fs.ls('/mnt/edgar-logs/')

# COMMAND ----------

from datetime import datetime, timedelta

start_date = datetime.strptime("2017-01-02", "%Y-%m-%d")
end_date = datetime.strptime("2017-06-30", "%Y-%m-%d")
weekdays = {w: [] for w in range(53)}

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)
        
for d in daterange(start_date, end_date):
    week_no = d.isocalendar()[1]
    weekdays[week_no].append(d.strftime("/mnt/edgar-logs/log%Y%m%d.csv"))

# COMMAND ----------

weekdays[1]

# COMMAND ----------

spark.conf.set("spark.sql.avro.compression.codec", "deflate")
spark.conf.set("spark.sql.avro.deflate.level", "5")

# COMMAND ----------

dfs = spark.read.format("csv").option("header","true").option("inferSchema", "true").load("/mnt/edgar-logs/log20170101.csv")

# COMMAND ----------

files = weekdays[1]
dfs = spark.read.format("csv").option("header","true").option("inferSchema", "true").load(files)

# COMMAND ----------

dfs.head(10)

# COMMAND ----------

from pyspark.sql.functions import col, unix_timestamp, to_date,  date_format
from pyspark.sql import functions as sf
df = dfs.withColumn('accessdt', 
                   sf.concat(sf.from_unixtime(sf.unix_timestamp(dfs.date), "yyyy-MM-dd"),sf.lit(' '), sf.col('time')))
df = df.withColumn('weekno', date_format(to_date(col('accessdt'), 'yyyy-MM-dd'),'W'))
df = df.selectExpr("ip", "accessdt", "zone","regexp_extract(cik, '(\d+).(\d+)', 1) as cik ", "accession", "extention", "code", "size", "idx", "norefer", "noagent", "find", "crawler", "browser", "weekno")

# COMMAND ----------

df.head(10)