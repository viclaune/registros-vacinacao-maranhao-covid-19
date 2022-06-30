from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession\
    .builder\
    .appName("job-2-spark")\
    .config("spark.sql.warehouse.dir", abspath('spark-warehouse'))\
    .config("fs.s3a.endpoint", "http://192.168.100.94:9000")\
    .config("fs.s3a.access.key", "minioadmin")\
    .config("fs.s3a.secret.key", "minioadmin")\
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("fs.s31.style.access", "True")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("parquet")\
    .option("header","True")\
    .option("inferSchema","True")\
    .parquet("processing/regs-vac-ma.parquet")