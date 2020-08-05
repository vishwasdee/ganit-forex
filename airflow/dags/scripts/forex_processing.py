from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Forex processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the file forex_rates.json from the HDFS
df = spark.read.json('hdfs://namenode:9000/forex/rates_filtered.json')


# Unpivot the sequence of Date values from column to row using stack expression
base=df.select("base").rdd.collect()[0].asDict()["base"]
start_at=df.select("start_at").rdd.collect()[0].asDict()["start_at"]
end_at=df.select("end_at").rdd.collect()[0].asDict()["end_at"]

#selcting only the columns that need to be unpivoted
df2=df.select("rates.*")

#generating stack expression
stack_exp = str(len(df2.columns))+','+','.join(["'{}',`{}`".format(v,v) for v in df2.columns])

#transforming the dataframe and adding other static columns
result=df2.select(expr(f'''stack({stack_exp})''').alias('forex_date','vals')).select('forex_date', 'vals.*').withColumn("base",lit(base)).withColumn("start_at",lit(start_at)).withColumn("end_at",lit(end_at))

#removing duplicates if any and replacing Null values with 0 in rates if any.
forex_rates=result.select('base','forex_date','USD','JPY','CAD','GBP','NZD','INR','start_at','end_at').dropDuplicates(['base', 'forex_date']).fillna(0, subset=['USD','JPY','CAD','GBP','NZD','INR']).orderBy("forex_date")


# Export the dataframe into the Hive table forex_rates
forex_rates.write.mode("overwrite").insertInto("exchange_rates",overwrite=True)