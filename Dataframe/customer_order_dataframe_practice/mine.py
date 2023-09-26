from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("customer_avg").master("local[*]").getOrCreate()

schema = StructType([ \
                     StructField("customer_id", IntegerType(), True), \
                     StructField("item_id", IntegerType(), True), \
                     StructField("spent", FloatType(), True)])      
                     
           
df = spark.read.schema(schema).csv("customer-orders.csv")
df.printSchema()


cust_grouped = df.groupBy("customer_id").agg(func.sum(func.col("spent")))
new_df = cust_grouped.withColumn("total_spent",func.round(func.col("sum(spent)"),\
2)).sort("total_spent")

new_df=new_df.select("customer_id", "total_spent")



cust_grouped.show()
new_df.show(new_df.count())
