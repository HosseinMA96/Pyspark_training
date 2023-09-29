from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("Marvel_Names.txt")

lines = spark.read.text("Marvel_Graph.txt")

lines.show()
# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
#find minimum number of connections

min_conn = max(connections.agg(func.min("connections")).first()[0], 0)
print("bokhor", min_conn)

one_conn_ids = connections.filter(func.col("connections") == min_conn).select("id")

result = names.join(one_conn_ids, "id")

result.show(result.count())

print()


#mostPopularName = names.filter(func.col("id") == least_popular[0]).select("name").first()

#print(mostPopularName[0] + " is the most popular superhero with " + str(least_popular[1]) + " co-appearances.")

