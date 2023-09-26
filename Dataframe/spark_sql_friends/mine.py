from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()

print("Let's display the name column:")
selected = people.select("age", "friends")



print("Group by age")
selected.groupBy("age").avg("friends").sort("age").show()

spark.stop()

