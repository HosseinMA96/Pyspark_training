from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

def parse_lines(line):
    splt = line.split(',')
    customer_id = int(splt[0])
    money = float(splt[2])

    return (customer_id, money)

lines = sc.textFile('customer-orders.csv')
customer_paid = lines.map(parse_lines)




aggregated_customer = customer_paid.reduceByKey(lambda x, y: x + y)
sortd = aggregated_customer.map(lambda x:(x[1], x[0])).sortByKey()

normal = sortd.map(lambda x:(x[1], x[0]))
results = normal.collect()

for result in results:
    print(f'customer {result[0]} total {result[1]: .2f}')
    #print(result, len(result))
