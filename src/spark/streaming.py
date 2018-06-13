from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import StructType
from pyspark.sql import functions
import time
#import redis
import config

#redis_server = "localhost"
#r = redis.StrictRedis(redis_server, port=6379, db=0)
#
#
#def write_redis(k, val):
#    """write into redis"""
#    redis_db.set(k, val)

def main():
	spark = SparkSession \
	    .builder \
	    .appName("Twitchatter") \
	    .getOrCreate()
	# Create DataFrame representing the stream of input lines from connection to localhost:9999
	# Subscribe to 1 topic
	df = spark \
	  .readStream \
	  .format("kafka") \
	  .option("kafka.bootstrap.servers", config.ip_address) \
	  .option("subscribe", "my_topic") \
	  .load()

        #Kafka streams from source are as "key":"value"..etc.
	df.printSchema()
        
        #Select key:value and discard others
	ds=df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        print('key:value {}'.format(t['value']))
	
	## Split the value and make a dataframe with right column names 
	#t1 = t.select("value")
	#t1.printSchema()
	#split_col = functions.split(t['value'], ',')
	#t1 = t1.withColumn('SourcePN', split_col.getItem(0))
	#t1 = t1.withColumn('DestPN', split_col.getItem(1))
	#t1 = t1.withColumn('Location', split_col.getItem(2))
	#t1 = t1.withColumn('startT', split_col.getItem(3))
	#t1 = t1.withColumn('endT', split_col.getItem(4))
	#t1 = t1.withColumn('Type', split_col.getItem(5))
	#t1 = t1.withColumn('Company', split_col.getItem(6))
	#t1 = t1.drop('value')

        ##Start Counting per second !!
	#count_df=t1.groupBy("startT","Location").count()
	#count_df = count_df.withColumn('key',\
	#functions.concat(functions.col("startT"),functions.lit(','),functions.col("Location")))
	##Kafka sink reads key:value.Make dataframe kafka stream sink compatible 
        #count_df=count_df.selectExpr("key", "count as value")
	
        ##Send data to kafka sink 
	#query = count_df \
	#.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  	#.writeStream \
  	#.format("kafka") \
  	#.option("kafka.bootstrap.servers", "localhost:9092") \
  	#.option("topic", "my-topic") \
	#.option("checkpointLocation", "/home/ubuntu/insight-project-arnab/test/")\
	#.outputMode("complete") \
  	#.start()
	
        # write to cassandra 
        # https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/spark/structuredStreaming.html
        #query = fileStreamDf.writeStream\
        # .option("checkpointLocation", '/home/ubuntu/twitchatter/test/')\
        # .format("org.apache.spark.sql.cassandra")\
        # .option("keyspace", "analytics")\
        # .option("table", "test")\
        # .start()

	#query.awaitTermination()

if __name__ == '__main__':
    main()

