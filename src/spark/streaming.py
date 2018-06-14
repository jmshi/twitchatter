import pyspark_cassandra
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.streaming import StreamingContext
#from pyspark.sql.types import StructType
from pyspark.sql.types import *
from pyspark.sql import functions as f
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
          .option("startingOffsets","earliest") \
	  .load()

        #Kafka streams from source are as "key":"value"..etc.
	df.printSchema()
        
        #Select key:value and discard others
        # value schema: {"username":"xxx","message":"xxx","channel":"xxx","time":"xxx"}
        schema = StructType().add("username",StringType()).add("message",StringType()).add("channel",StringType()).add("time",StringType())
        ds = df.selectExpr("CAST(value AS STRING)") \
               .select(f.from_json("value",schema).alias("message")) \
               .select("message.*")
        ds.printSchema()
        query = ds.writeStream.outputMode("append").format("console").start()
        query.awaitTermination()
        	
        # Do some simple count: unique user count per channel
        user_count = ds.groupBy("channel","username").count()
	
        # write to cassandra 
        # https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/spark/structuredStreaming.html
        #query = user_count.writeStream\
        # .option("checkpointLocation", '/home/ubuntu/twitchatter/test/')\
        # .format("org.apache.spark.sql.cassandra")\
        # .option("keyspace", "analytics")\
        # .option("table", "test")\
        # .start()

        #
        #query = user_count.writeStream.foreach(x=> println(x).format("console").start()

        query.awaitTermination()

if __name__ == '__main__':
    main()

