#from pyspark_cassandra 
#from pyspark_cassandra import streaming
#from cassandra.cluster import Cluster

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import TopicAndPartition

import json
import time
import redis
import config

#def track_trends(rdditer):
#        redis_session = redis.Redis(host=config.redis_address, port=config.redis_port, db=config.dbcount)
#        with redis_session.pipeline() as pipe:
#             for data in rdditer:
#                 pipe.zincrby("trends", str(data[2]), 1)
#                 pipe.execute()

def update(rdditer):
        r = redis.Redis(host=config.redis_address, port=config.redis_port, db=config.dbcount)
        for data in rdditer:
            r.set('none',data)

def main():
        sc = SparkContext(appName="Twitchatter")
        sc.setLogLevel('ERROR')

        ssc = StreamingContext(sc, 3) # every 3 seconds
        ssc.checkpoint(config.spark_ckpt)
        #ssc.checkpoint("file:///home/ubuntu/efs/")

        zkQuorum = [config.zk_address]
        #zkQuorum = ','.join([ip+":2181" for ip in kafka_cluster])
        topic = [config.topic]
        print("{}{}".format(zkQuorum,topic))
       
        partition = 0
        start = 0
        topicpartition = TopicAndPartition(topic[0],partition)
        
        #kvs = KafkaUtils.createDirectStream(ssc,topic,{"metadata.broker.list": config.ip_address},
        #        fromOffsets={topicpartition: int(start)})
        kvs = KafkaUtils.createDirectStream(ssc,topic,{"metadata.broker.list": config.ip_address})
        #kvs.pprint()
        
        parsed = kvs.map(lambda v: json.loads(v[1]))
	parsed.pprint()
        #parsed.foreachRDD(lambda rdd: rdd.foreachPartition(update))
        def updateTotalCount(currentState,countState):
            if countState is None:
                countState = 0
            return sum(currentState,countState)

        msg_counts = parsed.map(lambda v: (v[u'channel'],1)).reduceByKey(lambda x,y: x+y)
        msg_counts.pprint()
        total_msg_counts = parsed.map(lambda v: (v[u'channel'],1)).updateStateByKey(updateTotalCount)
        total_msg_counts.pprint()

        #user_counts = parsed.map(lambda v: (v[u'username'],1)).reduceByKey(lambda x,y: x+y)
        #user_counts.pprint()




        ##counts = lines.map(lambda line: line.split(";")) \
        ##.map(lambda y: (y[2],int(y[3]))) \
        ##.reduceByKey(lambda a, b: a+b)

        ##counts.foreachRDD(storeToRedis)

        ssc.start()
        ssc.awaitTermination()

        ##Kafka streams from source are as "key":"value"..etc.
	#df.printSchema()
        #
        ##Select key:value and discard others
        ##value schema: {"username":"xxx","message":"xxx","channel":"xxx","time":"xxx"}
        #schema = StructType().add("username",StringType()).add("message",StringType()).add("channel",StringType()).add("time",StringType())
        #ds = df.selectExpr("CAST(value AS STRING)") \
        #       .select(f.from_json("value",schema).alias("message")) \
        #       .select("message.*")
        #ds.printSchema()
        ## uncomment to see data flowing in
        ##query = ds.writeStream.outputMode("append").format("console").start()
        ##query.awaitTermination()

        ## write is not available for streaming data; we use create_table to create table and keyspaces
        ##ds.write.format("org.apache.spark.sql.cassandra").options(table="rawtable",keyspace="test").save(mode="append")
        #
        ## again structured stream does not provide savetocassandra functionality
        ##ds.saveToCassandra("test","rawtable")
        #
        ## dump data to parquet files
        #query = ds.writeStream \
        #  .format("parquet") \
        #  .option("startingOffsets", "earliest") \
        #  .option("checkpointLocation", "/home/ubuntu/twitchatter/test/check/") \
        #  .option("path", "/home/ubuntu/Downloads") \
        #  .start()
        #query.awaitTermination()
        #	
        ## Do some simple count: unique user count per channel
        #user_count = ds.groupBy("channel","username").count()
        ##user_count.write.format("org.apache.spark.sql.cassandra").options(table="protable",keyspace="test").save(mode="append")
        ##user_count.saveToCassandra("test","protable")

        ## write to cassandra 
        ## https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/spark/structuredStreaming.html
        ##query = user_count.writeStream\
        ## .option("checkpointLocation", '/home/ubuntu/twitchatter/test/')\
        ## .format("org.apache.spark.sql.cassandra")\
        ## .option("keyspace", "analytics")\
        ## .option("table", "test")\
        ## .start()


if __name__ == '__main__':
    #r = redis.StrictRedis(host=config.redis_address, port=config.redis_port, db=0)
    #r.set('foo', 'bar')
    #r.get('foo')
    main()

