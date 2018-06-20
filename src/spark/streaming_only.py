import pyspark_cassandra
from pyspark_cassandra import streaming
from cassandra.cluster import Cluster

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

def storeToRedis(rdd):
        r = redis.Redis(host=config.redis_address, port=config.redis_port, db=config.redis_dbcount)
        for data in rdd.collect():
            r.set(data[0],data[1])

def main():
        sc = SparkContext(appName="Twitchatter")
        sc.setLogLevel('ERROR')

        ssc = StreamingContext(sc, 3) # every 3 seconds
        # set checkpoint directory:use default fs protocol in core-site.xml
        ssc.checkpoint("hdfs://"+config.spark_ckpt)
        print("hdfs://"+config.spark_ckpt)

        zkQuorum = [config.zk_address]
        #zkQuorum = ','.join([ip+":2181" for ip in kafka_cluster])
        topic = [config.topic]
        print("{}{}".format(zkQuorum,topic))
       
        partition = 0
        start = 0
        topicpartition = TopicAndPartition(topic[0],partition)
        
        kvs = KafkaUtils.createDirectStream(ssc,topic,{"metadata.broker.list": config.ip_address},
                fromOffsets={topicpartition: int(start)})
        #kvs = KafkaUtils.createDirectStream(ssc,topic,{"metadata.broker.list": config.ip_address})
        #kvs.pprint()
        kvs.checkpoint(600)
        
        parsed = kvs.map(lambda v: json.loads(v[1]))
	#parsed.pprint()

        def updateTotalCount(currentState,countState):
            if countState is None:
                countState = 0
            return sum(currentState,countState)

        #msg_counts = parsed.map(lambda v: (v[u'channel'],1)).reduceByKey(lambda x,y: x+y)
        #msg_counts.pprint()

        total_msg_counts = parsed.map(lambda v: (v[u'channel'],1)).updateStateByKey(updateTotalCount)
        print(total_msg_counts)
        total_msg_counts.pprint()

        #total_user_counts = parsed.map(lambda v: (v[u'username'],1)).updateStateByKey(updateTotalCount)
        #total_user_counts.pprint()

        # 1) dump data to redis
        total_msg_counts.foreachRDD(storeToRedis)

        #total_msg_counts.foreachRDD(lambda rdd: rdd.foreachPartition(storeToRedis))
        #parsed.foreachRDD(lambda rdd: rdd.foreachPartition(storeToRedis))

        # 2) dump data to cassandra
        time_channel_user = parsed.map(lambda v: {"timestamp":v['time'],"channel":v[u'channel'],"username":v[u'username']})

        # connect to cassandra cluster
        cluster = Cluster([config.cass_seedip])
        session = cluster.connect()

        ## create and set cassandra keyspace to work
        # only once. for the future, set check existence conditions 
        #session.execute("CREATE KEYSPACE "+ config.cass_keyspace +" WITH replication = {'class':                 'SimpleStrategy', 'replication_factor': '3'};")
        #session.set_keyspace(config.cass_keyspace)

        ## create tables to insert data
        #session.execute("CREATE TABLE time_channel_user (timestamp text, channel text, username text, primary    key(timestamp,username));")

        time_channel_user.saveToCassandra(config.cass_keyspace,"time_channel_user")



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

