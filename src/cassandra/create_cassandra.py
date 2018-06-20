import config
from cassandra.cluster import Cluster

# Connect to cassandra cluster
cluster = Cluster([config.cass_seedip])
session = cluster.connect()

# Create and set cassandra keyspace to work
session.execute("CREATE KEYSPACE"+ config.cass_keyspace +"WITH replication = {'class':" + config.cass_class+", 'replication_factor':"+config.cass_rf+"};")
session.set_keyspace(config.cass_keyspace)

# Create tables to insert data
session.execute("CREATE TABLE user_song_log (timestamp int, user_id int, song_id int, primary key(timestamp, user_id));")


#session.execute("CREATE TABLE user_to_song (user_id int, req_time int, song_id int,  primary key (user_id, req_time)) WITH CLUSTERING ORDER BY (req_time DESC);")
#session.execute("CREATE TABLE song_to_user (song_id int, req_time int, user_id int,  primary key (song_id, req_time, user_id)) WITH CLUSTERING ORDER BY (req_time DESC);")
#session.execute("CREATE TABLE user_connections (user_id int, follows_id int, relevance_score float, primary key(user_id, follows_id));")
#session.execute("CREATE TABLE user_relevance (user_id int, timestamp int, suggested_user_id int, relevance_score float, primary key (user_id, timestamp, suggested_user_id)) WITH CLUSTERING ORDER BY (timestamp DESC);")
#session.execute("CREATE TABLE frequent_song_pairs (song_id int, freq_song_id int, frequency int, primary key (song_id, freq_song_id));")

# Close the connection
#session.shutdown()
#cluster.shutdown()

