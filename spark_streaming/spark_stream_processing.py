"""spark streaming for"""

from __future__ import print_function

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark_cassandra import streaming
import pyspark_cassandra, sys
import json

from pyspark import StorageLevel

from pyspark.sql.functions import sum
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql import SQLContext 
from pyspark.sql import functions


#input data from kafka are video events logs_tuples, with video id as key
#(5, '{"user_id": 46423675, "event_type": "play", "timestamp": 6.233047650152182, "video_id": 5, "longitude": -113.17312289290828, "latitude": 22.513776675163744}')


# Spark Streaming micro batches
BATCH_INTERVAL  = 1 #unit second
WINDOW_LENGTH  = 30 * BATCH_INTERVAL #compute counts for last window_length
FREQUENCY  = 10 * BATCH_INTERVAL #count updated every frequency

sc = SparkContext(appName="sparkStreaming")
ssc = StreamingContext(sc, batch_length)
ssc.checkpoint("checkpoint")

# Specify all the nodes you are running Kafka on
kafkaBrokers = {"metadata.broker.list": "34.214.48.94:9092, 34.214.64.79:9092"}

# Get kafka topics -> TODO: split into multiple spark sessions, based on throughput per second
video_data = KafkaUtils.createDirectStream(ssc, 'video_topic', kafkaBrokers)
like_data = KafkaUtils.createDirectStream(ssc, 'like_topic', kafkaBrokers)
dislike_data = KafkaUtils.createDirectStream(ssc, 'dislike_topic', kafkaBrokers)
comment_data = KafkaUtils.createDirectStream(ssc, 'comment_topic', kafkaBrokers)
watch_data = KafkaUtils.createDirectStream(ssc, 'watch_topic', kafkaBrokers)


# For realtime map, stream processed, written to database
# layers of map, computed per video:
#     realtime no. of viewers -- last state count + new_play - new_leave
#     realtime no. of likes   -- last state likes + add(likes) in window
#     realtime no. of dislikes -- last state dislikes + add(dislikes) in window
#     realtime no. of comments -- las state comments + add(comments) in window
#     (optional) realtime no. of increased viewews -- simple add(new_play) - add(new_leave)
#     (optional) realtime no. of increased likes   -- simple add of likes in window
#     (optional) realtime no. of increased dislikes -- simple add of dislikes in window
#     (optional) realtime no. of increased comments -- simple add of comments in window


# realtime no. of viewers: 
def countUpdater(currentCount, countState):
    if countState is None:
        countState = 0
    return sum(currentCount, countState)

total_new_play = watch_data.map(lambda x: (x[0],1) if 'play' in x[1]).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, WINDOW_LENGTH, FREQUENCY)
total_new_leave = watch_data.map(lambda x: (x[0],1) if 'leave' in x[1]).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, WINDOW_LENGTH, FREQUENCY)

currentCount = total_new_start - total_new_end
total_video_watches = video_data.updateStateByKey(countUpdater)

total_video_watches.saveToCassandra("rate_data", "user_sum")


total_likes = like_data.map(lambda x: (x[0],1)).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, WINDOW_LENGTH, FREQUENCY)
total_dislikes = dislike_data.map(lambda x: (x[0],1)).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, WINDOW_LENGTH, FREQUENCY)
total_comment = comment_data.map(lambda x: (x[0],1)).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, WINDOW_LENGTH, FREQUENCY)





# elastic search within comments - TODO
ssc.start()




