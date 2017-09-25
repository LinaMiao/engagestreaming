from __future__ import print_function

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import pyspark_cassandra, sys
import json

from pyspark import StorageLevel

from pyspark.sql.functions import sum
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql import SQLContext
from pyspark.sql import functions



def nlpSentiment(comment):
    #use stanford nlp for sentiment analysis:



def main():
    #input data from kafka are video events logs_tuples
    #(5, '{"user_id": 46423675, "event_type": "play", "timestamp": 6.233047650152182, "video_id": 5, "longitude": -113.17312289290828, "latitude": 22.513776675163744}')


    # Spark Streaming micro batches
    BATCH_INTERVAL  = 1 #unit second
    WINDOW_LENGTH  = 3 * BATCH_INTERVAL #compute counts for last window_length
    FREQUENCY  = 3 * BATCH_INTERVAL #count updated every frequency

    sc = SparkContext(appName="sparkStreaming")
    ssc = StreamingContext(sc, WINDOW_LENGTH)
    ssc.checkpoint("checkpoint")

    val version = "3.6.0"
    val model = s"stanford-corenlp-$version-models" // append "-english" to use the full English model
    val jars = ssc.listJars in Spark 2.0
    if (!jars.exists(jar => jar.contains(model))) {
      import scala.sys.process._
      s"wget http://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/$version/$model.jar -O /tmp/$model.jar".!!
      ssc.addJar(s"/tmp/$model.jar")
    }

    # Specify all the nodes you are running Kafka on
    kafkaBrokers = {"metadata.broker.list": "54.213.76.65:9092, 54.149.138.179:9092"}

    # Get kafka topics
    like_data = KafkaUtils.createDirectStream(ssc, ['comment_topic'], kafkaBrokers)
    raw_like = like_data.map(lambda x: json.loads(x[1]))

    like_tuple = raw_like.map(lambda x: (x[0],1))

    total_likes = like_tuple.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, WINDOW_LENGTH, FREQUENCY)
    total_likes.pprint()

    #total_likes.saveToCassandra("a","b")
    total_likes.saveAsTextFiles("hdfs://ec2-54-201-180-66.us-west-2.compute.amazonaws.com:9000/user/like_data/")

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
