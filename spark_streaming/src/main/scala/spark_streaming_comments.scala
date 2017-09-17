import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.redis.RedisClient


import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._


object CommentStreaming {



  def main(args: Array[String]) {

    val brokers = "ec2-54-213-76-65.us-west-2.compute.amazonaws.com:9092"
    val topics = "comments_topic"
    val topicsSet = topics.split(",").toSet

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("comment_data")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    //ssc.checkpoint("./tmp")


    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val windowStream = messages.window(Seconds(10), Seconds(10))




    windowStream.foreachRDD { rdd =>

        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._

        val lines = rdd.map(_._2)
        val lines2DF = lines.map(_.split(","))
                            .map(t => (t(0).toString, t(1).toString)).toDF("id", "comment")
        //lines2DF.rdd.saveAsTextFile("hdfs://ec2-54-201-180-66.us-west-2.compute.amazonaws.com:9000/user/price_data_output_scala1")



        val sentiments = lines2DF
        .select('id, cleanxml('comment).as('doc))
        .select('id, explode(ssplit('doc)).as('sen))
        .select('id, sentiment('sen).as('sentiment).cast("float")).toDF("id", "sentiment")
        //.select('sen, tokenize('sen).as('words), ner('sen).as('nerTags), sentiment('sen).as('sentiment))
        //sentiments.show(5)

        //sentiments.groupBy("id").count().show()
        sentiments.groupBy("id")
          .agg(avg("sentiment"))
          .rdd.saveAsTextFile("hdfs://ec2-54-201-180-66.us-west-2.compute.amazonaws.com:9000/user/price_data_output_scala2")



        //val r = new RedisClient("52.34.86.155", 6379, secret=Option("127001"))
        //output.collect().foreach( t => {r.set(t(0), t(1).toString())})



                            }


    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}



/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}
