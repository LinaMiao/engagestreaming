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


object EngageStreaming {



  def main(args: Array[String]) {

    val brokers = "ec2-54-213-76-65.us-west-2.compute.amazonaws.com:9092"
    val topics_1 = "comments_topic"
    val topicsSet_1 = topics_1.split(",").toSet
    val topics_2 =  "dislike_topic"
    val topicsSet_2 = topics_2.split(",").toSet

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("engage")
    sparkConf.set("spark.streaming.concurrentJobs", "2")

    val ssc = new StreamingContext(sparkConf, Seconds(10))
    //ssc.checkpoint("./tmp")


    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages_1 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet_1)
    val windowStream_1 = messages_1.window(Seconds(10), Seconds(10))

    windowStream_1.foreachRDD { rdd =>

        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._

        val lines = rdd.map(_._2)
        val lines2DF = lines.map(_.split(","))
                            .map(t => (t(0).toString, t(1).toString)).toDF("id", "comment")
        //lines2DF.rdd.saveAsTextFile("hdfs://ec2-54-201-180-66.us-west-2.compute.amazonaws.com:9000/user/price_data_output_scala1")


        lines2DF.groupBy("id")
                .count()
                .toDF("id","count1")
                .show(5)

        //val sentiments = lines2DF
        //.select('id, cleanxml('comment).as('doc))
        //.select('id, explode(ssplit('doc)).as('sen))
        //.select('id, sentiment('sen).as('sentiment).cast("float")).toDF("id", "sentiment")


        //.select('sen, tokenize('sen).as('words), ner('sen).as('nerTags), sentiment('sen).as('sentiment))



        //sentiments.groupBy("id")
        //          .agg(avg("sentiment"))
        //          .show(5)

        //  .rdd.saveAsTextFile("hdfs://ec2-54-201-180-66.us-west-2.compute.amazonaws.com:9000/user/price_data_output_scala1")



        //val r = new RedisClient("52.34.86.155", 6379, secret=Option("127001"))
        //output.collect().foreach( t => {r.set(t(0), t(1).toString())})

                            }
      val messages_2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet_1)
      val windowStream_2 = messages_2.window(Seconds(10), Seconds(10))

      windowStream_2.foreachRDD { rdd =>

          val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
          import sqlContext.implicits._

          val lines = rdd.map(_._2)
          //lines.saveAsTextFile("hdfs://ec2-54-201-180-66.us-west-2.compute.amazonaws.com:9000/user/price_data_output_scala")


          val dislike = lines.map(_.split(","))
                             .map(t => (t(0).toString))
                             .toDF("id")
                             .groupBy("id")
                             .count()
                             .toDF("id","count2")

          dislike.show(5)

          //dislike.rdd.saveAsTextFile("hdfs://ec2-54-201-180-66.us-west-2.compute.amazonaws.com:9000/user/price_data_output_scala2")




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
