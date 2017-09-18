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
    val topics_2 =  "like_topic"
    val topicsSet_2 = topics_2.split(",").toSet
    val topics_3 = "dislike_topic"
    val topicsSet_3 = topics_1.split(",").toSet
    val topics_4 =  "video_topic"
    val topicsSet_4 = topics_2.split(",").toSet
    val topics_5 =  "watch_topic"
    val topicsSet_5 = topics_2.split(",").toSet


    // Create context with 10 second batch interval
    val sparkConf = new SparkConf().setAppName("engage")
    sparkConf.set("spark.streaming.concurrentJobs", "3")

    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("./tmp")


    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)


    val messages_1 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet_1)
    val windowStream_1 = messages_1.window(Seconds(10), Seconds(10))





    windowStream_1.foreachRDD { rdd =>

        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._

        val lines = rdd.map(_._2)
        val lines2DF = lines.map(_.split(","))
                            .map(t => (t(0).toString, t(1).toString)).toDF("comments_id", "comment")

        val comment_counts = lines2DF.groupBy("comments_id")
                .count()
                .toDF("comments_id","count1")


        //val sentiments = lines2DF
        //.select('id, cleanxml('comment).as('doc))
        //.select('id, explode(ssplit('doc)).as('sen))
        //.select('id, sentiment('sen).as('sentiment).cast("float")).toDF("id", "sentiment")


        //.select('sen, tokenize('sen).as('words), ner('sen).as('nerTags), sentiment('sen).as('sentiment))



        //sentiments.groupBy("id")
        //          .agg(avg("sentiment"))
        //          .show(5)

        //  .rdd.saveAsTextFile("hdfs://ec2-54-201-180-66.us-west-2.compute.amazonaws.com:9000/user/price_data_output_scala1")



        val r = new RedisClient("54.201.180.66", 6379, secret=Option("127001"))
        comment_counts.collect().foreach( t => {r.set(t(0), t(1).toString())})

                            }

      // like_topic
      val messages_2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet_2)
      val windowStream_2 = messages_2.window(Seconds(10), Seconds(10))

      val initialRDD = ssc.sparkContext.parallelize(List(("0", 0), ("1", 0)))
      val mappingFunc = (id: String, one: Option[Int], state:State[Int]) => {
        val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
        val output = (id, sum)
        state.update(sum)
        output
      }

      val stateCount = windowStream_2.map(_._2).map(_.split(",")).map(t => (t(0).toString,1)).mapWithState(
        StateSpec.function(mappingFunc).initialState(initialRDD))
      stateCount.print()




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
