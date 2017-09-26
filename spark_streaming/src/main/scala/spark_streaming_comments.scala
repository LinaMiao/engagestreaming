// multli-stream processing of multiple topics, window aggregation, stateful counting, and sentiment analysis with
// stanford-corenlp
// topic_1: comment_topic (window NLP sentiment) -> redis db1 (video_id, avg(sentiment) in sliding window)
// topic_2: like_topic (window aggregation) -> redis db2 (video_id, count(likes) in sliding window)
// topic_3: dislike_topic (window_aggregation) -> redis db3 (vide_id, count(dislikes) in sliding window)
// topic_4: start_topic (bookkeeping streaming/geohash) -> redis db4 (video_id, lat, long, geohash (TODO-FOR RANGE QUERIES)) ** broadcaster event
// topic_5: end_topic (bookkeeping streaming/geohash) -> redis db5 (video_id, lat, long, geohash (TODO-FOR RANGE QUERIES)) ** broadcaster event
// topic_6: play_topic (stateful counting, geophashing(TODO)) -> redis db6 (video_id, state_count(start))
// topic_7: leave_topic (stateful counting, geohasing(TODO)) -> redis db7 (video_id, state_count(leave))

import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.redis.RedisClient
import org.apache.spark.sql.SQLContext._


import java.util.Properties

import scala.collection.JavaConverters._
import scala.util.parsing.json._
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._


object EngageStreaming {

  def main(args: Array[String]) {
    // topics from kafka
    val brokers = "ec2-54-245-21-146.us-west-2.compute.amazonaws.com:9092"
    val topics_1 = "comments_topic"
    val topicsSet_1 = topics_1.split(",").toSet
    val topics_2 =  "like_topic"
    val topicsSet_2 = topics_2.split(",").toSet
    val topics_3 = "dislike_topic"
    val topicsSet_3 = topics_3.split(",").toSet
    val topics_4 =  "start_topic"
    val topicsSet_4 = topics_4.split(",").toSet
    val topics_5 =  "end_topic"
    val topicsSet_5 = topics_5.split(",").toSet
    val topics_6 =  "play_topic"
    val topicsSet_6 = topics_6.split(",").toSet
    val topics_7 =  "leave_topic"
    val topicsSet_7 = topics_7.split(",").toSet

    // Create context with 10 second batch interval
    val sparkConf = new SparkConf().setAppName("engage")
    sparkConf.set("spark.streaming.concurrentJobs", "16")
    sparkConf.set("spark.sql.shuffle.partitions", "400")
    sparkConf.set("spark.worker.cleanup.enabled", "True")
    sparkConf.set("spark.driver.memory", "2g")
    sparkConf.set("spark.executor.memory", "2g")


    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("hdfs://ec2-54-245-160-86.us-west-2.compute.amazonaws.com:9000/tmp/spark_checkpoint")

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    // Dstream for topic_1 - comments, with sliding window 30, 30
    val messages_1 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet_1)
    val windowStream_1 = messages_1.window(Seconds(60), Seconds(60))

    windowStream_1.foreachRDD { rdd =>

        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._

        val nlp = rdd.map(_._2)
                            .map(_.split("\\{"))
                            .map(t => (t(0).toString().stripPrefix("[").dropRight(3).toString(), t(1).toString().split("user_id")(0)))
                            .toDF("id", "comment_raw")
                            .select('id, regexp_replace($"comment_raw", "[^A-Za-z?!.]+", " ").as('comment))
                            .filter(not($"comment" === " "))
                            .select('id, explode(ssplit('comment)).as('sen))
                            //.select('id, 'comment.as('sen))
                            // .select('id, tokenize('sen).as('tokens), sentiment('sen).as('sentiments))
                            // .toDF("id", "tokens", "sentiments")
                            // .groupBy("id")
                            // .agg(collect_list("tokens"), collect_list("sentiments"))

        // let's tokenize everything
        val token = nlp.select('id, tokenize('sen).as('tokens))
                        .groupBy("id")
                        .agg(collect_list("tokens").as('tokens_collect))


        // let's randomly choose 1000 sentences per minute for sentiment analysis
        val senCount = nlp.count()
        val prob:Double = if(senCount>1000) 1000.0/senCount else 1

        val sent_score = nlp.filter(($"id"*0 + scala.util.Random.nextFloat) > (1-prob))
                            .select('id, sentiment('sen).as('sentiments))
                            .groupBy("id")
                            .agg(collect_list("sentiments").as('sentiments_collect))
 


        //.select('sen, tokenize('sen).as('words), ner('sen).as('nerTags), sentiment('sen).as('sentiment)) -> potential extension

        val r1 = new RedisClient("54.245.160.86", 6379, database=1, secret=Option("127001"))
        // update database only if content is not empty
        if(token.count() > 0)
          token.collect().foreach( t => {r1.set(t(0) + "_token", t(1))})
        if(sent_score.count() > 0)
          sent_score.collect().foreach( t => {r1.set(t(0) + "_sentiment", t(1))})


        //System.gc()
        //sentiments.show()

        //redis for each partition
        // rdd.foreachPartition { p =>
        //   val r1 = new RedisClient("54.245.160.86", 6379, database=1, secret=Option("127001"))
        //     p.foreach(t => {r1.set(t(0), t(1).toString())} )
        // }

                           }
    //
    //   // DStream for topic2, likes, with sliding window 10, 10
    //   val messages_2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet_2)
    //   val windowStream_2 = messages_1.window(Seconds(10), Seconds(10))
    //
    //   windowStream_2.foreachRDD { rdd =>
    //
    //       val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    //       import sqlContext.implicits._
    //
    //       val likesCount  = rdd.map(_._2)
    //                           .map(_.split(","))
    //                           .map(t => (t(0).toString().stripPrefix("[").toString(), 1))
    //                           .toDF("id","likes")
    //                           .groupBy("id")
    //                           .count()
    //
    //       val r2 = new RedisClient("54.245.160.86", 6379,database=2,secret=Option("127001"))
    //       likesCount.collect().foreach( t => {r2.set(t(0), t(1).toString())})
    //       //System.gc()
    //                           }
    //
    //   // DStream for topic3, dislikes, with sliding window 10, 10
    //   val messages_3 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet_3)
    //   val windowStream_3 = messages_3.window(Seconds(10), Seconds(10))
    //
    //   windowStream_3.foreachRDD { rdd =>
    //
    //       val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    //       import sqlContext.implicits._
    //
    //       val dislikesCount = rdd.map(_._2)
    //                           .map(_.split(","))
    //                           .map(t => (t(0).toString().stripPrefix("[").toString(), 1))
    //                           .toDF("id","dislikes")
    //                           .groupBy("id")
    //                           .count()
    //
    //
    //       val r3 = new RedisClient("54.245.160.86", 6379,database=3,secret=Option("127001"))
    //       dislikesCount.collect().foreach( t => {r3.set(t(0), t(1).toString())})
    //       //System.gc()
    //                           }
    // //
    //   // DStream for topic4, starts, with sliding window 10, 10
      // val messages_4 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet_4)
      // val windowStream_4 = messages_4.window(Seconds(10), Seconds(10))
      //
      // windowStream_4.foreachRDD { rdd =>
      //
      //     val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      //     import sqlContext.implicits._
      //
      //     val starts = rdd.map(_._2)
      //                         .map(_.split(","))
      //                         .map(t => (t(0).toString().stripPrefix("[").toString(), t(0).toString() + t(1).toString() + t(2).toString() + t(3).toString() + t(4).toString() + t(5).toString() + t(6).toString()))
      //                         .toDF("id","content")
      //
      //
      //
      //     val r4 = new RedisClient("54.245.160.86", 6379,database=4,secret=Option("127001"))
      //     starts.collect().foreach( t => {r4.set(t(0), t(1))})
      //     //starts.show()
      //     //System.gc()
      //                         }
    // //
    // // DStream for topic5, ends, with sliding window 10, 10
    // val messages_5 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet_5)
    // val windowStream_5 = messages_5.window(Seconds(10), Seconds(10))
    //
    // windowStream_5.foreachRDD { rdd =>
    //
    //     val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    //     import sqlContext.implicits._
    //
    //     val endsCount = rdd.map(_._2)
    //                         .map(_.split(","))
    //                         .map(t => (t(0).toString().stripPrefix("[").toString(), t(0).toString() + t(1).toString() + t(2).toString() + t(3).toString() + t(4).toString() + t(5).toString() + t(6).toString()))
    //                         .toDF()
    //
    //
    //
    //     val r5 = new RedisClient("54.245.160.86", 6379,database=5,secret=Option("127001"))
    //     endsCount.collect().foreach( t => {r5.set(t(0), t(1))})
    //     //System.gc()
    //                         }
    //
      // // DStream for topic6, plays, with sliding window 10, 10
      // val messages_6 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet_6)
      // val windowStream_6 = messages_6.window(Seconds(10), Seconds(10))
      //
      // val initialRDD_6 = ssc.sparkContext.parallelize(List(("0", 0), ("1", 0)))
      // val mappingFunc_6 = (id6: String, one6: Option[Int], state6:State[Int]) => {
      //   val sum6 = one6.getOrElse(0) + state6.getOption.getOrElse(0)
      //   val output6 = (id6, sum6)
      //   state6.update(sum6)
      //   output6
      // }
      //
      // val playStateCount = windowStream_6.map(_._2).map(_.split(",")).map(t => (t(0).toString(),1)).mapWithState(
      //   StateSpec.function(mappingFunc_6).initialState(initialRDD_6))
      //
      // playStateCount.foreachRDD { rdd =>
      //     val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      //     import sqlContext.implicits._
      //
      //     val playCount = rdd.toDF("id","playCount")
      //                         .groupBy("id")
      //                         .count()
      //
      //     val r6 = new RedisClient("54.245.160.86", 6379,database=6,secret=Option("127001"))
      //     playCount.collect().foreach( t => {r6.set(t(0).toString().stripPrefix("[").toString(), t(1))})
      //
      //                         }
    // //
    // //
      // DStream for topic7, leaves, with sliding window 10, 10
      // val messages_7 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet_7)
      // val windowStream_7 = messages_7.window(Seconds(10), Seconds(10))
      //
      // val initialRDD_7 = ssc.sparkContext.parallelize(List(("0", 0), ("1", 0)))
      // val mappingFunc_7 = (id7: String, one7: Option[Int], state7:State[Int]) => {
      //   val sum7 = one7.getOrElse(0) + state7.getOption.getOrElse(0)
      //   val output7 = (id7, sum7)
      //   state7.update(sum7)
      //   output7
      // }
      //
      // val leaveStateCount = windowStream_7.map(_._2).map(_.split(",")).map(t => (t(0).toString(),1)).mapWithState(
      //   StateSpec.function(mappingFunc_7).initialState(initialRDD_7))
      //
      // leaveStateCount.foreachRDD { rdd =>
      //     val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      //     import sqlContext.implicits._
      //
      //     val leaveCount = rdd.toDF("id","leaveCount")
      //                         .groupBy("id")
      //                         .count()
      //
      //     val r7 = new RedisClient("54.245.160.86", 6379, database=7,secret=Option("127001"))
      //     leaveCount.collect().foreach( t => {r7.set(t(0).toString().stripPrefix("[").toString(), t(1))})
      //
      //                           }



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
