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
import scala.util.Random
import scala.io.Source
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._


object EngageStreaming {

  def main(args: Array[String]) {
    // read cluster information from file
    val clusterInfo = Source.fromFile("clusterInfo.txt").getLines.toList
    val kafkaBrokers = clusterInfo(0)
    val sparkCheckpoint = clusterInfo(1)
    val redisBrokers = clusterInfo.(2)
    val redisPort = clusterInfo(3)
    val redisPassword = clusterInfo(4)

    // topics from kafka
    val topics = ("comments_topic", "like_topic", "dislike_topic", "start_topic", "end_topic", "play_topic","leave_topic")
    val topicsSet_1 = Set(topics._1)
    val topicsSet_2 = Set(topics._2)
    val topicsSet_3 = Set(topics._3)
    val topicsSet_4 = Set(topics._4)
    val topicsSet_5 = Set(topics._5)
    val topicsSet_6 = Set(topics._6)
    val topicsSet_7 = Set(topics._7)

    // spark setups
    val sparkConf = new SparkConf().setAppName("engage")
    sparkConf.set("spark.streaming.concurrentJobs", "18")
    sparkConf.set("spark.sql.shuffle.partitions", "400")
    sparkConf.set("spark.worker.cleanup.enabled", "True")
    sparkConf.set("spark.driver.memory", "2g")
    sparkConf.set("spark.executor.memory", "2g")

    // spark context with 10s microbatch
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint(sparkCheckpoint)
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokers)


    // Dstream for topic_1 - comments, with sliding window 60s, 60s
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
                     .filter(not($"sen" === " "))


        // let's tokenize everything
        val token = nlp.select('id, tokenize('sen).as('tokens))
                        .groupBy("id")
                        .agg(collect_list("tokens").as('tokens_collect))

        val r1 = new RedisClient(redisBrokers, redisPort, database=1, secret=Option(redisPassword))
        // update database only if content is not empty
        token.collect().foreach( t => {if(t(1).toString().length > 15) // t(1) comes as as string likes "wrappedarray()", len(wrappedarray()) == 15
                                          r1.set(t(0) + "_token", t(1))})


        val tokenCount = rdd.map(_._2)
                            .map(_.split("\\{"))
                            .map(t => (t(0).toString(), t(1).toString().split(" ").length))
                            .toDF("id", "len")
                            .select(col("len"))
                            .map(_(0).asInstanceOf[Int])
                            .reduce(_+_)

        val prob:Double = if(tokenCount> 5000) 5000.0/tokenCount else 1

        val sent_score = nlp.map(row => {val rand = new scala.util.Random
                                        (row.getString(0),row.getString(1),rand.nextDouble())})
                            .toDF("id","sen","rand")
                            .filter(($"rand" >= (1.0-prob)))
                            .select('id, sentiment('sen).as('sentiments), 'rand)
                            .groupBy("id")
                            .agg(collect_list("sentiments").as('sentiments_collect))


        sent_score.collect().foreach( t => {if(t(1).toString().length > 15)
                                              r1.set(t(0) + "_sentiment", t(1))})


                           }


      // DStream for topic2, likes, with sliding window 10, 10
      val messages_2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet_2)
      val windowStream_2 = messages_1.window(Seconds(10), Seconds(10))

      windowStream_2.foreachRDD { rdd =>

          val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
          import sqlContext.implicits._

          val likesCount  = rdd.map(_._2)
                               .map(_.split(","))
                               .map(t => (t(0).toString().stripPrefix("[").toString(), 1))
                               .toDF("id","likes")
                               .groupBy("id")
                               .count()

          val r2 = new RedisClient(redisBrokers, redisPort,database=2,secret=Option(redisPassword))
          likesCount.collect().foreach( t => {r2.set(t(0), t(1).toString())})
          //System.gc()

                              }

      // DStream for topic3, dislikes, with sliding window 10, 10
      val messages_3 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet_3)
      val windowStream_3 = messages_3.window(Seconds(10), Seconds(10))

      windowStream_3.foreachRDD { rdd =>

          val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
          import sqlContext.implicits._

          val dislikesCount = rdd.map(_._2)
                                 .map(_.split(","))
                                 .map(t => (t(0).toString().stripPrefix("[").toString(), 1))
                                 .toDF("id","dislikes")
                                 .groupBy("id")
                                 .count()


          val r3 = new RedisClient(redisBrokers, redisPort,database=3,secret=Option(redisPassword))
          dislikesCount.collect().foreach( t => {r3.set(t(0), t(1).toString())})
          //System.gc()

                              }
    //
      // DStream for topic4, starts, with sliding window 10, 10
      val messages_4 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet_4)
      val windowStream_4 = messages_4.window(Seconds(10), Seconds(10))

      windowStream_4.foreachRDD { rdd =>

          val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
          import sqlContext.implicits._

          val starts = rdd.map(_._2)
                          .map(_.split(","))
                          .map(t => (t(0).toString().stripPrefix("[").toString(), t(0).toString() + t(1).toString() + t(2).toString() + t(3).toString() + t(4).toString() + t(5).toString() + t(6).toString()))
                          .toDF("id","content")



          val r4 = new RedisClient(redisBrokers, redisPort,database=4,secret=Option(redisPassword))
          starts.collect().foreach( t => {r4.set(t(0), t(1))})
          //starts.show()
          //System.gc()

                              }


    // DStream for topic5, ends, with sliding window 10, 10
    val messages_5 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet_5)
    val windowStream_5 = messages_5.window(Seconds(10), Seconds(10))

    windowStream_5.foreachRDD { rdd =>

        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._

        val endsCount = rdd.map(_._2)
                           .map(_.split(","))
                           .map(t => (t(0).toString().stripPrefix("[").toString(), t(0).toString() + t(1).toString() + t(2).toString() + t(3).toString() + t(4).toString() + t(5).toString() + t(6).toString()))
                           .toDF()



        val r5 = new RedisClient(redisBrokers, redisPort,database=5,secret=Option(redisPassword))
        endsCount.collect().foreach( t => {r5.set(t(0), t(1))})
        //System.gc()

                            }

      // DStream for topic6, plays, with sliding window 10, 10
      val messages_6 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet_6)
      //val windowStream_6 = messages_6.window(Seconds(10), Seconds(10))

      val initialRDD_6 = ssc.sparkContext.parallelize(List(("0", 0), ("1", 0)))
      val mappingFunc_6 = (id6: String, one6: Option[Int], state6:State[Int]) => {
        val sum6 = one6.getOrElse(0) + state6.getOption.getOrElse(0)
        val output6 = (id6, sum6)
        state6.update(sum6)
        output6
      }

      val playStateCount = messages_6.map(_._2).map(_.split(",")).map(t => (t(0).toString(),1)).mapWithState(
        StateSpec.function(mappingFunc_6).initialState(initialRDD_6))


      playStateCount.foreachRDD { rdd =>
          val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
          import sqlContext.implicits._

          val playCount = rdd.toDF("id","playCount")

          val r6 = new RedisClient(redisBrokers, redisPort,database=6,secret=Option(redisPassword))
          playCount.collect().foreach( t => {r6.set(t(0).toString().stripPrefix("[").toString(), t(1))})


                             }
    //
    //
    //  DStream for topic7, leaves, with sliding window 10, 10
      val messages_7 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet_7)
      val windowStream_7 = messages_7.window(Seconds(10), Seconds(10))

      val initialRDD_7 = ssc.sparkContext.parallelize(List(("0", 0), ("1", 0)))
      val mappingFunc_7 = (id7: String, one7: Option[Int], state7:State[Int]) => {
        val sum7 = one7.getOrElse(0) + state7.getOption.getOrElse(0)
        val output7 = (id7, sum7)
        state7.update(sum7)
        output7
      }

      val leaveStateCount = windowStream_7.map(_._2).map(_.split(",")).map(t => (t(0).toString(),1)).mapWithState(
        StateSpec.function(mappingFunc_7).initialState(initialRDD_7))

      leaveStateCount.foreachRDD { rdd =>
          val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
          import sqlContext.implicits._

          val leaveCount = rdd.toDF("id","leaveCount")

          val r7 = new RedisClient(redisBrokers, redisPort, database=7,secret=Option(redisPassword))
          leaveCount.collect().foreach( t => {r7.set(t(0).toString().stripPrefix("[").toString(), t(1))})

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
