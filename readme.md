# Engage
## Introduction
Engage is built with the ultimate goal as improving audiences' engagement to on line live videos. Recent years we observe a boom of online live videos, according to [blog](https://www.go-globe.com/blog/live-streaming-statistics/), we would expect 82% of internet traffic goes to streaming videos, together with huge potentials. Bright facts being said, I would like to mention some current unfortunate facts. Significant number of viewers tend to leave a live video within short time, and reluctantly to actively react to videos (like, dislike, especially making comments). One approach I propose to improve the broadcasters' and audiences' engagement is providing real-time feedback with use of system logs. Say a broadcaster is able to know clearly what his/her audiences talks about in the last minute, what do they care most about, how do they think of my tone/speed; an audience is able to know finally his/her comments are being collected to show to the broadcaster and all the other audiences, so every word and reaction count not flushed away.   

## Data pipeline
![pipeline_image](https://github.com/LinaMiao/engagestreaming/blob/master/images/pipeline_image.png)
Data was randomly simulated log like events, with the logic as random broadcaster start a video at random time with random duration, and at random time of a video, random user is going to join and leave the video with random watch time duration which generating a watch session; within a watch session, a viewer would react to the video(like/dislike/comment) at random time with random possibility (see file [sim.py](https://github.com/LinaMiao/engagestreaming/blob/master/simulation/sim.py) for details), logs come as Json format.

Apache Kafka is chosen for its scalability and fault tolerant as ingestion tool. Topics from Kafka are consumed both with spark streaming for real time analysis and with [pinterest secor](https://github.com/pinterest/secor) writing to AWS S3 to create the source of truth and source of any batch processing requirements. At least once semantics are chosen here, given the consideration that some duplications are tolerant-able in the use case, but drop of events like "leave" or "end" would be more disastrous, exactly once would be too much of an over expensive choice. A duplication scheme would certainly a next step urgent on the AWS S3 data.

Apache Spark stream processing is chosen for its  Real time analytics chosen to implement currently include:
1) NLP analysis, analyze the current most popular words and sentiment score of a video's comments, using [Stanford CoreNLP wrapper for Apache Spark](https://github.com/databricks/spark-corenlp), with choice of 1 minute sliding window. An adaptive sampling filter was implemented as an defensive approach for any unexpected large traffic volume of comments, that is large than the capacity of 1 minute processing power of the most expensive processing: NLP sentiment analysis.
2) Window aggregation counting of recent new likes, new dislikes of video, with a sliding window 10s.
3) Computing concurrent number of views, with stateful counting of all plays and all leaves for a video.

[Redis](https://redis.io/) is chosen as a temporary storage of computing results for flask UI to query, given its support for fast and frequent queries.

On flask, a simple map (refresh every 10 seconds) was chosen to illustrate the computation results, with default view of squares representing individual videos, with square size proportional number of concurrent number of views, and color proportional to number of new likes in last 10s. On click of a video square, summary of aggregation results would show, including number of concurrent views, number of total views, number of new likes(last 10s), number of new dislikes(last 10s), most popular words mentioned in comments in last 1 minute, sentiment score of all comments in last 1 minute. Fancier looks or queries would certainly be available given all computation results are written in redis and ready for flask query, but I choose not to spend more time here.
![screenshot_flask](https://github.com/LinaMiao/engagestreaming/blob/master/images/screenshot_flask.png)
## AWS clusters
Kafka run on a cluster of one master (x4.large), 3 workers (x4.large), shared with [kafka manager](https://github.com/yahoo/kafka-manager) and secor.
Spark run on a cluster of one master (x4.large), 3 workers (x4.large), co-resident with Redis
Flask run on a single node (x4.large).

## Demo
Please visit [link]() for a short video of flask UI visualization.
