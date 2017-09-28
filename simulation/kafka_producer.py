import sys

import os
import time
from kafka import KafkaProducer, KeyedProducer
import json


def logToTuple(log):
    #convert json event log to (key, value) tuple, key as video_id,
    #input would be expected like '{"user_id": 8671, "event_type": "play", "timestamp": 0.4131126659468838, "video_id": 3, "longitude": -90.84796994391165, "latitude": 49.18385876614184, 'comment': 'lalala'}'
    key = json.loads(log)['video_id']
    value = log
    return (key, value)


def main():
    """Reads input file and sends the lines as json to Kafka"""

    # Set up Producer: send to all instances
    ipfile = open('ip_addresses.txt', 'r')
    ips = ipfile.read()[:-1]
    ipfile.close()
    ips = ips.split(', ')

    producer_start = KafkaProducer(bootstrap_servers=ips,value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    producer_end = KafkaProducer(bootstrap_servers=ips,value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    producer_play = KafkaProducer(bootstrap_servers=ips,value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    producer_leave = KafkaProducer(bootstrap_servers=ips,value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    producer_like = KafkaProducer(bootstrap_servers=ips,value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    producer_dislike = KafkaProducer(bootstrap_servers=ips,value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    producer_comment = KafkaProducer(bootstrap_servers=ips,value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Read the file over and over and send the messages line by line

    curr_time = 0
    #os.system('python redis_flushdb.py')

    while True:
    # Open file and send the messages line by line
        with open(sys.argv[1]) as f:

            for line in f:


                # prepare videos to (key, value) tuples, for topic partition
                log_tuple = logToTuple(line)

                #partition events into potion of seconds (.1 min to mimic 1sec high throughput)
                interval = 10
                timestamp = int((json.loads(line)['timestamp'])*interval)/float(interval)
                if timestamp != curr_time:
                    time.sleep(1)
                    print time.strftime('%X %x %Z'), curr_time
                    curr_time = int(timestamp*interval)/float(interval)


                # send the messages to separate topics
                if 'comment' in line:
                    producer_comment.send('comments_topic', log_tuple)
                elif 'start' in line:
                    producer_start.send('start_topic', log_tuple)
                elif 'end' in line:
                    producer_end.send('end_topic', log_tuple)
                elif 'dislike' in line:
                    producer_like.send('dislike_topic', log_tuple)
                elif 'like' in line:
                    producer_dislike.send('like_topic', log_tuple)
                elif 'play' in line:
                    producer_play.send('play_topic', log_tuple)
                elif 'leave' in line:
                    producer_leave.send('leave_topic', log_tuple)

        #time.sleep(3600)
        #os.system('python redis_flushdb.py')




if __name__ == "__main__":
    main()
