"""
Simulation of video logs, written to a json file event.txt
Event type included:
    Broadcaster start live streaming   - start_video
    Broadcaster end live streaming     - end_video
    Watchers start watching video      - play
    Watchers react to video positively - like
    Watchers react to video negatively - dislike
    Watchers comment to video          - comment
    Watchers end watching video        - leave

Simulation logic:
    Within in the simulation window , each video would be simulated to start with random start time and random duration. For each video, random number of watchers are random generated to start/like/dislike/comment/leave watching at random time.

TO_DOs:
    Location weighted distribution
    Use us geojson for better boundries
    Videos -  Random matching real world video names
    Users(Broadcasters/watchers) - Random matching real world names

"""

import random
import json

VIDEO_COUNT = 1000
MINUTE = 10
HOUR = MINUTE * 6
WATCH_TIL_END_PERCENTAGE = 0.3
COMMENT_POSIBILITY = 0.2 #the smaller the more likely of having a comment, decrease to test throughput of spark NLP streaming


FILE_NAME = 'events.txt'
USER_UNIVERSE_SIZE = 1e5
START_TIME_RANGE = MINUTE
VIDEO_MAX_POPULARITY = 5e3
LAT_LOWBOUND = 24
LAT_HIGHBOUND = 50
LONG_LOWBOUND =  -124
LONG_HIGHBOUND =  -66

# use imdb review data as comments for now
REIEWS_POS_DIR = './imdb_review'
pos_reviews_file = []
with open('./imdb_review/pos/file_list') as f:
    for line in f:
        pos_reviews_file.append('./imdb_review/pos/'+ line[:-1])

neg_reviews_file = []
with open('./imdb_review/neg/file_list') as f:
    for line in f:
        neg_reviews_file.append('./imdb_review/neg/'+ line[:-1])


class Video:

    @staticmethod
    def create_video(id):
        return Video(id)

    def __init__(self, id):
        self.id = id
        self.broadcaster_id = int(random.uniform(0, USER_UNIVERSE_SIZE));
        self.start_time = int(random.uniform(0, START_TIME_RANGE))
        #self.duration = int(random.uniform(0.2 * MINUTE, HOUR)) # more realistic
        self.duration = int(random.uniform(0.2 * MINUTE, 3 * MINUTE )) # artifical to increase event density
        self.popularity = int(random.gammavariate(0.5, 10) * VIDEO_MAX_POPULARITY / 10)
        self.lat = random.uniform(LAT_LOWBOUND,LAT_HIGHBOUND)
        self.long = random.uniform(LONG_LOWBOUND,LONG_HIGHBOUND)
        self.comment = ''

    def create_watch_session(self):
        is_watch_to_end = random.uniform(0, 10) >= WATCH_TIL_END_PERCENTAGE * 10
        video_end_time = self.start_time + self.duration
        watch_start_time = random.uniform(self.start_time, video_end_time)
        watch_end_time = video_end_time if is_watch_to_end else random.uniform(self.start_time, video_end_time)
        watch_session = WatchSession(self.id, watch_start_time, watch_end_time)
        return watch_session

    def to_events(self):
        return [Event(self.start_time, 'start_video', self.broadcaster_id, self.id, self.lat, self.long),
                Event(self.start_time + self.duration, 'end_video', self.broadcaster_id, self.id, self.lat, self.long)]


class WatchSession:
    def __init__(self, video_id, start_time, end_time):
        self.user_id = int(random.uniform(0, USER_UNIVERSE_SIZE))
        self.video_id = video_id
        self.start_time = start_time
        self.end_time = end_time
        self.lat = random.uniform(LAT_LOWBOUND,LAT_HIGHBOUND)
        self.long = random.uniform(LONG_LOWBOUND,LONG_HIGHBOUND)


        # reactions (like, dislike, comment, happends )
        self.reaction_time = random.uniform(self.start_time, self.end_time)

        #assume each user is going to react once, at random time of video
        like_pobablity = random.uniform(0,1)
        if like_pobablity >= 0.7:
            self.reaction = 'like'
        elif like_pobablity < 0.7:
            self.reaction = 'dislike'


        #comments
        rand_hot_spot_len = random.uniform(0.1, 0.4) * (self.end_time-self.start_time)
        rand_hot_spot_start = random.uniform(0.2,0.5) * (self.end_time-self.start_time) + self.start_time
        rand_hot_spot_end = rand_hot_spot_start + rand_hot_spot_len

        self.comment_time = random.uniform(rand_hot_spot_start,rand_hot_spot_end)
        comment_possibility = random.uniform(0,1)
        if comment_possibility > COMMENT_POSIBILITY:
            self.comment = self.fetch_comment(like_pobablity>=0.5, comment_possibility)
        else:
            self.comment = ''

    def fetch_comment(self, pos, seed):
        #randomly fetch a comment
        # pos - 1 return positive comment
        #     - 0 return negative comment
        # random seed, between 0 and 1
        comment = []
        if pos == 1:
            with open(pos_reviews_file[int(seed*len(pos_reviews_file))]) as f:
                for line in f:
                    comment.append(line)
        else:
            with open(neg_reviews_file[int(seed*len(neg_reviews_file))]) as f:
                for line in f:
                    comment.append(line)

        return ' '.join(comment)


    def to_events(self):
        ret = [Event(self.start_time, 'play', self.user_id, self.video_id, self.lat, self.long),
                Event(self.end_time, 'leave', self.user_id, self.video_id, self.lat,self.long),
                Event(self.reaction_time, self.reaction, self.user_id, self.video_id, self.lat, self.long)]
        if len(self.comment) > 0:
            ret.append(Event(self.comment_time, 'comment', self.user_id, self.video_id, self.lat, self.long, self.comment))



        return ret



class Event:
    def __init__(self, timestamp, event_type, user_id, video_id, lat, long, comment=''):
        self.timestamp = timestamp
        self.event_type = event_type
        self.user_id = user_id
        self.video_id = video_id
        self.lat = lat
        self.long = long
        self.comment = comment

    def __str__(self):
        if len(self.comment) > 0:
            return json.dumps({'timestamp': self.timestamp, 'event_type': self.event_type, 'user_id': self.user_id, 'video_id': self.video_id, 'latitude': self.lat, 'longitude':self.long, 'comment':self.comment })
        else:
            return json.dumps({'timestamp': self.timestamp, 'event_type': self.event_type, 'user_id': self.user_id, 'video_id': self.video_id, 'latitude': self.lat, 'longitude':self.long })

    def __repr__(self):
        return self.__str__()




events = []
for num in range (0, VIDEO_COUNT):
    print 'generating video ' + str(num)
    video = Video.create_video(num)
    events.extend(video.to_events())
    for user in range(0, video.popularity):
        watch_session = video.create_watch_session()
        events.extend(watch_session.to_events())


f = open(FILE_NAME, 'w')
print 'sorting events'
events = sorted(events, key= lambda event: event.timestamp)

print 'writing to file'
for event in events:
    f.write(str(event))
    f.write('\n')

f.close()
