# UI: map with video total views as size of box
#     choice of overlay: windowed sentiment_score / windowed no. of likes / windowed no. of dislikes
#
#     Onclick of

# TODOs: polygon to circle, size adjust
#      : Onclick of videos
#      :
from flask import Flask, request, g, render_template, redirect, url_for, jsonify, abort
import redis
import numpy as np
import json
import os
import re
from collections import Counter

app = Flask(__name__)
app.config.from_object(__name__)


with open("/home/ubuntu/Engage/flaskapp/key.txt", 'r') as key_file:
    ip = key_file.readline().strip()
    password = key_file.readline().strip()

r1 = redis.StrictRedis(host=ip,
                      port=6379,
                      password=password,
                      db = 1)
r2 = redis.StrictRedis(host=ip,
                      port=6379,
                      password=password,
                      db = 2)
r3 = redis.StrictRedis(host=ip,
                      port=6379,
                      password=password,
                      db = 3)
r4 = redis.StrictRedis(host=ip,
                      port=6379,
                      password=password,
                      db = 4)
r5 = redis.StrictRedis(host=ip,
                      port=6379,
                      password=password,
                      db = 5)
r6 = redis.StrictRedis(host=ip,
                      port=6379,
                      password=password,
                      db = 6)
r7 = redis.StrictRedis(host=ip,
                      port=6379,
                      password=password,
                      db = 7)

# a color function, linear scale curr_val betwen min_val and max_val
def get_colors(curr_val, min_val, max_val):

    color_level = 356*(curr_val-min_val)/max_val

    if color_level < 179:
        blue = "f4"
        red = hex(66 + int(color_level))[-2:]
    else:
        red = "f4"
        blue = hex(422 - int(color_level))[-2:]

    color = "#%s66%s"%(red, blue)
    return color

#stop word list from nltk
stop_words = ['all', 'just', 'being', 'over', 'both', 'through', 'yourselves', \
'its', 'before', 'o', 'hadn', 'herself', 'll', 'had', 'should', 'to', 'only', 'won', \
'under', 'ours', 'has', 'do', 'them', 'his', 'very', 'they', 'not', 'during', 'now', \
'him', 'nor', 'd', 'did', 'didn', 'this', 'she', 'each', 'further', 'where', 'few', \
'because', 'doing', 'some', 'hasn', 'are', 'our', 'ourselves', 'out', 'what', 'for', \
'while', 're', 'does', 'above', 'between', 'mustn', 't', 'be', 'we', 'who', 'were', \
'here', 'shouldn', 'hers', 'by', 'on', 'about', 'couldn', 'of', 'against', 's', 'isn', \
'or', 'own', 'into', 'yourself', 'down', 'mightn', 'wasn', 'your', 'from', 'her', 'their', \
'aren', 'there', 'been', 'whom', 'too', 'wouldn', 'themselves', 'weren', 'was', 'until', \
'more', 'himself', 'that', 'but', 'don', 'with', 'than', 'those', 'he', 'me', 'myself', 'ma', \
'these', 'up', 'will', 'below', 'ain', 'can', 'theirs', 'my', 'and', 've', 'then', 'is', 'am', \
'it', 'doesn', 'an', 'as', 'itself', 'at', 'have', 'in', 'any', 'if', 'again', 'no', 'when', \
'same', 'how', 'other', 'which', 'you', 'shan', 'needn', 'haven', 'after', 'most', 'such', \
'why', 'a', 'off', 'i', 'm', 'yours', 'so', 'y', 'the', 'having', 'once',\
"wrappedarray","comment","the","like","film","movie","think", "br", "one", "would", "much",\
"txt", "even", "u", "thought", "k", "really", "know", "get", "n"]


# get top words from past comment
def get_words(key):

    words_raw = r1.get(key+"_token")
    if words_raw is not None:
        #clean a little bit
        words = re.sub("[^A-Za-z]"," ",words_raw).split()
        # remove stop words
        words = [word.lower() for word in words if word.lower() not in stop_words]
        common = Counter(words).most_common(5)
        return str(common)
    else:
        return "Comments yet to come"

# get sentiment_score
def get_sentiment_score(key):

    sentiment_score_raw = r1.get(key+"_sentiment")
    if sentiment_score_raw is not None:
        sentiment = [float(s) for s in sentiment_score_raw.split(",")[13:-1]]
        if len(sentiment) > 0:
            print sentiment
            sentiment_avg = round(np.mean(sentiment),2)
            return str(sentiment_avg)
        else:
            return "Sentiment score yet to come"
    return "Sentiment score yet to come"


#get no. of current views
def get_view(key, debug=0):
    playsCount = r6.get(key)
    if r7.get(key):
        endsCount = r7.get(key)
    else:
        endsCount = 0
    views = float(playsCount) - float(endsCount) # this could be used to indicate out of order log files,

    if debug:
        return views, float(playsCount), float(endsCount)
    else:
        return views


# get number of views at current time window, used as default load view,
# curr_view as size
def get_views():
    grids = []

    for key in r6.keys():

        total_views = r6.get(key)
        # get location info
        item = r4.get(key).split()
        lat0 = float(item[12][:-5])
        long0 = float(item[10][:-5])

        views,plays,leaves = get_view(key,1)


        corners = []

        size = min(float(views)/5000.0,1)
        dxs = [-1.5, -1.5, 1.5, 1.5]
        dys = [-1, 1, 1, -1]
        for dx,dy in zip(dxs,dys):
            corners.append(lat0 + dy*size)
            corners.append(long0 + dx*size)

        words = get_words(key)
        sentiment_score = get_sentiment_score(key)
        recentLikes = r2.get(key) if r2.get(key) is not None else 0
        recentDislikes = r3.get(key)

        # total views as color
        #color = get_colors(int(total_views),100,10000)

        # total views as color
        color = get_colors(int(recentLikes),1,100)

        onClick = ["Concurent viewers: "+str(int(views)), "Total viewers: "+str(total_views),
        "Recent likes: "+str(recentLikes), "Recent dislikes: "+str(recentDislikes),
        "Top popular words: "+str(words), "Sentiment score: "+str(sentiment_score)]

        grids.append(corners + onClick + [color])
    return grids


# on click of per video
# 1-where are all the viewers (within in window)
# 2-what are the top 10 words (within window)
# 3-what is the sentiment score (within window)
# 4-where are all the likes (within window) - optional
# 5-where are all the dislikes (within window) - optional
def get_views_details():
    grids = []
    for key in r6.keys():
        item = r4.get(key).split()
        lat0 = float(item[12][:-5])
        long0 = float(item[10][:-5])
        color = get_colors(1,1,5)
    return grids

# route to show the map
@app.route('/')
def index():
    grids = []
    print 'hellllllllllllllo index'

    grids = get_views()
    #print grids
    return render_template("index.html", grids=grids)


if __name__ == '__main__':
    app.run()
