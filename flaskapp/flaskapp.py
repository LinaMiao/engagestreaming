# UI: map with video total views as size of box
#     choice of overlay: windowed sentiment_score / windowed no. of likes / windowed no. of dislikes
#
#     Onclick of

# TODOs: polygon to circle, size adjust
#      : Onclick of videos
#      :
from flask import Flask, request, g, render_template, redirect, url_for, jsonify, abort
import redis
import numpy
import json
import os

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



# get sentiment_score, mapping score to colors
def get_comments():

    grids = []
    for key in r1.keys():
        sentiment = r1.get(key)
        item = r4.get(key).split()
        lat0 = float(item[12][:-5])
        long0 = float(item[10][:-5])
        color = get_colors(float(sentiment),1,3)

        corners = []
        views = get_view(key)
        views = int(r6.get(key))
        size = 0.5 * views/1000.0
        dxs = [-1, -1, 1, 1]
        dys = [-1, 1, 1, -1]
        for dx,dy in zip(dxs,dys):
            corners.append(lat0 + dx*size)
            corners.append(long0 + dy*size)

        grids.append(corners + ["video_id:"+key, "sentiment_score:"+str(round(float(sentiment),2))] + [color])
    return grids


# get number of likes, mapping score to colors
def get_likes():

    grids = []
    for key in r1.keys():
        likesCount = r2.get(key)
        item = r4.get(key).split()
        lat0 = float(item[12][:-5])
        long0 = float(item[10][:-5])
        color = get_colors(float(likesCount),100,500)

        corners = []
        views = get_view(key)
        size = 0.5 * views/1000.0
        dxs = [-1, -1, 1, 1]
        dys = [-1, 1, 1, -1]
        for dx,dy in zip(dxs,dys):
            corners.append(lat0 + dx*size)
            corners.append(long0 + dy*size)

        grids.append(corners + ["video_id:"+key, "number of likes:"+str(round(float(likesCount),2))] + [color])
    return grids


# get number of dislikes, mapping score to colors
def get_dislikes():

    grids = []
    for key in r1.keys():
        dislikesCount = r3.get(key)
        item = r4.get(key).split()
        lat0 = float(item[12][:-5])
        long0 = float(item[10][:-5])
        color = get_colors(float(dislikesCount),100,500)

        corners = []
        views = get_view(key)
        size = 0.5 * views/10.0
        dxs = [-1, -1, 1, 1]
        dys = [-1, 1, 1, -1]
        for dx,dy in zip(dxs,dys):
            corners.append(lat0 + dx*size)
            corners.append(long0 + dy*size)

        grids.append(corners + ["video_id:"+key, "number of likes:"+str(round(float(dislikesCount),2))] + [color])
    return grids


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

# get number of views at current time window
def get_views():
    grids = []

    for key in r6.keys():

        total_views = r6.get(key)
        item = r4.get(key).split()
        lat0 = float(item[12][:-5])
        long0 = float(item[10][:-5])

        views,plays,leaves = get_view(key,1)
        print key, views, plays, leaves
        color = get_colors(1,1,5) #get uniform color, use size to diff
        #color = get_colors(views, 0, 100)
        corners = []


        size = float(total_views)/100.0
        dxs = [-2, -2, 2, 2]
        dys = [-1, 1, 1, -1]
        for dx,dy in zip(dxs,dys):
            corners.append(lat0 + dy*size)
            corners.append(long0 + dx*size)

        grids.append(corners + ["Number of live views:"+str(round(float(plays),2)), "Number of leave views:"+str(leaves)] + [color])
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


# route for the api of grid and values
@app.route('/api/alldata')
def all_data_api():
    data, volume = query_all_data()
    return jsonify(data)


# route for display form for query a location
@app.route('/query', methods=['GET', 'POST'])
def query():
    if request.method == 'POST':
        lat = request.form['lat']
        lon = request.form['lon']
        return redirect('http://autolog.online/api/query?lat=%s&long=%s'%(lat, lon))
    else:
        return render_template("query.html")

# route for the api of a specific grid
@app.route('/api/query')
def grid_data_api():
    lat = float(request.args.get('lat'))
    lon = float(request.args.get('long'))
    print lat, lon, type(lat), type(lon)
    grid_id = 50*(int(round((37.813187 - lat)/0.00013633111))/18) + \
              int(round((lon + 122.528741387)/0.00017166233))/18
    data = query_all_data(grid_id=grid_id)
    if data:
        data = data.split(';')
        return jsonify({"average speed": data[0],
                        "volume": data[1]})
    else:
        return jsonify({"average speed": 0,
                        "volume": 0})


# route for display graph
@app.route('/graph')
def graph():
    generate_graph()
    return render_template("graph.html")

# route for about me page
@app.route('/about_me')
def about_me():
    return redirect("https://www.linkedin.com/in/lina-miao-9312a453/")

if __name__ == '__main__':
    app.run()
