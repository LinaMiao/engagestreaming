from flask import Flask, request, g, render_template, redirect, url_for, jsonify, abort
import redis
import numpy
import json
import os

app = Flask(__name__)
app.config.from_object(__name__)

def query_all_data(grid_id=None):
    """
    Helper function to get all data from redis database.
    The ip and the password is loaded from the key.txt.
    The function returns the data as dictionary with
    grid id as key and dictionary of average speed and
    volume as the value and an integer of max_volume.
    """
    with open("/home/ubuntu/flaskapp/key.txt", 'r') as key_file:
        ip = key_file.readline().strip()
        password = key_file.readline().strip()

    r = redis.StrictRedis(host=ip,
                          port=6379,
                          password=password)
    if grid_id:
        return r.get(grid_id)
    else:
        data = {}
        max_volume = 1
        for key in r.scan_iter():
            val = r.get(key).split(';')
            data[key] = {"average speed": val[0],
                         "volume": val[1]}
            if int(val[1]) > max_volume:
                max_volume = int(val[1])
        return data, max_volume


# convert the pixel index into lat and long
def location_convertor(location):
    x = (122.470891 - 122.391583)/(800 - 338)
    y = -(37.813187 - 37.690489)/(900)
    x0 = -122.478101 - 295*x
    y0 = 37.813187

    return (y0 + location[1]*y, x0 + location[0]*x)


def get_colors(current_vol, max_vol):
    """
    The color is calculated directly to the
    proportion of max_val. A proportion of 0
    will return blue color code and a proportion
    of 1 will return red color code. Everything
    in between will be linearly scaled.
    """
    color_level = 356*current_vol/max_vol
    if color_level < 179:
        blue = "f4"
        red = hex(66 + color_level)[-2:]
    else:
        red = "f4"
        blue = hex(422 - color_level)[-2:]

    color = "#%s66%s"%(red, blue)
    return color


def generate_graph():
    """
    This function help to generate the json file for graph.
    It first reads all the entries from database 1 and filter
    out all records with no connections. Then it will add the
    edges to the set until the size reach 5000 (over 5000 edges
    will make the graph very massy). Finally it will use those
    information to generation an dictionary and dump it as "data.json".
    """
    with open("/home/ubuntu/flaskapp/key.txt", 'r') as key_file:
        ip = key_file.readline().strip()
        password = key_file.readline().strip()

    r = redis.StrictRedis(host=ip,
                          port=6379,
                          password=password,
                          db = 1)

    max_edges = 5000
    node_group = []
    node_group_len = []
    for k in r.keys():
        nodes = map(int, k[13:-1].split(', '))
        if len(nodes) > 1:
            node_group.append(nodes)
            node_group_len.append(len(nodes))

    nodes = set([])
    links = set([])
    node_group_len = numpy.array(node_group_len)
    for i in numpy.argsort(node_group_len)[::-1]:
        current_node = node_group[i]
        for j in xrange(len(current_node) - 1):
            for k in xrange(j, len(current_node)):
                nodes.add(current_node[j])
                nodes.add(current_node[k])
                if current_node[j] < current_node[k]:
                    links.add((current_node[j], current_node[k]))
                elif current_node[j] > current_node[k]:
                    links.add((current_node[k], current_node[j]))
        if len(links) > max_edges:
            break

    data_json = {"nodes":[], "links": []}
    for i in nodes:
        data_json["nodes"].append({"id": i, "group": 1})
    for i in links:
        data_json["links"].append({"source": i[0],
                                   "target": i[1],
                                   "value": 1})

    with open('/home/ubuntu/flaskapp/static/data.json', 'w') as outfile:
        json.dump(data_json, outfile)

    print len(data_json["nodes"]),len(data_json["links"])



# route to show the map
@app.route('/')
def index():
    grids = []
    print 'hellllllllllllllo index'
    # data, max_volume = query_all_data()

    # grids = []
    # for i in xrange(0,900,18):
    #     for j in xrange(0,900,18):
    #         grid_id = str(50*(j/18) + i/18)

    #         # calculate the lat and long of each
    #         # corner of the grid
    #         lat1, lon1 = location_convertor((i,j))
    #         lat2, lon2 = location_convertor((i,j + 18))
    #         lat3, lon3 = location_convertor((i + 18,j + 18))
    #         lat4, lon4 = location_convertor((i + 18,j))

    #         # if there are cars in the grid, show the average
    #         # speed and volume. If not, just show NA for average
    #         # speed and 0 for valume.
    #         if grid_id in data:
    #             speed = "average speed: %s"%data[grid_id]["average speed"]
    #             volume = "traffic volume: %s"%data[grid_id]["volume"]
    #             grids.append([lat1, lon1, lat2, lon2,
    #                           lat3, lon3, lat4, lon4,
    #                           speed, volume,
    #                           get_colors(int(data[grid_id]["volume"]),
    #                                      max_volume)])
    #         else:
    #             grids.append([lat1, lon1, lat2, lon2,
    #                           lat3, lon3, lat4, lon4,
    #                           "average speed: NA", "traffic volume: 0",
    #                           get_colors(0, max_volume)])
    grids = []
    grids.append([39, -98, 40, -100.5,
                  50, -100, 50, -98.6,
                  "average speed: NA", "traffic volume: 0",
                  get_colors(0, 10)])
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
    return redirect("https://www.linkedin.com/in/genedersu")

if __name__ == '__main__':
    app.run()
