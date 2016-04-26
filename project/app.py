from flask import Flask,url_for
from utils import *
import redis, json, time, sys

REFRESH_RATE = 30


app = Flask(__name__,static_url_path='')
r = redis.StrictRedis(host='localhost', port=6379, db=0)

@app.route('/')
def index():
    return app.send_static_file('index.html')

current_keyword = None
counter = 0

@app.route('/search/<keyword>')
def search(keyword):
    global current_keyword
    current_keyword = keyword
    success = False
    # flush the redis
    r.flushall()
    # launch a new session using the new keyword
    success = launchListener(keyword)
    if not success:
        return "Cannot not launch listener"
    return "search launched"

states = set(["HI", "AK", "FL", "SC", "GA", "AL", "NC", "TN", "RI", "CT", "MA",
    "ME", "NH", "VT", "NY", "NJ", "PA", "DE", "MD", "WV", "KY", "OH",
    "MI", "WY", "MT", "ID", "WA", "DC", "TX", "CA", "AZ", "NV", "UT",
    "CO", "NM", "OR", "ND", "SD", "NE", "IA", "MS", "IN", "IL", "MN",
    "WI", "MO", "AR", "OK", "KS", "LS", "VA"])

@app.route('/stats')
def show_stats():
    #listener = getListener()
    #if listener != None:
    #    print "try to print"
    #    print listener.stdout.readline()

    global counter, current_keyword
    print "counter: ", counter
    counter += 1
    if counter == REFRESH_RATE and current_keyword != None:
        counter = 0
        launchListener(current_keyword)

    stats = {}

    for state in states:
        stats[state] = {
                'avg': 0.0,
                'p_count': 0,
                'n_count': 0,
                'sum': 0.0
        }

    for key in r.scan_iter():
    	value = json.loads(r.get(key))
    	sentiment = float(value['sentiment'])
        location = value['location']
        stats[location]['sum'] += sentiment
        if sentiment >= 0:
            stats[location]['p_count'] += 1
        else:
            stats[location]['n_count'] += 1

        stats[location]['avg'] = stats[location]['sum'] / (stats[location]['p_count'] \
                + stats[location]['n_count'])

    ret = json.dumps(stats,indent=4, separators=(',', ': '))
    return ret

if __name__ == '__main__':
    app.debug = True
    app.run(host= '0.0.0.0', port=3000)
