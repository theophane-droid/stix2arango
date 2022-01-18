from flask import Flask, json, request
import flask
import argparse
from pyArango.connection import Connection
from datetime import datetime
from urllib.parse import unquote

from stix2arango.version import __version__, __author__
from stix2arango.request import Request

app = Flask(__name__)
db_conn = None

"""
    Simple wrapper around stix2arango requests. 
"""

@app.route('/')
def home():
    return  {'version': __version__,}

@app.route('/request', methods=['GET'])
def request_for_stix():
    # request with stix2arango
    pattern = request.args.get('pattern')
    timestamp = request.args.get('timestamp')
    tags = request.args.get('tags')
    if timestamp:
        date = datetime.fromtimestamp(int(timestamp))
    else:
        date = datetime.now()
    if tags :
        tags = unquote(tags).split(',')
    else:
        tags = []
    if not pattern:
        return {'error': 'pattern is required'}
    r = Request(db_conn, date)
    return {'results': r.request(unquote(pattern), tags)}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='start a web server, which wraps stix2arango')
    parser.add_argument('--host', default='smaug.local', help='database host')
    parser.add_argument('--port', default=8529, help='database port')
    parser.add_argument('--db', default='smaug', help='database name')
    parser.add_argument('--user', default='root', help='database user')
    parser.add_argument('--password', default='', help='database password')
    args = parser.parse_args()
    if args.password == '':
        print('Please provide a password for the database')
        exit(1)
    url = 'http://{}:{}'.format(args.host, args.port)
    print(url)
    conn = Connection(username=args.user, password=args.password, arangoURL=url)
    db_conn = conn[args.db]
    app.run(host='0.0.0.0', port=622)
