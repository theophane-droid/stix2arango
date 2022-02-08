import os
import sys

if 'TEST' in os.environ != '':
    sys.path.insert(0, '/app')

from flask import Flask, json, request
import flask
import argparse
from pyArango.connection import Connection
from datetime import datetime
from urllib.parse import unquote

from stix2arango.version import __version__, __author__
from stix2arango.request import Request
from stix2arango.feed import vaccum, Feed
from stix2arango.storage import snapshot, snapshot_restore

app = Flask(__name__)
db_conn = None


"""
    Simple wrapper around stix2arango requests.
"""


@app.route('/')
def home():
    return {'version': __version__}


@app.route('/request', methods=['GET'])
def request_for_stix():
    # request with stix2arango
    pattern = request.args.get('pattern')
    timestamp = request.args.get('timestamp')
    tags = request.args.get('tags')
    depth = request.args.get('depth')
    if timestamp:
        date = datetime.fromtimestamp(int(timestamp))
    else:
        date = datetime.now()
    if tags:
        tags = unquote(tags).split(',')
    else:
        tags = []
    if depth:
        depth = int(depth)
    else:
        depth = 3
    if not pattern:
        return {'error': 'pattern is required'}
    r = Request(db_conn, date)
    return {'results': r.request(unquote(pattern), tags, max_depth=depth)}


@app.route('/vaccum', methods=['GET'])
def vaccum_database():
    vaccum(db_conn)
    return {'results': 'ok'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='start a web server, which wraps stix2arango'
    )
    parser.add_argument(
        '--host',
        help='database host'
    )
    parser.add_argument(
        '--port',
        default=8529,
        help='database port'
    )
    parser.add_argument(
        '--db',
        help='database name'
    )
    parser.add_argument(
        '--user',
        default='root',
        help='database user'
    )
    parser.add_argument(
        '--password',
        default='',
        help='database password'
    )
    parser.add_argument(
        '--action',
        default='web_server',
        help='can be web_server, vaccum, snapshot or restore'
    )
    parser.add_argument(
        '--snapshot_dir',
        help='Directory of the snapshot to take/restore'
    )
    parser.add_argument(
        '--https-cert',
        default=None,
        help='Path to the HTTPS certificate for web_server'
    )
    parser.add_argument(
        '--web-port',
        default=622,
        help='Port for the web server'
    )

    args = parser.parse_args()
    if not args.host:
        print('Please provide a host for the database')
        exit(1)
    if not args.db:
        print('Please provide a database name')
        exit(1)
    if not args.password:
        print('Please provide a password for the database')
        exit(1)
    url = 'http://{}:{}'.format(args.host, args.port)
    conn = Connection(
        username=args.user,
        password=args.password,
        arangoURL=url
    )
    db_conn = conn[args.db]
    if args.action == 'web_server':
        app.run(host='0.0.0.0', port=args.web_port)
    elif args.action == 'vaccum':
        vaccum(db_conn)
    elif args.action == 'snapshot':
        if not args.snapshot_dir:
            print('Please provide a snapshot directory')
            exit(1)
        feeds = Feed.get_last_feeds(db_conn, datetime.now())
        snapshot(
            args.host,
            args.port,
            args.user,
            args.password,
            args.db,
            args.snapshot_dir,
            feeds
        )
    elif args.action == 'restore':
        if not args.snapshot_dir:
            print('Please provide a snapshot directory')
            exit(1)
        snapshot_restore(
            args.host,
            args.port,
            args.user,
            args.password,
            args.db,
            args.snapshot_dir
        )
