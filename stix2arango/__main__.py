import os
import sys

if 'TEST' in os.environ != '':
    sys.path.insert(0, '/app')

from flask import Flask, json, request
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
import flask
import argparse
from pyArango.connection import Connection
from datetime import datetime
from urllib.parse import unquote

from stix2arango.version import __version__, __author__
from stix2arango.request import Request
from stix2arango.feed import vaccum, Feed
from stix2arango.storage import snapshot, snapshot_restore
from stix2arango.utils import ArangoUser

app = Flask(__name__)
db_conn = None
arangoURL = None
login_manager = LoginManager()
authenticated_users = {}


"""
    Simple wrapper around stix2arango requests.
"""

@app.route('/')
@login_required
def home():
    return {'version': __version__}

@app.route('/request', methods=['GET'])
@login_required
def request_for_stix():
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
@login_required
def vaccum_database():
    vaccum(db_conn)
    return {'results': 'ok'}


@app.route('/login', methods=['POST'])
def login():
    global authenticated_users
    if 'name' in request.form and 'password' in request.form:
        user = ArangoUser(
            request.form['name'], 
            request.form['password'],
            arangoURL
        )
        if user.is_authenticated():
            authenticated_users[int(user.id)] = user
            login_user(user)
            flask.flash('Logged in successfully.')
            next = flask.request.args.get('next')
            return flask.redirect(next or flask.url_for('home'))
        else:
            return {'error': 'Invalid username or password'}
    return {'results': 'please post name & password to authenticate'}

@login_manager.user_loader
def load_user(user_id):
    return authenticated_users[int(user_id)]

def launch_web_server(args):
    global login_manager
    login_manager.init_app(app)
    app.config['SECRET_KEY'] = os.urandom(30).hex()
    if args.ssl_cert and args.ssl_key:
        app.run(
            host='0.0.0.0',
            port=args.web_port,
            ssl_context=(args.ssl_cert, args.ssl_key)
        )
    else:
        app.run(
            host='0.0.0.0',
            port=args.web_port
        )

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
        '--ssl_cert',
        default=None,
        help='Path to the ssl certificate for web_server'
    )
    parser.add_argument(
        '--ssl_key',
        default=None,
        help='Path to the ssl key for web_server'
    )
    parser.add_argument(
        '--web_port',
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
    arangoURL = 'http://{}:{}'.format(args.host, args.port)
    conn = Connection(
        username=args.user,
        password=args.password,
        arangoURL=arangoURL
    )
    db_conn = conn[args.db]
    if args.action == 'web_server':
        launch_web_server(args)
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
