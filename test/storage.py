import sys, os
sys.path.insert(0, '/app')
from pyArango.connection import *

from stix2arango.storage import snapshot, snapshot_restore
from stix2arango.feed import Feed

def get_database():
    password = os.environ['ARANGO_ROOT_PASSWORD']
    url = os.environ['ARANGO_URL']
    db_conn = Connection(username='root', password=password, arangoURL=url)
    try:
        database = db_conn.createDatabase('stix2arango')
    except CreationError:
        database = db_conn['stix2arango']
    return database


def snapshot_test(db_conn):
    host_and_port = os.environ['ARANGO_URL'].split('/')[-1]
    host, port = host_and_port.split(':')
    user = 'root'
    password = os.environ['ARANGO_ROOT_PASSWORD']
    database = 'stix2arango'
    feeds = Feed.get_last_feeds(db_conn, datetime.now())
    snapshot(
        host,
        port,
        user,
        password,
        database,
        '/dump', 
        feeds
    )
    snapshot_restore(
        host,
        port,
        user,
        password,
        database,
        '/dump'
    )

snapshot_test(get_database())
