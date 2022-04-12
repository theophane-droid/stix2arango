import psycopg2
import os
from datetime import datetime

from psycopg2.errors import UndefinedTable, InFailedSqlTransaction
from stix2 import IPv4Address, AutonomousSystem, Identity
from stix2 import Relationship, Incident, IPv6Address
from pyArango.connection import Connection
from pyArango.theExceptions import CreationError

from stix2arango.feed import Feed, vaccum
from stix2arango.storage import TIME_BASED, STATIC
from stix2arango.postgresql import PostgresOptimizer
from stix2arango.request import Request

db = 'stix2arango'
user = 'root'
pass_ = 'changeme'
host = 'localhost'

def get_number_of_table_for_feed(feed_name, cursor):
    s = "SELECT"
    s += " table_schema"
    s += ", table_name"
    s += " FROM information_schema.tables"
    s += " WHERE"
    s += " ("
    s += " table_schema = 'public'"
    s += " )"
    s += " ORDER BY table_schema, table_name;"
    cursor = PostgresOptimizer.postgres_conn.cursor()
    cursor.execute(s)
    results = [list(r)[1] for r in cursor.fetchall()]
    results = [r for r in results if r.startswith(feed_name) ]
    return len(results)

# can be debugged with :
#  psql --username=root --host=localhost --port=5432 --database=stix2arango

def get_database():
    password = os.environ['ARANGO_ROOT_PASSWORD']
    url = os.environ['ARANGO_URL']
    db_conn = Connection(username='root', password=password, arangoURL=url)
    try:
        database = db_conn.createDatabase('stix2arango')
    except CreationError:
        database = db_conn['stix2arango']
    return database


print('> Test postgres optimizer')

auth = "dbname='%s' user='%s' host='%s' password='%s'" % (db, user, host, pass_)

postgres_conn = psycopg2.connect(auth)
arango_conn = get_database()
feed = Feed(
    arango_conn, 
    'posgres_test', 
    tags=['postgres'], 
    date=datetime.now(),
    storage_paradigm=TIME_BASED
    )
PostgresOptimizer.postgres_conn = postgres_conn
optimized_field0 = 'ipv4-addr:value'
optimizer0 = PostgresOptimizer(optimized_field0)
optimized_field1 = 'ipv4-addr:x_ip:broadcast_addr'
optimizer1 = PostgresOptimizer(optimized_field1)
feed.optimizers.append(optimizer0)
# feed.optimizers.append(optimizer1)

_autonomous_system = AutonomousSystem(number=1234, name='Google')
ipv4_1 = IPv4Address(value='97.8.8.8', belongs_to_refs=[_autonomous_system.id])
ipv4_2 = IPv4Address(value='97.8.8.0/24', belongs_to_refs=[_autonomous_system.id])
_identity = Identity(name='My grand mother', identity_class='individual')
_relation = Relationship(source_ref=_identity.id, target_ref=ipv4_1.id, relationship_type='attributed-to')
feed.insert_stix_object_in_arango([_autonomous_system,ipv4_1, ipv4_2, _identity, _relation])

PostgresOptimizer.postgres_conn.commit()

request = Request(arango_conn, datetime.now())
results = request.request("  [ipv4-addr:value  =   '97.8.8.8' ]  ",
                       max_depth=-10, tags=['postgres'])

print('\n\n\nresults:')
for r in results:
    print('\t\t', r)
print('END results')
assert(len(results) == 1)

print('OK')


print('\n\n> Postgres Vaccum test')
feed = Feed(
    arango_conn, 
    'posgres_vaccum_test', 
    tags=['postgres'], 
    date=datetime.now(),
    storage_paradigm=TIME_BASED,
    vaccum_date=datetime.fromtimestamp(10)
    )
optimizer0 = PostgresOptimizer(optimized_field0)
feed.optimizers.append(optimizer0)

ipv4 = IPv4Address(value='97.8.1.0/24')
feed.insert_stix_object_in_arango([ipv4])
vaccum(arango_conn)
feeds = Feed.get_last_feeds(arango_conn, datetime(2022, 12, 12))
for feed in feeds:
    if feed.feed_name == 'vaccumentest':
        raise Exception('Vaccum failed')
    table_name = optimizer0.table_name
    sql = "select * from " + table_name
    try:
        cursor = PostgresOptimizer.postgres_conn.cursor()
        cursor.execute(sql)
        raise RuntimeError('vaccum failed !')
    except (UndefinedTable, InFailedSqlTransaction):
        cursor.close()
print('OK')

print('> static storage with optimizer')
feed = Feed(
    arango_conn, 
    'posgres_test_static', 
    tags=['postgres'], 
    date=datetime.now(),
    storage_paradigm=STATIC
    )
optimizer0 = PostgresOptimizer(optimized_field0)
feed.optimizers.append(optimizer0)
ipv4 = IPv4Address(value='97.8.1.0/24')
feed.insert_stix_object_in_arango([ipv4])

feed = Feed(
    arango_conn, 
    'posgres_test_static', 
    tags=['postgres'], 
    date=datetime.now(),
    storage_paradigm=STATIC
    )
feed.optimizers.append(optimizer0)
ipv4 = IPv4Address(value='97.8.1.9')
feed.insert_stix_object_in_arango([ipv4])

# check if the number of tables is 1
PostgresOptimizer.postgres_conn.commit()
assert(get_number_of_table_for_feed(feed.feed_name, PostgresOptimizer.postgres_conn.cursor()) == 1)