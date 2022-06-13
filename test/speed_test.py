import sys
from datetime import datetime

sys.path.insert(0, '/home/theo/stix2arango')

from stix2 import DomainName
import psycopg2
import time

from stix2arango.feed import Feed
from stix2arango.utils import get_database
from stix2arango.postgresql import PostgresOptimizer
from stix2arango.request import Request

from random import choice

import itertools

db_conn = get_database()

db = 'stix2arango'
user = 'root'
pass_ = 'changeme'
host = 'localhost'
auth = "dbname='%s' user='%s' host='%s' password='%s'" % (db, user, host, pass_)
postgres_conn = psycopg2.connect(auth)

PostgresOptimizer.postgres_conn = postgres_conn

alpha = 'abcdefghijklmnopqrstuvwxyz'

def gen_domain(N):
    return [''.join(list(e)) + '.com' for e in 
            itertools.permutations(alpha, N)]
    

domains = gen_domain(4)

def insert_no_optimizer():
    feed = Feed(db_conn, 'speed_test_no_optimizer', tags=['speed_test_no_optimizer'])
    l_stix_obj = []
    for domain_name in domains:
        domain = DomainName(value=domain_name)
        l_stix_obj += [domain]
    feed.insert_stix_object_in_arango(l_stix_obj)

def insert_optimizer():
    feed = Feed(db_conn, 'speed_test_optimizer', tags=['speed_test_optimizer'])
    optimizer = PostgresOptimizer('domain-name:value')
    feed.optimizers.append(optimizer)
    l_stix_obj = []
    for domain_name in domains:
        domain = DomainName(value=domain_name)
        l_stix_obj += [domain]
    feed.insert_stix_object_in_arango(l_stix_obj)
     
def request_no_optimizer(requested_domain, count):
    request = Request(db_conn, datetime.now())
    pattern = '[domain-name:value = \"%s\"]' % (requested_domain)
    results = request.request(pattern, tags=['speed_test_no_optimizer'], max_depth=0)
    assert(len(results) == count)

def request_optimizer(requested_domain, count):
    request = Request(db_conn, datetime.now())
    pattern = '[domain-name:value = \"%s\"]' % (requested_domain)
    results = request.request(pattern, tags=['speed_test_optimizer'], max_depth=0)
    assert(len(results) == count)


if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print('usage %s : (insert|request)' % (sys.argv[0]))

    if len(sys.argv) == 1 or sys.argv[1] == 'request':
        N = 10
        tot = 0
        for _ in range(N):
            requested_domain = choice(domains)
            start = time.time()
            request_no_optimizer(requested_domain, 1)
            tot += time.time() - start
        print('With no optimizer : %f elapsed' % (tot))
        
        tot = 0
        for _ in range(N):
            requested_domain = choice(domains)
            start = time.time()
            request_optimizer(requested_domain, 1)
            tot += time.time() - start
        print('With optimizer : %f elapsed' % (tot))
        print('For %d requests in %d documents' % (N, len(domains)))

    if sys.argv[1] == 'insert':
        start = time.time()
        insert_no_optimizer()
        print('With no optimizer : %f secs elapsed' % (time.time() - start))
        
        start = time.time()
        insert_optimizer()
        print('With optimizer : %f secs elapsed' % (time.time() - start))
        print('For %d documents' % (len(domains)))