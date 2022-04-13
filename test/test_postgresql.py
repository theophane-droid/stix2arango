import psycopg2
import unittest
import os
import sys
from datetime import datetime

from requests import request

sys.path.insert(0, '/app')
sys.path.insert(0, '.')
sys.path.insert(0, '..')

from psycopg2.errors import UndefinedTable, InFailedSqlTransaction
from stix2 import IPv4Address, AutonomousSystem, Identity, File
from stix2 import Relationship, Incident, IPv6Address
from pyArango.connection import Connection
from pyArango.theExceptions import CreationError

from stix2arango.feed import Feed, vaccum
from stix2arango.storage import TIME_BASED, STATIC
from stix2arango.postgresql import PostgresOptimizer
from stix2arango.request import Request
from stix2arango.utils import get_database


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
    cursor.close()
    return len(results)

auth = "dbname='%s' user='%s' host='%s' password='%s'" % (db, user, host, pass_)
arango_conn = get_database()
postgres_conn = psycopg2.connect(auth)
PostgresOptimizer.postgres_conn = postgres_conn

class TestMerge(unittest.TestCase):
    def setUp(self) :
        self.feed = Feed(
            arango_conn, 
            'posgres_merge_test', 
            tags=['postgres'], 
            date=datetime.now(),
            storage_paradigm=TIME_BASED
            )
        self.optimizer = PostgresOptimizer('ipv4-addr:x_ip')
        self.feed.optimizers.append(self.optimizer)
        self.insert_obj()
        
    def insert_obj(self):
        obj_list = []
        autonomous_system_1 = AutonomousSystem(number=123, name='fake')
        autonomous_system_2 = AutonomousSystem(number=124, name='fake2')
        obj_list += [autonomous_system_1]
        obj_list += [autonomous_system_2]
        obj_list += [IPv4Address(value='97.8.1.6', belongs_to_refs=[autonomous_system_1.id])]
        obj_list += [IPv4Address(value='97.8.1.7', belongs_to_refs=[autonomous_system_1.id])]
        obj_list += [IPv4Address(value='97.8.1.8', belongs_to_refs=[autonomous_system_1.id])]
        obj_list += [IPv4Address(value='97.8.1.9', belongs_to_refs=[autonomous_system_2.id])]
        obj_list += [IPv4Address(value='97.8.1.10', belongs_to_refs=[autonomous_system_1.id])]
        obj_list += [IPv4Address(value='97.8.1.11', belongs_to_refs=[autonomous_system_2.id])]
        self.feed.insert_stix_object_in_arango(obj_list)
    
    def test_obj_merge(self):
        aql = 'for el in ' + self.feed.storage_paradigm.get_collection_name(self.feed) + ' return el'
        results = arango_conn.AQLQuery(aql, raw_results=True)

        self.assertEqual(len(results), 4)
        sql = 'select * from ' + self.optimizer.table_name + ';'
        cursor = PostgresOptimizer.postgres_conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        cursor.close()
        self.assertEqual(len(results), 6)

        request = Request(arango_conn, datetime.now())
        results = request.request("  [autonomous-system:number  = 124 ]  ",
                            max_depth=1, tags=['postgres'])
        self.assertEqual(len(results), 3)


class TestOptimizer(unittest.TestCase):
    def setUp(self):
        self.feed = Feed(
            arango_conn, 
            'posgres_test', 
            tags=['test_postgres_optimizer'], 
            date=datetime.now(),
            storage_paradigm=TIME_BASED
            )
        optimized_field0 = 'ipv4-addr:value'
        optimizer0 = PostgresOptimizer(optimized_field0)
        optimized_field1 = 'ipv4-addr:x_ip:broadcast_addr'
        optimizer1 = PostgresOptimizer(optimized_field1)
        self.feed.optimizers.append(optimizer0)
        self.insert()
        
    def insert(self):
        _autonomous_system = AutonomousSystem(number=1234, name='Google')
        self.ipv4_1 = IPv4Address(value='97.8.8.8', belongs_to_refs=[_autonomous_system.id])
        ipv4_2 = IPv4Address(value='97.8.8.0/24', belongs_to_refs=[_autonomous_system.id])
        _identity = Identity(name='My grand mother', identity_class='individual')
        _relation = Relationship(source_ref=_identity.id, target_ref=self.ipv4_1.id, relationship_type='attributed-to')
        self.feed.insert_stix_object_in_arango([_autonomous_system,self.ipv4_1 , ipv4_2, _identity, _relation])
        
    
    def test_postgres_optimizer(self):
        request = Request(arango_conn, datetime.now())
        results = request.request("  [ipv4-addr:value  =   '97.8.8.8' ]  ",
                       max_depth=0, tags=['test_postgres_optimizer'])
        print(list(self.ipv4_1.keys()))
        print(list(results[0].keys()))
        dict_1 = dict(self.ipv4_1)
        dict_2 = dict(results[0])
        del dict_2['x_tags']
        del dict_2['x_feed']
        self.assertEqual(len(dict_1.keys()), len(dict_2.keys()))         
        self.assertEqual(dict_1, dict_2)
    

class TestRemoveField(unittest.TestCase):
    def test_remove_field(self):
        optimized_field0 = 'ipv4-addr:value'
        optimizer0 = PostgresOptimizer(optimized_field0)
        obj1 = {'type': 'ipv4-addr', 'value' : 'coucou'}
        obj2 = {'type': 'domain-name', 'value' : 'coucou'}
        r = optimizer0.delete_fields_in_object(obj1)
        self.assertEqual(r, {'type': 'ipv4-addr'})
        r = optimizer0.delete_fields_in_object(obj2)
        self.assertEqual(r, {'type': 'domain-name', 'value': 'coucou'})
        obj3 = {'type': 'ipv4-addr', 'value2' : 'coucou'}
        r = optimizer0.delete_fields_in_object(obj3)


class TestVaccum(unittest.TestCase):
    def setUp(self):
        self.feed = Feed(
            arango_conn, 
            'v', 
            tags=['postgres'], 
            date=datetime.now(),
            storage_paradigm=TIME_BASED,
            vaccum_date=datetime.fromtimestamp(10)
            )
        optimized_field0 = 'ipv4-addr:value'
        self.optimizer0 = PostgresOptimizer(optimized_field0)
        self.feed.optimizers.append(self.optimizer0)
        ipv4 = IPv4Address(value='97.8.1.0/24')
        self.feed.insert_stix_object_in_arango([ipv4])

    def test_vaccum(self):
        vaccum(arango_conn)
        feeds = Feed.get_last_feeds(arango_conn, datetime(2022, 12, 12))
        feeds = [f for f in feeds if 'v' == f.feed_name]
        self.assertEqual(len(feeds), 0)


class StaticStorageTest(unittest.TestCase):
    def setUp(self):
        feed = Feed(
            arango_conn, 
            'posgres_test_static', 
            tags=['postgres'], 
            date=datetime.now(),
            storage_paradigm=STATIC
            )
        optimizer2 = PostgresOptimizer('ipv4-addr:x_ip')
        feed.optimizers.append(optimizer2)
        ipv4 = IPv4Address(value='97.8.1.9')
        feed.insert_stix_object_in_arango([ipv4])

        feed = Feed(
            arango_conn, 
            'posgres_test_static', 
            tags=['postgres'], 
            date=datetime.now(),
            storage_paradigm=STATIC
            )
        optimizer2 = PostgresOptimizer('ipv4-addr:x_ip')
        feed.optimizers.append(optimizer2)
        ipv4_1 = IPv4Address(value='97.8.1.0/24')
        ipv4_2 = IPv4Address(value='97.8.1.7')
        feed.insert_stix_object_in_arango([ipv4_1, ipv4_2])
        self.feed = feed
    
    def test_static_storage(self):
        nb_tables = get_number_of_table_for_feed(
            self.feed.feed_name, 
            PostgresOptimizer.postgres_conn.cursor()
            )
        self.assertEquals(nb_tables, 1)
        

class MatchIpOnCidrTest(unittest.TestCase):
    def setUp(self):
        self.pattern = "[ipv4-addr:x_ip = '97.8.1.7']"
        self.request = Request(arango_conn, datetime.now())
    
    def test_match_ip_on_cidr(self):
        results = self.request.request(self.pattern,
                        max_depth=0, tags=['postgres'])
        self.assertEqual(len(results), 3)


class TwoOptimizesTest(unittest.TestCase):
    def setUp(self):
        feed = Feed(
            arango_conn, 
            'postgres_2optimizers', 
            tags=['postgres_test2'], 
            date=datetime.now(),
            storage_paradigm=TIME_BASED
            )
        optimizer1 = PostgresOptimizer('ipv4-addr:x_ip')
        optimizer2 = PostgresOptimizer('autonomous-system:number')
        feed.optimizers.append(optimizer1)
        feed.optimizers.append(optimizer2)
        obj_list = []

        autonomous_system_1 = AutonomousSystem(number=123, name='fake')
        autonomous_system_2 = AutonomousSystem(number=124, name='fake2')
        obj_list += [autonomous_system_1]
        obj_list += [autonomous_system_2]
        obj_list += [IPv4Address(value='97.8.1.6', belongs_to_refs=[autonomous_system_1.id])]
        obj_list += [IPv4Address(value='97.8.1.7', belongs_to_refs=[autonomous_system_1.id])]
        obj_list += [IPv4Address(value='97.8.1.8', belongs_to_refs=[autonomous_system_1.id])]
        obj_list += [IPv4Address(value='97.8.1.9', belongs_to_refs=[autonomous_system_2.id])]
        obj_list += [IPv4Address(value='97.8.1.10', belongs_to_refs=[autonomous_system_1.id])]
        obj_list += [IPv4Address(value='97.8.1.11', belongs_to_refs=[autonomous_system_2.id])]
        self.obj_list = obj_list
        self.feed = feed
    
    def test_insert(self):
        self.feed.insert_stix_object_in_arango(self.obj_list)
    
    
    def test_request(self):
        request = Request(arango_conn, datetime.now())
        results = request.request("  [autonomous-system:number  = 124 ]  ",
                        max_depth=1, tags=['postgres_test2'])
        self.assertEqual(len(results), 3)
        results = request.request("  [ipv4-addr:x_ip = '97.8.1.6' ]  ",
                        max_depth=10, tags=['postgres_test2'])
        self.assertEqual(len(results), 2)


class TestTreesObject:
    def __init__(self):
        self.setUp()
        self.test_trees_obj()

    def setUp(self):
        self.feed = Feed(
            arango_conn, 
            'posgres_trees_obj', 
            tags=['posgres_trees_obj'], 
            date=datetime.now(),
            storage_paradigm=STATIC
            )
        self.file1 = File(name='file1', hashes={
            'md5':'e0323a9039add2978bf5b49550572c7c', 
            'sha256':'961b6dd3ede3cb8ecbaacbd68de040cd78eb2ed5889130cceb4c49268ea4d506'})
        self.file2 = File(name='file2', hashes={
            'md5':'1aabac6d068eef6a7bad3fdf50a05cc8',
            'sha256':'3b64db95cb55c763391c707108489ae18b4112d783300de38e033b4c98c3deaf'})
        self.optimizer = PostgresOptimizer('file:hashes:md5')
        self.feed.optimizers.append(self.optimizer)
        self.feed.insert_stix_object_in_arango([self.file1, self.file2])
    
    def test_trees_obj(self):
        feed = Feed(
            arango_conn, 
            'posgres_trees_obj', 
            tags=['posgres_trees_obj'], 
            date=datetime.now(),
            storage_paradigm=STATIC
            )
        file1 = File(name='file1', hashes={
            'md5':'e0323a9039add2978bf5b49550572c7c', 
            'sha256':'961b6dd3ede3cb8ecbaacbd68de040cd78eb2ed5889130cceb4c49268ea4d506'})
        file2 = File(name='file2', hashes={
            'md5':'1aabac6d068eef6a7bad3fdf50a05cc8',
            'sha256':'3b64db95cb55c763391c707108489ae18b4112d783300de38e033b4c98c3deaf'})
        optimizer = PostgresOptimizer('file:hashes:MD5')
        feed.optimizers.append(optimizer)
        feed.insert_stix_object_in_arango([file1, file2])
        request = Request(arango_conn, datetime.now())
        results = request.request(" [file:hashes:MD5 = 'e0323a9039add2978bf5b49550572c7c']  ",
                        max_depth=1, tags=['posgres_trees_obj'])
        assert(len(results) == 1)
        dict_file = dict(file1)
        assert(results[0] == dict_file)
        cursor = PostgresOptimizer.postgres_conn.cursor()
        sql = 'select * from ' + optimizer.table_name + ';'
        cursor.execute(sql)
        results = cursor.fetchall()
        cursor.close()
        self.assertEqual(len(results), 2)

TestTreesObject()