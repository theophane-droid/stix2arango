import unittest

import sys, os
from requests import request
sys.path.insert(0, '/app')

from stix2 import IPv4Address, AutonomousSystem, Identity, Relationship, Incident, IPv6Address
from pyArango.connection import *
from pyArango.theExceptions import CreationError

from stix2arango.feed import Feed, vaccum
from stix2arango.request import Request, pattern_compil
from stix2arango.storage import GROUPED, GROUPED_BY_MONTH, TIME_BASED
from stix2arango import stix_modifiers
from stix2arango.exceptions import (PatternAlreadyContainsType,
                                    MalformatedExpression,
                                    FieldCanNotBeCalculatedBy)
from stix2arango.utils import get_database
from datetime import datetime


class TestRequest(unittest.TestCase):
    def setUp(self):
        self.db_conn = get_database()
        self.request = Request(self.db_conn, datetime.now())

    # def test_index(self, col_name):
    #     print('\n\n> Test index creation')
    #     aql = 'FOR doc IN %s filter doc.a.b==1 RETURN doc' % (col_name)
    #     self.test_one_index(col_name, aql)
    #     aql = 'FOR doc IN %s filter doc.a.b==1 OR doc.a.b.c==2 RETURN doc' % (col_name)
    #     self.test_one_index(col_name, aql)
    #     aql = 'FOR doc IN %s filter doc.b.b>=1 OR doc.c.d.c==2 RETURN doc' % (col_name)
    #     self.test_one_index(col_name, aql)
    #     aql = """FOR doc IN %s filter doc.b>=1 AND doc.b==2 RETURN doc""" % (col_name)
    #     self.test_one_index(col_name, aql)
    #     aql = """FOR doc IN %s filter doc.d.b>=1 
    #         AND (doc.d.d.c==2 OR doc.e.n.n<2 AND (doc.e.n!=2)) RETURN doc""" % (col_name)
    #     self.test_one_index(col_name, aql)
    #     print('OK')


    # def test_one_index(self, col_name, aql):
    #     query = self.db_conn.AQLQuery(aql)
    #     str_explain = str(query.explain())
    #     with self.assertRaises(AssertionError):
    #         assert('filter' in str_explain)
            
    #     request._create_index_from_query(col_name, aql)
    #     aql = 'FOR doc IN %s filter doc.a.b==1 RETURN doc' % (col_name)
    #     query = self.db_conn.AQLQuery(aql)
    #     str_explain = query.explain()
    #     try:
    #         assert(not 'filter' in str_explain)
    #     except:
            # pass
    def test_pattern_compil(self):
        self.one_pattern_test(
            '[ipv4-addr:value = "mushroom"]',
            except_str='(f.value == "mushroom") AND f.type == "ipv4-addr"'
        )
        self.one_pattern_test(
            "[ipv4-addr:value = 'mushroom']",
            except_str="""(f.value == 'mushroom') AND f.type == \"ipv4-addr\""""
        )
        # simple quote with special chars
        self.one_pattern_test(
            "[ipv4-addr:value = '   #=(% %*=><$=\"']", 
            except_str="(f.value == '   #=(% %*=><$=\"') AND f.type == \"ipv4-addr\""
        )
        # double quote with special chars
        self.one_pattern_test(
            '[ipv4-addr:value = "   #=(% %*=><$=\'"]', 
            except_str='(f.value == "   #=(% %*=><$=\'") AND f.type == \"ipv4-addr\"'
        )
        # parentheisis-ception
        self.one_pattern_test(
            '([[[ipv4-addr:value = "mushroom"]]])'
        )
        # unbalanced parenthesis
        self.one_pattern_test(
            '([([ipv4-addr:value = "mushroom"]]])',
            except_error=MalformatedExpression
        )
        # space in pattern
        self.one_pattern_test(
            '[[ ipv4-addr:value        = "mushroom "   ]]',
            except_str='(f.value == "mushroom ") AND f.type == "ipv4-addr"'
            
        )
        # space in pattern 2
        self.one_pattern_test(
            '[[ ipv4-addr :value        = "mushroom "   ]]',
            except_error=MalformatedExpression
        )

        # logical operator + comparaison operator
        self.one_pattern_test(
            '[ipv4-addr:x_value1 <= 2 AND (ipv4-addr:value != "mushroom" OR ipv4-addr:x_value2 > 1)]',
        )
        print('OK')

    def one_pattern_test(self, pattern, except_error=False, except_str=None):
        if except_error:
            with self.assertRaises (except_error):
                pattern_compil(pattern)
        else:
            result = pattern_compil(pattern)
            if except_str:
                self.assertTrue(except_str in str(result))
            else:
                self.assertTrue(result)
    
    def test_like(self):
        print('\n\n> Like test')
        feed = Feed(self.db_conn, 'like_test', storage_paradigm=TIME_BASED)
        id = Identity(name='PowerRangers', identity_class='group')
        feed.insert_stix_object_in_arango([id])
        pattern = "[identity:name LIKE '%Rangers']"
        results = self.request.request_one_feed(feed, pattern)
        assert(len(results) > 0)
        
        pattern = "[identity:name LIKE 'P%s']"
        results = self.request.request_one_feed(feed, pattern)
        assert(len(results) > 0)
        
        pattern = "[identity:name LIKE '_owerRangers']"
        results = self.request.request_one_feed(feed, pattern)
        assert(len(results) > 0)
        
        pattern = "[identity:name LIKE 'aaa%']"
        results = self.request.request_one_feed(feed, pattern)
        assert(len(results) == 0)
        print('OK')

    # def remove_tests():
    #     colname = 'meta_history'
    #     try:
    #         col = db_conn[colname]
    #     except:
    #         return
    #     docs = col.fetchAll()
    #     for doc in docs:
    #         if doc['feed_name'] == 'test_idx':
    #             doc.delete()
    #     try:
    #         db_conn['test_idx_grouped'].delete()
    #     except Exception:
    #         pass 