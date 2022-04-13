# ensure that we use the current version of the package
import unittest

import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '.')


from stix2 import IPv4Address, AutonomousSystem, Identity
from stix2 import Relationship, Incident, IPv6Address
from pyArango.connection import *
from stix2arango.feed import Feed, vaccum
from stix2arango.request import Request
from stix2arango.storage import GROUPED, GROUPED_BY_MONTH, TIME_BASED, STATIC
from stix2arango.utils import get_database
from stix2arango import stix_modifiers
from datetime import datetime, timedelta

db_conn = get_database()

class GeneralTest(unittest.TestCase):
    def setUp(self):
        self.autonomous_system = AutonomousSystem(number=1234, name='Google')
        self.ipv4 = IPv4Address(value='97.8.8.8', belongs_to_refs=[self.autonomous_system.id])
        self.identity = Identity(name='My grand mother', identity_class='individual')
        self.relation = Relationship(source_ref=self.identity.id, target_ref=self.ipv4.id, relationship_type='attributed-to')
        self.ipv4_net = IPv4Address(value='97.8.8.0/24', belongs_to_refs=[self.autonomous_system.id])

        self.ipv6 = IPv6Address(value='2001:0db8:85a3:0000:0000:8a2e:0370:7334', belongs_to_refs=[self.autonomous_system.id])

        self.feed = Feed(db_conn, 'timefeed', tags=['paynoattention', 'time_based'], storage_paradigm=TIME_BASED)
    
    def test_insert(self):
        self.feed.insert_stix_object_in_arango([self.ipv4,
                                           self.autonomous_system,
                                           self.identity,
                                           self.relation,
                                           self.ipv4_net,
                                           self.ipv6])
    
    
    # test with grouped paradigm
    def test_grouped(self):
        autonomous_system = AutonomousSystem(number=1234, name='Google')
        ipv4 = IPv4Address(value='97.8.8.8', belongs_to_refs=[autonomous_system.id])
        identity = Identity(name='My grand mother', identity_class='individual')
        relation = Relationship(source_ref=identity.id, target_ref=ipv4.id, relationship_type='attributed-to')

        feed = Feed(db_conn, 'groupedfeed', tags=['paynoattention', 'grouped'], storage_paradigm=GROUPED)
        feed.insert_stix_object_in_arango([ipv4, autonomous_system, identity, relation])

        # test with grouped-by-month paradigm
        feed = Feed(db_conn, 'grouped_by_month_feed', tags=['paynoattention', 'dogstory'], storage_paradigm=GROUPED_BY_MONTH)
        identity = Identity(name='My dog', identity_class='individual')
        course_of_action = Incident(name='INC 1078', description='My dog barked on neighbors')
        relation = Relationship(source_ref=course_of_action.id, target_ref=identity.id, relationship_type='attributed-to')
        feed.insert_stix_object_in_arango([identity, course_of_action, relation])

        feeds = Feed.get_last_feeds(db_conn, datetime(2022, 12, 12))

    def test_request_1(self):
        request = Request(db_conn, datetime.now())
        results = request.request("  [ipv4-addr:x_ip  =   '97.8.8.8' ]  ",
                            tags=['time_based'], max_depth=1)
        self.assertEqual(len(results), 5)

        request = Request(db_conn, datetime.now())
        results = request.request("""[    identity:name = 'My grand mother']""",
                            tags=['time_based'])
        self.assertEqual(len(results), 3)


    def test_request_2(self):
        feed = Feed(db_conn, 'patterntestfeed', tags=['patterntestfeed'], storage_paradigm=TIME_BASED, )
        ipv4 = IPv4Address(value='97.8.1.0/24')
        feed.insert_stix_object_in_arango([ipv4])
        request = Request(db_conn, datetime.now())
        results = request.request("[ipv4-addr:x_ip='97.8.1.8']",
                            tags=['patterntestfeed'])
        self.assertEqual(len(results), 1)

        results = request.request("[ malware:name  = 'Adware'  ]",
                            tags=['pattern'])
        self.assertEqual(len(results), 0)


    def test_vaccum(self):
        feed = Feed(db_conn, 'vaccumentest', tags=['vaccum'], storage_paradigm=TIME_BASED, vaccum_date=datetime.fromtimestamp(10))
        ipv4 = IPv4Address(value='97.8.1.0/24')
        feed.insert_stix_object_in_arango([ipv4])
        vaccum(db_conn)
        feeds = Feed.get_last_feeds(db_conn, datetime(2022, 12, 12))
        for feed in feeds:
            if feed.feed_name == 'vaccumentest':
                raise Exception('Vaccum failed')


    def test_optimisation_patch(self):
        r = '[ipv4-addr:value = "mushroom" OR ipv4-addr:net != "red hot"]'
        request = Request(db_conn, datetime.now())
        results = request.request(r)
        self.assertGreater(len(results), 0)

    def test_patch_issue20(self):
        feed = Feed(db_conn, 'patch20', tags=['patch20'], storage_paradigm=TIME_BASED)
        ipv4 = IPv4Address(value='97.8.8.8')
        identity = Identity(name='My grand mother', identity_class='individual')
        feed.insert_stix_object_in_arango([ipv4, identity])
        autonomous_system = AutonomousSystem(number=1234, name='Google')
        ipv4 = IPv4Address(value='97.8.8.8', belongs_to_refs=[autonomous_system.id])
        feed.insert_stix_object_in_arango([ipv4, autonomous_system])
        feeds = Feed.get_last_feeds(db_conn, datetime.now())
        for f in feeds:
            if f.feed_name == 'patch20':
                self.assertEqual(f.inserted_stix_types,
                                ['ipv4-addr', 'identity', 'autonomous-system']
                                )
    def test_static_storage_issue_21(self):
        feed = Feed(db_conn, 'staticfeed', storage_paradigm=STATIC)
        ipv4 = IPv4Address(value='97.8.8.8')
        feed.insert_stix_object_in_arango([ipv4])
        col_name = feed.storage_paradigm.get_collection_name(feed)
        self.assertEqual(db_conn[col_name].count(), 1)
        feed = Feed(db_conn, 'staticfeed', storage_paradigm=STATIC)
        feed.insert_stix_object_in_arango([self.identity, self.autonomous_system])
        self.assertEqual(db_conn[col_name].count(), 2)

    def test_grouped_search(self):
        request = Request(db_conn, datetime.now() - timedelta(days=1000))
        r = request.request("[identity:name = 'My grand mother']", tags=['grouped'])
        self.assertGreater(len(r), 0)