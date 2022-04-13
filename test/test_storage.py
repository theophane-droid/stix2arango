import unittest

import sys, os
sys.path.insert(0, '/app')
from pyArango.connection import *

from stix2arango.storage import snapshot, snapshot_restore
from stix2arango.feed import Feed
from stix2arango.utils import get_database


class TestSnapshot(unittest.TestCase):
    def setUp(self):
        self.host, self.port ='localhost', 8529
        self.user = 'root'
        self.password = 'changeme'
        self.database = 'stix2arango'
        self.db_conn = get_database()
        self.feeds = Feed.get_last_feeds(self.db_conn, datetime.now())
    
    def test_snapshot(self):
        snapshot(
            self.host,
            self.port,
            self.user,
            self.password,
            self.database,
            '~/dump', 
            self.feeds
        )
        snapshot_restore(
            self.host,
            self.port,
            self.user,
            self.password,
            self.database,
            '~/dump')