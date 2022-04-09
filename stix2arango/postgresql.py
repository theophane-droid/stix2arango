from storage import get_collection_name

import psycopg2
import uuid

class PostgresOptimizer:
    def __init__(self, posgres_conn):
        self.posgres_conn = posgres_conn
        self.uuid = str(uuid.uuid4())
        self.table_name = None

    def create_table(self, feed, fields):
        self.table_name = get_collection_name(feed.feed_name) + self.uuid
        cursor = posgres_conn.cursor()
        curso.execute('create table %s', self.table_name)



    def __dict__(self):
        return {}