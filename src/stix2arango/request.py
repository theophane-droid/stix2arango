from pyArango.query import AQLQuery

from stix2arango.feed import Feed
from stix2arango.storage import get_collection_name

class Request:
    def __init__(self, db_conn, date):
        self.db_conn = db_conn
        self.date = date


    def remove_arango_fields(self, object):
        return {k:v for k, v in object.items() if not k.startswith('_')}


    def request_one_feed(self, feed, values):
        col_name = get_collection_name(feed)
        aql_prefix = 'FOR f IN {}  '.format(col_name)
        aql_suffix = ' RETURN f'
        aql_middle = 'FILTER '
        for k, v in values.items():
            aql_middle += 'f.{} == "{}" AND '.format(k, v)
        aql_middle = aql_middle[:-4]

        aql = aql_prefix + aql_middle + aql_suffix
        matched_results = self.db_conn.AQLQuery(aql, raw_results=True)
        results = []
        for r in matched_results:
            vertexes = self.graph_traversal(r['_id'])
            for vertex in vertexes:
                vertex = vertex.getStore()
                vertex = self.remove_arango_fields(vertex)
                vertex['x_feed'] = feed.feed_name
                results.append(self.remove_arango_fields(vertex))
        return results


    def request(self, tags=[], values={}):
        feeds = Feed.get_last_feeds(self.db_conn, self.date)
        feeds = [feed for feed in feeds if set(tags).issubset(set(feed.tags))]
        results = []
        for feed in feeds:
            results.extend(self.request_one_feed(feed, values))
        return results
    

    def graph_traversal(self, id, max_depth=5):
        col_name = id.split('/')[0]
        aql = """FOR v, e in 0..{} ANY '{}' {} RETURN v""".format(max_depth, id, 'edge_' + col_name)
        return self.db_conn.AQLQuery(aql, raw_results=True)