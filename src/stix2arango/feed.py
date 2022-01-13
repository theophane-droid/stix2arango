from pyArango.theExceptions import CreationError
from datetime import datetime

from stix2arango.storage import TIME_BASED, GROUPED, get_collection_name
from stix2arango import stix_modifiers
from stix2arango.version import __version__


class Feed:
    db_conn = None
    feed_name = None
    tags = None
    date = None
    storage_paradigm = None
    edge_to_insert = []
    obj_inserted = {}
    inserted_stix_types = []
    def __init__(self, db_conn, feed_name, tags=[], date=None, storage_paradigm=TIME_BASED):
        self.db_conn = db_conn
        self.feed_name = feed_name
        self.relations_to_insert = []
        self.tags = tags
        if date:
            self.date = date
        else:
            self.date = datetime.now()
        self.storage_paradigm = storage_paradigm
        self.version = __version__


    def insert_one_object(self, object, colname):
        if object.type in stix_modifiers:
            args = dict(object)
            print(args)
            object = stix_modifiers[object.type](**args)

        if object.type not in self.inserted_stix_types:
            self.inserted_stix_types.append(object.type)

        try:
            self.db_conn.createCollection(className='Collection', name=colname)
        except CreationError:
            pass
        col = self.db_conn[colname]
        object = dict(object)
        # check if there if there is relation in the object
        for key in object:
            suffix = key.split('_')[-1] 
            if suffix == 'ref' :
                self.edge_to_insert.append((object['id'], object[key], key))
            elif suffix == 'refs':
                for ref in object[key]:
                    self.edge_to_insert.append((object['id'], ref, key))
        # save doc
        doc = col.createDocument(object)
        doc.save()
        self.obj_inserted[object['id']] = doc
        return doc


    def insert_stix_object_in_arango(self, l_object):
        self.save_feed()
        colname = get_collection_name(self)
        for object in l_object:
            self.insert_one_object(object, colname)
        self.insert_edge_in_arango()


    def insert_edge_in_arango(self):
        colname = 'edge_' + get_collection_name(self)
        try:
            self.db_conn.createCollection(className='Edges', name=colname)
        except CreationError:
            pass
        col = self.db_conn[colname]
        for src, dest, label in self.edge_to_insert:
            edge = {"_from": self.obj_inserted[src]._id, 
                    "_to": self.obj_inserted[dest]._id, 
                    "label": label}
            doc = col.createDocument(edge)
            doc.save()
        self.edge_to_insert = []


    def save_feed(self):
        colname = 'meta_history'
        try:
            self.db_conn.createCollection(className='Collection', name=colname)
        except CreationError as e:
            pass
        col = self.db_conn[colname]
        doc = col.createDocument(self.__dict__())
        doc.save()
        return doc
    

    def __dict__(self):
        return {
            'feed_name': self.feed_name,
            'date': int(self.date.timestamp()),
            'tags': self.tags,
            'storage_paradigm': self.storage_paradigm,
            'version': self.version,
            'inserted_stix_types': self.inserted_stix_types
        }
    

    def __str__(self):
        return 'Feed: {}'.format(self.__dict__())


    def get_last_feeds(db_conn, d_before):
        # get all docs from meta_history collection
        colname = 'meta_history'
        col = db_conn[colname]
        docs = col.fetchAll()

        results_feeds = {}
        for doc in docs:
            date = datetime.fromtimestamp(doc['date'])
            feed = Feed(db_conn, doc['feed_name'], doc['tags'], date, doc['storage_paradigm'])
            if date.timestamp() < d_before.timestamp():
                if feed.feed_name not in results_feeds:
                    results_feeds[feed.feed_name] = feed
                elif feed.date > results_feeds[feed.feed_name].date:
                    results_feeds[feed.feed_name] = feed
        return [v for v in results_feeds.values()]