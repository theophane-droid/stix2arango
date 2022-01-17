from pyArango.theExceptions import CreationError
from datetime import datetime

from stix2arango.storage import TIME_BASED, GROUPED, get_collection_name
from stix2arango import stix_modifiers
from stix2arango.version import __version__


class Feed:
    """A Feed is a container for a set of STIX objects."""
    db_conn = None
    feed_name = None
    tags = None
    date = None
    storage_paradigm = None
    edge_to_insert = []
    obj_inserted = {}
    inserted_stix_types = []
    def __init__(self, db_conn, feed_name, tags=[], date=None, storage_paradigm=TIME_BASED):
        """Initialize a Feed object.

        Args:
            db_conn (pyarango database object): the database connection
            feed_name (str): the name of the feed
            tags (list, optional): the tags that the feed will carry. Defaults to [].
            date (datetime, optional): date of the next insertion. Defaults to now.
            storage_paradigm (int, optional): explain to stix2arango how to store/request objects depending on time. Defaults to TIME_BASED.
        """
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


    def __insert_one_object(self, object, colname):
        """Insert a single object in the database.

        Args:
            object (stix object): the object to insert
            colname (str): the name of the collection

        Returns:
            pyarango doc: the stored document
        """
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
        """Insert a list of stix objects in the database.

        Args:
            l_object (list): the list of stix objects to insert
        """
        self.__save_feed()
        colname = get_collection_name(self)
        for object in l_object:
            self.__insert_one_object(object, colname)
        self.__insert_edge_in_arango()


    def __insert_edge_in_arango(self):
        """Insert the edges in the database."""
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


    def __save_feed(self):
        """Save the feed in the database."""
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
        """Get the last feeds before a certain date.

        Args:
            db_conn (pyarango database): the database connection
            d_before (datetime): the date before which we want the feeds

        Returns:
            list: the list of feeds
        """        
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