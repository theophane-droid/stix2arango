from operator import is_
from pickle import GLOBAL
from pickletools import optimize
from pyArango.theExceptions import (
    CreationError,
    DeletionError)
from datetime import datetime, timedelta
from stix2arango.postgresql import PostgresOptimizer

from stix2arango.storage import TIME_BASED, STATIC, STORAGE_PARADIGMS, GROUPED
from stix2arango import stix_modifiers
from stix2arango.utils import is_valid_feed_name
from stix2arango import version
from stix2arango.postgresql import PostgresOptimizer
from stix2arango.exceptions import InvalidFeedName, InvalidObjectForOptimizer


def vaccum(db_conn):
    actual_date = datetime.now()
    colname = 'meta_history'
    col = db_conn[colname]
    docs = col.fetchAll()
    for doc in docs:
        date = datetime.fromtimestamp(doc['date'])
        if 'vaccum_date' in doc:
            vaccum_date = datetime.fromtimestamp(doc['vaccum_date'])
            try:
                optimizers = doc['optimizers']
            except KeyError:
                optimizers = None
            feed = Feed(
                db_conn,
                doc['feed_name'],
                doc['tags'],
                date,
                doc['storage_paradigm'],
                vaccum_date
            )
            if feed.vaccum_date != 0 and feed.vaccum_date <= actual_date:
                # remove doc
                doc.delete()

                # remove collection
                col_name = feed.storage_paradigm.get_collection_name(feed)
                edge_col_name = 'edge_' + col_name
                # remove col 
                try:
                    col = db_conn[col_name]
                    col.delete()
                except DeletionError:
                    pass
                # remove edge col
                try:
                    edge_col = db_conn[edge_col_name]
                    edge_col.delete()
                except DeletionError:
                    pass
                # remove pg table
                if optimizers:
                    for opti in optimizers:
                        if opti['class'] == 'postgresoptimizer' and \
                            PostgresOptimizer.postgres_conn:
                            table_name = feed.storage_paradigm.get_collection_name(feed) + opti['uuid']
                            sql = 'drop table if exists ' + table_name
                            cursor = PostgresOptimizer.postgres_conn.cursor()
                            cursor.execute(sql)
                            cursor.close()
                            PostgresOptimizer.postgres_conn.commit()

class Feed:
    """A Feed is a container for a set of STIX objects."""
    
    def __init__(
                    self,
                    db_conn,
                    feed_name,
                    tags=[],
                    date=None,
                    storage_paradigm=TIME_BASED,
                    vaccum_date=None,
                    inserted_stix_types=[]
                ):
        """Initialize a Feed object.

        Args:
            db_conn (pyarango database object): the database connection
            feed_name (str): the name of the feed
            tags (list, optional): the tags that the feed will carry. \
                Defaults to [].
            date (datetime, optional): date of the next insertion. \
                Defaults to now.
            storage_paradigm (int, optional): method to store/request objects.
                Defaults to TIME_BASED.
            vaccum_date (datetime, optional): date of the feed deletion. \
                Defaults to 90 days.
        """
        if(not(is_valid_feed_name(feed_name))):
            raise InvalidFeedName()

        self.obj_inserted = {}
        self.optimized_obj = {}
        self.db_conn = db_conn
        self.feed_name = feed_name
        self.relations_to_insert = []
        self.tags = tags
        self.has_been_emptied = False
        self.feed_already_saved = False
        self.key = None

        if date:
            self.date = date
        else:
            self.date = datetime.now()
        if type(storage_paradigm) == int:
            self.storage_paradigm = STORAGE_PARADIGMS[storage_paradigm - 1]
        else:
            self.storage_paradigm = storage_paradigm
        if vaccum_date:
            self.vaccum_date = vaccum_date
        else:  # if vaccum_date is not set, set it to date + 90 days
            self.vaccum_date = self.date + timedelta(days=90)
        if inserted_stix_types:
            self.inserted_stix_types = inserted_stix_types
        else:
            self.inserted_stix_types = []
        self.edge_to_insert = []
        self.optimizers = []
        self.version = version.__version__

    def drop(self):
        """
        Drop actual feed's collections
        """
        self.has_been_emptied = True
        self.feed_already_saved = False
        try:
            col_name = self.storage_paradigm.get_collection_name(self)
            self.db_conn[col_name].delete()
            self.db_conn['edge_' + col_name].delete()
        except( KeyError, DeletionError):
            pass
        colname = 'meta_history'
        col = self.db_conn[colname]
        docs = col.fetchAll()

        for doc in docs:
            if doc['feed_name'] == self.feed_name:
                doc.delete()

    def __insert_one_object(self, object, colname):
        """Insert a single object in the database.

        Args:
            object (stix object): the object to insert
            colname (str): the name of the collection

        Returns:
            pyarango doc: the stored document
        """
        object = dict(object)
        object_save = dict(object)
        if object['type'] not in self.inserted_stix_types:
            self.inserted_stix_types.append(object['type'])
            self.__update_inserted_object_list()
        
        if len(self.optimizers) > 0:
            for optimizer in self.optimizers:
                object = optimizer.delete_fields_in_object(object)
        elif object['type'] in stix_modifiers:
            args = dict(object)
            object = stix_modifiers[object['type']](**args)

        try:
            self.db_conn.createCollection(className='Collection', name=colname)
        except CreationError:
            pass
        col = self.db_conn[colname]
        # check if there if there is relation in the object
        for key in object_save:
            suffix = key.split('_')[-1]
            if suffix == 'ref':
                self.edge_to_insert.append((object_save['id'], object_save[key], key))
            elif suffix == 'refs':
                for ref in object_save[key]:
                    self.edge_to_insert.append((object_save['id'], ref, key))
        # save doc
        compact = str(object)
        try:
            doc = self.optimized_obj[compact]
            self.obj_inserted[object_save['id']] = doc
        except KeyError:
            doc = col.createDocument(object)
            doc.save()
            self.optimized_obj[compact] = doc
            self.obj_inserted[object_save['id']] = doc
        _id = doc['_id']
        for optimizer in self.optimizers:
            try:
                optimizer.insert_stix_obj(object_save, _id, self)
            except InvalidObjectForOptimizer:
                pass
        return doc

    def insert_stix_object_in_arango(self, l_object):
        """Insert a list of stix objects in the database.

        Args:
            l_object (list): the list of stix objects to insert
        """
        if self.storage_paradigm == STATIC and not(self.has_been_emptied):
            self.drop()
            for optimizer in self.optimizers:
                optimizer.drop_table(self.feed_name)
        self.db_conn.reload()
        if not self.feed_already_saved:
            self.__save_feed()
            self.feed_already_saved = True
        colname = self.storage_paradigm.get_collection_name(self)
        for object in l_object:
            self.__insert_one_object(object, colname)
        self.__insert_edge_in_arango()

    def __update_inserted_object_list(self):
        _dict = self.__dict__()
        _dict['_key'] = self.key
        aql = """REPLACE %s in meta_history """ % (_dict)
        self.db_conn.AQLQuery(aql)

    def __insert_edge_in_arango(self):
        """Insert the edges in the database."""
        colname = 'edge_' + self.storage_paradigm.get_collection_name(self)
        try:
            self.db_conn.createCollection(className='Edges', name=colname)
        except CreationError:
            pass
        col = self.db_conn[colname]
        for src, dest, label in self.edge_to_insert:
            try:
                edge = {"_from": self.obj_inserted[src]._id,
                        "_to": self.obj_inserted[dest]._id,
                        "label": label}
            except KeyError:                
                raise RuntimeError("""Error while trying to create a 
                relation with a non-existing object...""")
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
        self.key = doc._key
        return doc

    def __dict__(self):
        storage_paradigm_id = STORAGE_PARADIGMS.index(
            self.storage_paradigm
            ) + 1
        optimizers_dict = []
        for optimizer in self.optimizers:
            optimizers_dict += [optimizer.__dict__()]
        return {
            'feed_name': self.feed_name,
            'date': int(self.date.timestamp()),
            'tags': self.tags,
            'storage_paradigm': storage_paradigm_id,
            'version': self.version,
            'inserted_stix_types': self.inserted_stix_types,
            'vaccum_date': int(self.vaccum_date.timestamp()),
            'optimizers' : optimizers_dict
        }

    def __str__(self):
        return 'Feed: {}'.format(self.__dict__())

    def get_last_feeds(db_conn, d_before, postgres_conn=None):
        """Get the last feeds before a certain date.

        Args:
            db_conn (pyarango database): the database connection
            d_before (datetime): the date before which we want the feeds

        Returns:
            list: the list of feeds
        """
        # get all docs from meta_history collection
        colname = 'meta_history'
        try:
            col = db_conn[colname]
        except KeyError:
            return []
        docs = col.fetchAll()

        results_feeds = {}
        for doc in docs:
            date = datetime.fromtimestamp(doc['date'])
            if 'vaccum_date' in doc:
                vaccum_date = datetime.fromtimestamp(doc['vaccum_date'])
            else:
                vaccum_date = 0
            inserted_stix_types = None
            if 'inserted_stix_types' in doc:
                inserted_stix_types = doc['inserted_stix_types']
            feed = Feed(
                db_conn,
                doc['feed_name'],
                doc['tags'], date,
                doc['storage_paradigm'],
                vaccum_date,
                inserted_stix_types=inserted_stix_types
                )
            if 'optimizers' in doc:
                for doc_optimizer in doc['optimizers']:
                    if type(doc_optimizer) != dict:
                        continue
                    elif doc_optimizer['class'] == 'postgresoptimizer':
                        try:
                            optimizer = PostgresOptimizer(doc_optimizer['field'])
                            optimizer.uuid = doc_optimizer['uuid']
                            feed.optimizers.append(optimizer)
                        except RuntimeError:
                            pass
            if date.timestamp() < d_before.timestamp() or \
                feed.storage_paradigm in [STATIC, GROUPED]:
                if feed.feed_name not in results_feeds:
                    results_feeds[feed.feed_name] = feed
                elif feed.date > results_feeds[feed.feed_name].date:
                    results_feeds[feed.feed_name] = feed
        return [v for v in results_feeds.values()]