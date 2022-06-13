from datetime import datetime
import shutil
import os

from stix2arango.exceptions import (
    PackageMissing,
    DumpFailed)
from stix2arango.postgresql import PostgresOptimizer



class StorageParadigm:
    """An abstract class to specify how to name collection.
    Depending on the feed and date.
    """
    def __init__(self):
        pass

    def get_collection_name(self, feed):
        """Return the name of the collection to use for the given feed.

        Args:
            feed (stix2arango.feed.Feed): The feed to use.

        Returns:
            str: The name of the collection to use.
        """
        raise NotImplementedError()


class TimeBased(StorageParadigm):
    def get_collection_name(self, feed):
        str_timestamp = str(int(feed.date.timestamp()))
        return feed.feed_name + '_' + str_timestamp


class Grouped(StorageParadigm):
    def get_collection_name(self, feed):
        return feed.feed_name + '_grouped'


class Static(StorageParadigm):
    def get_collection_name(self, feed):
        return feed.feed_name + '_static'


class GroupedByTime(StorageParadigm):
    def __init__(self, nb_day):
        super().__init__()
        self.nb_day = nb_day

    def get_collection_name(self, feed):
        date_rounded = feed.date.timestamp() - \
             (feed.date.timestamp() % (self.nb_day * 24 * 3600))
        str_timestamp = str(int(date_rounded))
        return feed.feed_name + '_' + str_timestamp


# every insertion, a new collection is created
TIME_BASED = TimeBased()
# every data inserted goes to the same collection
GROUPED = Grouped()
# every data inserted in a month goes in the same collection
GROUPED_BY_MONTH = GroupedByTime(30)
# every data inserted in a day goes in the same collection
GROUPED_BY_DAY = GroupedByTime(1)
# every data inserted in a week goes in the same collection
GROUPED_BY_WEEK = GroupedByTime(7)
# every data inserted goes to the same collection but at each insertion, the collection is emptied
STATIC = Static()

STORAGE_PARADIGMS = [
    TIME_BASED,
    GROUPED,
    GROUPED_BY_MONTH,
    GROUPED_BY_DAY,
    GROUPED_BY_WEEK,
    STATIC
]


def snapshot(
        db_host,
        db_port,
        db_user,
        db_pass,
        db_name,
        output_dir,
        feed_list,
        date=None):
    """Take a snapshot from database of last data from feed_list at date.

    Args:
        db_host (str): database host
        db_port (str): database port
        db_user (str): database user
        db_pass (str): database password
        db_name (str): database name
        output_dir (str): directory where to store the snapshot
        feed_list (str): list of feeds to snapshot
        date (str, optional): Date from which take snapshot. Defaults to now.

    Raises:
        PackageMissing: [description]
        DumpFailed: [description]
    """
    if shutil.which('arangodump') is None:
        raise PackageMissing('arangodump')
    if PostgresOptimizer.db_host and shutil.which('pg_dump') is None:
        raise PackageMissing('pg_dump')
    if not date:
        date = datetime.now()
    col_list = []
    aql_command = """arangodump --output-directory {} \
            --overwrite true --server.endpoint tcp://{}:{} \
            --server.username {} \
            --server.password {} \
            --server.database {}""".format(
                output_dir,
                db_host,
                db_port,
                db_user,
                db_pass,
                db_name
            )

    col_list = [feed.storage_paradigm.get_collection_name(feed)
                for feed in feed_list]

    col_list +=  ['edge_' + feed.storage_paradigm.get_collection_name(feed)
                for feed in feed_list ]
    
    # ! quick fix, meta_history was not dumped
    col_list.append('meta_history')
    
    for col in col_list:
        aql_command += " --collection {}".format(col)
    r = os.system(aql_command)
    if r != 0:
        raise DumpFailed('arangodump exit with a non-zero status ' + aql_command)
    if PostgresOptimizer.db_host:
        pg_command = """pg_dump --dbname=postgresql://%s:%s@%s:%s/%s --format=t --no-comments --file %s/pg_dump.tgz """
        pg_command = pg_command % (
            PostgresOptimizer.db_user,
            PostgresOptimizer.db_pass,
            PostgresOptimizer.db_host,
            PostgresOptimizer.db_port,
            PostgresOptimizer.db_name,
            output_dir
        )
        table_list = [feed.storage_paradigm.get_collection_name(feed)
                for feed in feed_list if len(feed.optimizers) > 0]
        for t in table_list:
            pg_command += ' -t ' + t + '*'
        pg_command = pg_command.replace('\n','')
        r = os.system(pg_command)

def snapshot_restore(
        db_host,
        db_port,
        db_user,
        db_pass,
        db_name,
        input_dir
        ):
    """Restore data from a snapshot.

    Args:
        db_host (str): database host
        db_port (str): database port
        db_user (str): database user
        db_pass (str): database password
        db_name (str): database name
        input_dir (str): directory containing the snapshot

    Raises:
        ArangoDumpNotInstalled: if arangodb-client is not installed
        DumpFailed: if arangodump failed
    """

    if shutil.which('arangorestore') is None:
        raise PackageMissing('arangorestore')
    if PostgresOptimizer.db_host and shutil.wich('pg_restore' is None):
        raise PackageMissing('pg_restore')
    command = """arangorestore --server.endpoint tcp://{}:{} \
            --server.username {} \
            --server.password {} \
            --server.database {} \
            --input-directory {}""".format(
                db_host,
                db_port,
                db_user,
                db_pass,
                db_name,
                input_dir
            )
    r = os.system(command)
    if r != 0:
        raise DumpFailed('arangorestore exit with a non-zero status')
    if PostgresOptimizer.db_host:
        pg_command = """pg_restore --dbname=postgresql://%s:%s@%s:%s/%s %s/pg_dump.tgz"""
        pg_command = pg_command % (
            PostgresOptimizer.db_user,
            PostgresOptimizer.db_pass,
            PostgresOptimizer.db_host,
            PostgresOptimizer.db_port,
            PostgresOptimizer.db_name,
            input_dir
        )
        r = os.system(pg_command)
        if r != 0:
            raise DumpFailed('pg_restore exit with a non-zero status')