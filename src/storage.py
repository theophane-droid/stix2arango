from datetime import datetime

from exceptions import UnknownStorageParadigm

TIME_BASED = 1
GROUPED = 2

def get_collection_name(feed):
    if feed.storage_paradigm == TIME_BASED:
        str_timestamp = str(int(feed.date.timestamp()))
        return feed.feed_name + '_' + str_timestamp
    elif feed.storage_paradigm == GROUPED:
        return feed.feed_name + '_grouped'
    else:
        raise UnknownStorageParadigm(storage_paradigm)