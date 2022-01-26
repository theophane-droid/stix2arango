from datetime import datetime

from stix2arango.exceptions import UnknownStorageParadigm

#: every insertion, a new collection is created
TIME_BASED = 1 
#: every data inserted goes to the same collection
GROUPED = 2 
#: every data inserted in a month goes in the same collection
GROUPED_BY_MONTH = 3 
#: every data inserted in a day goes in the same collection
GROUPED_BY_DAY = 4 

def get_collection_name(feed):
    """Return the name of the collection to use for the given feed.

    Args:
        feed (stix2arango.feed.Feed): The feed to use.

    Raises:
        UnknownStorageParadigm: If the feed's storage paradigm is unknown.

    Returns:
        str: The name of the collection to use.
    """    
    if feed.storage_paradigm == TIME_BASED:
        str_timestamp = str(int(feed.date.timestamp()))
        return feed.feed_name + '_' + str_timestamp
    elif feed.storage_paradigm == GROUPED:
        return feed.feed_name + '_grouped'
    elif feed.storage_paradigm == GROUPED_BY_MONTH:
        date_rounded = datetime(feed.date.year, feed.date.month, 1)
        str_timestamp = str(int(date_rounded.timestamp()))
        return feed.feed_name + '_' + str_timestamp
    elif feed.storage_paradigm == GROUPED_BY_DAY:
        date_rounded = datetime(feed.date.year, feed.date.month, feed.date.day)
        str_timestamp = str(int(date_rounded.timestamp()))
        return feed.feed_name + '_' + str_timestamp
    else:
        raise UnknownStorageParadigm(storage_paradigm)