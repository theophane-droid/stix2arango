from datetime import datetime

from stix2arango.exceptions import UnknownStorageParadigm


class StorageParadigm:
    """An abstract class to specify how to name collection depending on the feed and date.
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

class GroupedByTime(StorageParadigm):
    def __init__(self, nb_day):
        super().__init__()
        self.nb_day = nb_day

    def get_collection_name(self, feed):
        date_rounded = feed.date.timestamp() - (feed.date.timestamp() % (self.nb_day * 24 * 3600))
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

STORAGE_PARADIGMS = [
    TIME_BASED,
    GROUPED,
    GROUPED_BY_MONTH,
    GROUPED_BY_DAY,
    GROUPED_BY_WEEK
]