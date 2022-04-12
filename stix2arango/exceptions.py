class UnknownStorageParadigm(Exception):
    """
    Exception raised when the storage paradigm is unknown.
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return 'Unknown storage paradigm: {}'.format(self.value)


class PatternAlreadyContainsType(Exception):
    """
    Exception raised when a pattern request two types of cyber obervables
    ex [file:hash:md5 = '...' AND ipv4-addr:value='8.8.8.8']
    """
    def __init__(self, type1, type2):
        self.type1 = type1
        self.type2 = type2

    def __str__(self):
        return """Pattern can only contain one type of cyber observables
        You tried to request {} and {}""".format(self.type1, self.type2)


class MalformatedExpression(Exception):
    """
    Exception raised when patterning is not correct
    """
    def __str__(self):
        return """Malformated pattern expression"""


class MalformatedComparaison(Exception):
    """
    Exception raised when patterning is not correct
    """
    def __init__(self, compare_string):
        self.compare_string = compare_string

    def __str__(self):
        return """Malformated compare string: {}""".format(self.compare_string)


class FieldCanNotBeCalculatedBy(Exception):
    """
    Exception raised when a field can not be calculated by a stixmodifier
    """
    def __init__(self, field, stix_modifiers):
        self.field = field
        self.stix_modifiers = stix_modifiers

    def __str__(self):
        return """Field {} can not be calculated by stixmodifier {}""" \
            .format(self.field, self.pattern)


class ArangoDumpNotInstalled(Exception):
    """
    Exception raised when the arangodump is not installed
    and trying to dump database
    """
    def __str__(self):
        return """ArangoDump is not installed"""


class ArangoDumpFailed(Exception):
    """
    Exception raised when the arangodump failed
    """
    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return """ArangoDump failed because {}""".format(self.reason)

class MergeFailedException(Exception):
    """
    Exception raised when stix2arango failed to merge two stix objects
    """
    def __init__(self, id):
        self.id = id

    def __str__(self) -> str:
        return """Failed to merge two objects with id %s""" % (id)


class InvalidFeedName(Exception):
    """Exception raised when feed_name contains a character different from lettes, digit or underscore,
      or if its length is greater than 30       

    Args:
        Exception (_type_): _description_
    """
    def __str__(self) -> str:
        return """Feed name can contain just letters, digits and underscore and its length should be less than 30"""

class InvalidObjectForOptimizer(Exception):
    def __init__(self, type):
        self.type = type

    def __str__(self) -> str:
        return """Object of type %s cannot be indexed in this optimizer""" % (self.type)