

class UnknownStorageParadigm(Exception):
    """
    Exception raised when the storage paradigm is unknown.
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return 'Unknown storage paradigm: {}'.format(self.value)