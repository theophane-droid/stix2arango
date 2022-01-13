from .version import __version__, __author__
from stix2arango import stixmodifier
import inspect

print('stix2arango by theophanedroid v{}'.format(__version__))

stix_modifiers = {}

for name, obj in inspect.getmembers(stixmodifier):
    if inspect.isclass(obj) and obj.__module__ == 'stix2arango.stixmodifier':
        stix_modifiers[obj.type] = obj