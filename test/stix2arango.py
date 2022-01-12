import sys, os
from stix2 import IPv4Address, AutonomousSystem, Identity, Relationship
from pyArango.connection import *
from pyArango.theExceptions import CreationError

# insert root folder in sys.path
sys.path.append('/app/src')
sys.path.append('/app/test')

from feed import Feed
from storage import GROUPED

def get_database():
    password = os.environ['ARANGO_ROOT_PASSWORD']
    db_conn = Connection(username='root', password=password, arangoURL='http://arangodb:8529')
    try:
        database = db_conn.createDatabase('stix2arango')
    except CreationError:
        database = db_conn['stix2arango']
    return database

if __name__ == "__main__":
    db_conn = get_database()

    # test with time-base paradigm
    autonomous_system = AutonomousSystem(number=1234, name='Google')
    ipv4 = IPv4Address(value='97.8.8.8', belongs_to_refs=[autonomous_system.id])
    identity = Identity(name='My grand mother', identity_class='individual')
    relation = Relationship(source_ref=identity.id, target_ref=ipv4.id, relationship_type='attributed-to')

    feed = Feed(db_conn, 'timefeed', tags={'paynoattention'})
    feed.insert_stix_object_in_arango([ipv4, autonomous_system, identity, relation])

    # test with grouped paradigm
    autonomous_system = AutonomousSystem(number=1234, name='Google')
    ipv4 = IPv4Address(value='97.8.8.8', belongs_to_refs=[autonomous_system.id])
    identity = Identity(name='My grand mother', identity_class='individual')
    relation = Relationship(source_ref=identity.id, target_ref=ipv4.id, relationship_type='attributed-to')

    feed = Feed(db_conn, 'groupedfeed', tags={'paynoattention'}, storage_paradigm=GROUPED)
    feed.insert_stix_object_in_arango([ipv4, autonomous_system, identity, relation])