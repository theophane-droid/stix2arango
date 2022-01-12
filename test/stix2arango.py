import sys, os
from stix2 import IPv4Address, AutonomousSystem, Identity, Relationship, Incident
from pyArango.connection import *
from pyArango.theExceptions import CreationError

# insert root folder in sys.path
sys.path.append('/app/src')
sys.path.append('/app/test')

from feed import Feed
from request import Request
from storage import GROUPED, GROUPED_BY_MONTH

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

    print('\n\n\n\n\n> 1. Inserting data')
    # test with time-base paradigm
    autonomous_system = AutonomousSystem(number=1234, name='Google')
    ipv4 = IPv4Address(value='97.8.8.8', belongs_to_refs=[autonomous_system.id])
    identity = Identity(name='My grand mother', identity_class='individual')
    relation = Relationship(source_ref=identity.id, target_ref=ipv4.id, relationship_type='attributed-to')


    feed = Feed(db_conn, 'timefeed', tags=['paynoattention'])
    feed.insert_stix_object_in_arango([ipv4, autonomous_system, identity, relation])

    # test with grouped paradigm
    autonomous_system = AutonomousSystem(number=1234, name='Google')
    ipv4 = IPv4Address(value='97.8.8.8', belongs_to_refs=[autonomous_system.id])
    identity = Identity(name='My grand mother', identity_class='individual')
    relation = Relationship(source_ref=identity.id, target_ref=ipv4.id, relationship_type='attributed-to')

    feed = Feed(db_conn, 'groupedfeed', tags=['paynoattention'], storage_paradigm=GROUPED)
    feed.insert_stix_object_in_arango([ipv4, autonomous_system, identity, relation])

    # test with grouped-by-month paradigm
    feed = Feed(db_conn, 'grouped_by_month_feed', tags=['paynoattention', 'dogstory'], storage_paradigm=GROUPED_BY_MONTH)
    identity = Identity(name='My dog', identity_class='individual')
    course_of_action = Incident(name='INC 1078', description='My dog barked on neighbors')
    relation = Relationship(source_ref=course_of_action.id, target_ref=identity.id, relationship_type='attributed-to')
    feed.insert_stix_object_in_arango([identity, course_of_action, relation])

    feeds = Feed.get_last_feeds(db_conn, datetime(2022, 12, 12))
    for feed in feeds:
        print(feed)

    print('\n\n\n\n\n> 2. Getting data')
    request = Request(db_conn, datetime.now())
    results = request.request(tags=['dogstory'],
                             values={'type': 'identity',
                                    'name' : 'My dog'})
    print(results)