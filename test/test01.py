import sys, os
from stix2 import IPv4Address, AutonomousSystem, Identity, Relationship, Incident, IPv6Address
from pyArango.connection import *
from pyArango.theExceptions import CreationError

# insert root folder in sys.path
sys.path.append('/app/src')
sys.path.append('/app/test')

from stix2arango.feed import Feed
from stix2arango.request import Request
from stix2arango.storage import GROUPED, GROUPED_BY_MONTH, TIME_BASED
from stix2arango import stix_modifiers
def get_database():
    password = os.environ['ARANGO_ROOT_PASSWORD']
    url = os.environ['ARANGO_URL']
    db_conn = Connection(username='root', password=password, arangoURL=url)
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
    ipv4_net = IPv4Address(value='97.8.8.0/24', belongs_to_refs=[autonomous_system.id])

    ipv6 = IPv6Address(value='2001:0db8:85a3:0000:0000:8a2e:0370:7334', belongs_to_refs=[autonomous_system.id])

    feed = Feed(db_conn, 'timefeed', tags=['paynoattention', 'time_based'], storage_paradigm=TIME_BASED)
    feed.insert_stix_object_in_arango([ipv4, autonomous_system, identity, relation, ipv4_net, ipv6])

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
    results = request.request("""[    identity:name = 'My grand mother']""",
                        tags=['time_based'])
    print(results)

    request = Request(db_conn, datetime.now())
    results = request.request("  [ipv4-addr:value  =   '97.8.8.8' ]  ",
                        tags=['time_based'], max_depth=1)
    print(results)

    feed = Feed(db_conn, 'patterntestfeed', tags=['pattern'], storage_paradigm=TIME_BASED)
    ipv4 = IPv4Address(value='97.8.1.0/24')
    feed.insert_stix_object_in_arango([ipv4])
    request = Request(db_conn, datetime.now())
    results = request.request("[ ipv4-addr:x_ip  ='97.8.1.8'  ]",
                        tags=['pattern'])
    print(results)

    results = request.request("[ malware:name  = 'Adware'  ]",
                        tags=['pattern'])
    print(results)