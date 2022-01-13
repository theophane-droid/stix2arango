# stix2arango

A Python lib to store STIX2.1 in arangodb.

## 1. Run test
Please create a **.env** file at project root :

```
ARANGO_ROOT_PASSWORD=superstrongpassword
ARANGO_DATA_DIR=/storage/path
```

## 2. Insert STIX2.1 in arangoDB

Here an example about how to insert stix2.1 objects in ArangoDB :

```python3
from stix2arango.feed import Feed
from stix2arango.storage import GROUPED, GROUPED_BY_MONTH, TIME_BASED

db_conn = my_arango_db_connection

# create a TIME_BASED Feed object
feed = Feed(db_conn, 'grouped_by_month_feed', tags=['paynoattention', 'dogstory'], storage_paradigm=GROUPED_BY_MONTH)

# create Stix2.1 objects
identity = Identity(name='My dog', identity_class='individual')
course_of_action = Incident(name='INC 1078', description='My dog barked on neighbors')
relation = Relationship(source_ref=course_of_action.id, target_ref=identity.id, relationship_type='attributed-to')

# insert objects
feed.insert_stix_object_in_arango([identity, course_of_action, relation])
```

### 2.1 Feed objects

A Feed object represents a CTI feed. It can wear multiple tags. In the example above, the feed named grouped_by_month_feed wear the 'paynoattention' tag. It can store objects in different ways :

### 2.2 Storage paradigm

Insertion time is very important in stix2arango. You can use different storage paradigms :
   
    - GROUPED : feed objects are stored in a single collection
    - GROUPED_BY_MONTH : feed objects are stored in a collection per month
    - GROUPED_BY_DAY : feed objects are stored in a collection per day
    - TIME_BASED : feed objects are stored in a new collection at each new insertion date

When you request a feed object, the feed's storage paradigm is used. 

    - GROUPED : at each request every objets you inserted are consulted
    - GROUPED_BY_MONTH : at each request every objets you inserted the last month are consulted
    - GROUPED_BY_DAY : at each request every objets you inserted the last day are consulted
    - TIME_BASED : at each request only the objets you inserted at the same timestamp are consulted

### 3. Query

TODO 

### 4. Pre-calculated fields

TODO

### 5. Custom requests
 