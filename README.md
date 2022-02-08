# stix2arango

Stix2arango provide an abstration layer on arangoDB to store and query stix2 objects.

stix2arango has numerous features:
- Native support for stix2 objects
- Pre-process stix2 objects before storing them
- Organization of data by information provider (feed)
- Tag feeds with one or more labels
- Query stix2 objects using stix2 patterning language
- Automatic index creation to optimize query performance
- Ability to manage your data over time and to request knowledge at a specified date
- Clean up your old data
- Take/restore snapshots of your data

## 1. Insert STIX2.1 in arangoDB

Here an example about how to insert stix2.1 objects in ArangoDB :

```python3
from stix2arango.feed import Feed
from stix2arango.storage import GROUPED, GROUPED_BY_MONTH, TIME_BASED

db_conn = my_arango_db_connection

# create a TIME_BASED Feed object
feed = Feed(db_conn, 'grouped_by_month_feed', tags=['paynoattention', 'dogstory'],  storage_paradigm=GROUPED_BY_MONTH)

# create Stix2.1 objects
identity = Identity(name='My dog', identity_class='individual')
course_of_action = Incident(name='INC 1078', description='My dog barked on neighbors')
relation = Relationship(source_ref=course_of_action.id, target_ref=identity.id, relationship_type='attributed-to')

# insert objects
feed.insert_stix_object_in_arango([identity, course_of_action, relation])
```

### 1.1 Feed objects

A Feed object represents a CTI feed. It can wear multiple tags. In the example above, the feed named grouped_by_month_feed wear the 'paynoattention' tag. It can store objects in different ways :

### 1.2 Storage paradigm

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

### 2. Query

You can request data using stix patterning syntax.
Currently, the following stix patterning features are supported :
- Comparaison operator : `<, <=, >, >=, =, !=, LIKE`
- Logical operator : `AND, OR`

Thus you can use patterning like :

```
[file:hash:md5 = 'a1b2c3d4e5' OR (file:hash:sha1 = 'a1b2c3d4e5' AND file.name = 'myfile.txt')]
```

With stix2arango, you can query objects like :

```python3
# first we create a request object, specifying the date before which we want to retrieve objects
request = Request(db_conn, datetime.now())
# then we query objects with the pattern we want, the tag we want and depth of the query
results = request.request("[ipv4-addr:value = '97.8.8.8']",
                        tags=['time_based'], max_depth=3)
print(results)
```

Notes :
 - max_depth is the maximum depth of the query : if you specify a depth of 0, you will get only the matching objects. If you specify a depth of 1, you will get the matching objects and their relationships. If you specify a depth of 2, you will get the matching objects, their relationships and their relationships' relationships.
 - You can specify tags to retrieve only objects carried by a feed with this tags.


### 3. Pre-calculated fields

You can use pre-calculated fields in your queries.
These fields are calculated when you insert objects in the database, then you can specify how to query them.

Example :
```python
from stix2arango.exceptions import FieldCanNotBeCalculatedBy
from stix2arango import stix_modifiers
from stix2 import IPv4Address

class IPV4Modifier(IPv4Address):
    type = 'ipv4-addr' # this specifify that we will overwrite all the 'ipv4-addr' objects
    def __init__(self, **kwargs):
        custom_properties = {}
        # we add a custom stix property if the ipv4-addr is a cidr notation
        if '/' in kwargs['value'] :
            custom_properties['x_is_cidr'] = True
        super().__init__(**kwargs, custom_properties=custom_properties)
    
    # then we overwrite the request method to add on x_is_cidr property
    def eval(field, operator, value):
        if field == 'x_is_cidr' and operator == '=' and value == 1:
            # we return an AQL condition
            return """f.x_is_cidr == True"""
        else:
            # if we raise this exception, stix2arango will use the default request method
            raise FieldCanNotBeCalculatedBy(field, type)

# Then we add the modifier to stix2arango
stix_modifiers.add_modifier(IPV4Modifier)
```

After you add the modifier in stix_midifiers, if you insert an ipv4-addr object, the field x_is_cidr will be set to True if the ipv4-addr is a cidr notation.
To use the new field in your query, you have first to insert the modifier in stix_modifiers.

## 4. Clean old inserted data

By default when a feed is created, the vaccum date is setted to date parameter + 90 days.
But you can change it using the vaccum_date parameter.

```python3
feed = Feed(db_conn, 'grouped_by_month_feed', tags=['vaccumexample'], storage_paradigm=GROUPED_BY_MONTH, date=datetime.now(), vaccum_date=datetime.now()+timedelta(days=5))
```

You can run the stix2arango.feed.vaccum method to clean database from feed objects older than vaccum_date.

## 5. The stix2arango module
stix2arango can be used as a module, to provide a simple web wrapper for stix2arango or to launch the vaccum method.

Launch a web server :
```bash
$ python3 -m stix2arango --host arango_host --port arango_port --db stix2arango --user root --password arangopasswd --action web_server
```

Launch a web server on https with self-signed certificate :
```bash
$ mkdir cert
$ openssl req -x509 -newkey rsa:4096 -nodes -out cert/cert.pem -keyout cert/key.pem -days 365
$ python3 -m stix2arango --host arango_host --port arango_port --db stix2arango --user root --password arangopasswd --action web_server --web_port 443 --ssl_key cert/key.pem --ssl_cert cert/cert.pem
```

Launch a vaccum :
```bash
$ python3 -m stix2arango --host arango_host --port arango_port --db stix2arango --user root --password arangopasswd --action vaccum
```

Take a snapshot of database :
```bash
$ python3 -m stix2arango --host arango_host --port arango_port --db stix2arango --user root --password arangopasswd* --action snapshot --snapshot_dir /tmp/snapshot
```
Warning : snapshot only takes last inserted data for each feed.

Restore a snapshot :
```bash
$ python3 -m stix2arango --host arango_host --port arango_port --db stix2arango --user root --password arangopasswd --action restore --snapshot_dir /tmp/snapshot
```

## 6. Web api

After you launched the web server as showed in 5, you can request data using api.

First authenticate on api using arangoDB creds :
```bash
$ curl -k -v -d "name=root&password=arangorootpassword" https://localhost:443/login -X POST
```

Then you get the session cookie and you can request data :
```bash
curl -k -b "session=session_id" "https://localhost:443/request?pattern=[ipv4-addr:value = '9.9.9.9']&tags=time_based&depth=3&timestamp=1644336051" -X GET
```
Only pattern argument is required. Results are returned in json.


## 7. Run test

Please install docker. Then, you can run test with the following commands : 

```
docker build env -t stix2arango
docker run -it --network host -e ARANGO_ROOT_PASSWORD=arangopasswd -e ARANGO_URL='http://localhost:8529' -v $(pwd):/app stix2arango 
```