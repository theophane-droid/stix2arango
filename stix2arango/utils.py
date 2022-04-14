from random import randint, Random
import uuid
import re
import os

from pyArango.connection import Connection
from pyArango.theExceptions import CreationError
from stix2 import parse

from stix2arango.exceptions import MergeFailedException

SPECIAL_CHARS = '[()]=<>'
STRING_CHARS = '"\''
SEPARATOR_CHARS = ' \t'


def remove_redondant_parenthesis(expression):
    n = len(expression)
    if (n == 0):
        return
    check = [True] * (n)
    record = []
    result = ""
    i = 0
    while (i < n):
        if (expression[i] == ')'):
            if ((len(record) == 0)):
                return
            elif (expression[record[-1]] == '('):
                check[i] = False
                check[record[-1]] = False
                record.pop()
            else:
                while (not(len(record) == 0) and
                        expression[record[-1]] != '('):
                    record.pop()
                if ((len(record) == 0)):
                    return
                record.pop()
        else:
            record.append(i)
        i += 1
    j = 0
    while (j < n):
        if (check[j]):
            result = result + str(expression[j])
        j += 1
    return result


def check_if_expression_is_balanced(pattern):
    open_tup = tuple('([')
    close_tup = tuple(')]')
    map = dict(zip(open_tup, close_tup))
    queue = []
    is_in_string = False
    string_opener = ''

    for i in pattern:
        if i in STRING_CHARS:
            if not is_in_string:
                is_in_string = True
                string_opener = i
                queue.append(string_opener)
            elif i == string_opener:
                is_in_string = False
                if not queue or string_opener != queue.pop():
                    return False
        elif i in open_tup and not is_in_string:
            queue.append(map[i])
        elif i in close_tup and not is_in_string:
            if not queue or i != queue.pop():
                return False
    if not queue:
        return True
    else:
        return


def remove_unused_space(aql):
    is_in_string = False
    string_opener = ''
    last_char = None
    result = ''
    for c in aql:
        if c in STRING_CHARS:
            if not is_in_string:
                is_in_string = True
                string_opener = c
            elif c == string_opener:
                is_in_string = False
        if c in ')]' and not is_in_string and last_char and last_char == ' ':
            result = result[:-1]
        if c == ' ' and not is_in_string:
            if last_char and (last_char != ' ' and last_char not in '(['):
                result += ' '
        else:
            result += c
        last_char = c
    return result


def update_id_for_sdo(sdo):
    """Update sdo id with a reproducible uuid base on fields

    Args:
        sdo (sdo): Stix sdo object

    Returns:
        sdo: updated sdo stix object
    """
    if sdo.type == 'relationship':
        raise TypeError('object should not be a relationship')

    sdo = dict(sdo)
    exclude_field = ['created', 'modified', 'spec_version', 'id']
    seed = {k:v for k,v in sdo.items() \
        if 'ref' not in k and k[:2] != 'x_' and k not in exclude_field}
    rd = Random()
    rd.seed(str(seed))

    _id = uuid.UUID(int=rd.getrandbits(128), version=4)
    sdo['id'] = sdo['type'] + "--" + str(_id)
    return parse(sdo, allow_custom=True)


def update_uid_for_obj_list(l_obj):
    """Replace sdo id by deterministic id and replace id in relations and references

    Args:
        l_obj (list) : list of stix objects
    
    Returns:
        list: list of updated sdo stix object
    """
    id_transform = {}
    updated_l_obj = []
    for sdo in l_obj:
        if sdo.type != 'relationship':
            old_id = sdo.id
            sdo = update_id_for_sdo(sdo)
            new_id = sdo.id
            id_transform[old_id] = new_id
        updated_l_obj.append(dict(sdo))
    for obj in updated_l_obj:
        for key, value in obj.items():
            if key.endswith('ref'):
                if value in id_transform:
                    obj[key] = id_transform[value]
            if key.endswith('refs'):
                obj[key] = [ id_transform[v] if v in id_transform  else v for v in obj[key] ]
    return [parse(obj) for obj in updated_l_obj]



def merge_obj(obj1, obj2):
    obj1 = dict(obj1)
    for key, value in obj2.items():
        if key not in obj1:
            obj1[key] = value
        elif type(value) == list:
            obj1[key] += [v for v in obj2[key] if v not in obj1[key]]
        elif value != obj1[key]:
            raise MergeFailedException(obj1['type'])
    return obj1

def merge_obj_list(l_obj):
    i = 0
    while i < len(l_obj):
        j = i + 1
        while j < len(l_obj):
            if 'id' in l_obj[i] and 'id' in l_obj[j] and\
                l_obj[i]['id'] == l_obj[j]['id']:
                try:
                    l_obj[i] = merge_obj(l_obj[i], l_obj[j])
                    del l_obj[j]
                except MergeFailedException:
                    j += 1
            else:
                j += 1
        i += 1
    return

class ArangoUser:
    def __init__(self, name, password, arangoURL):
        self.id = randint(0, 1000000)
        self.name = name
        self.password = password
        self.url = arangoURL

    def to_json(self):        
        return {"name": self.name,
                "email": self.email}

    def is_authenticated(self):
        try:
            Connection(
                username=self.name,
                password=self.password,
                arangoURL=self.url
            )
            return True
        except:
            return False

    def is_active(self):  
        return True           

    def is_anonymous(self):
        return False          

    def get_id(self):         
        return str(self.id)


def is_valid_feed_name(name):
    return re.match('^[a-zA-Z0-9_]*$', name) and len(name) <= 30

def get_database():
    try:
        password = os.environ['ARANGO_ROOT_PASSWORD']
        url = os.environ['ARANGO_URL']
    except KeyError:
        password = 'changeme'
        url = 'http://localhost:8529'
    db_conn = Connection(username='root', password=password, arangoURL=url)
    try:
        database = db_conn.createDatabase('stix2arango')
    except CreationError:
        database = db_conn['stix2arango']
    return database

import collections


def deep_dict_update(source, overrides):
    """
    Update a nested dictionary or similar mapping.
    Modify ``source`` in place.
    """
    for key, value in overrides.items():
        if isinstance(value, collections.Mapping) and value:
            returned = deep_dict_update(source.get(key, {}), value)
            source[key] = returned
        else:
            source[key] = overrides[key]
    return source