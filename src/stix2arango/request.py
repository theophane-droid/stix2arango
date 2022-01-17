from pyArango.query import AQLQuery

from stix2arango.feed import Feed
from stix2arango.storage import get_collection_name
from stix2arango.exceptions import (PatternAlreadyContainsType, 
            MalformatedExpression, MalformatedExpression, FieldCanNotBeCalculatedBy)
from stix2arango import stix_modifiers


SPECIAL_CHARS = '[()]=<>'
STRING_CHARS = '"\''
SEPARATOR_CHARS = ' \t'

def splitter(expression):
    is_in_string = False
    string_opener = ''
    result = []
    current_word = ''
    for c in expression:
        if c in STRING_CHARS:
            if not is_in_string:
                is_in_string = True
                string_opener = c
            elif c == string_opener:
                is_in_string = False
        if c in SPECIAL_CHARS and not is_in_string:
            if len(current_word) > 0:
                result.append(current_word)
            result.append(c)
            current_word = ''
        elif c not in SEPARATOR_CHARS or is_in_string:
            current_word += c
        elif c in SEPARATOR_CHARS and not is_in_string:
            if len(current_word) > 0:
                result.append(current_word)
                current_word = ''
    if len(current_word) > 0:
        result.append(current_word)
    return result

def word_compil(word):
    if len(word) == 0:
        return word, None
    if word == '[':
        return '(', None
    if word == ']':
        return ')', None
    if word[0] == '"' or word[0] == "'":
        return word, None
    if ':' in word:
        splitted = word.split(':')
        type = splitted[0]
        for sep in SEPARATOR_CHARS:
            type = type.replace(sep, '')
        return word, type
    return word, None

def compare_compile(compare_string):
    splitted_compare = splitter(compare_string)
    if len(splitted_compare) != 3:
        raise MalformatedExpression(compare_string)
    operator = splitted_compare[1]
    if ':' in splitted_compare[0] and splitted_compare[0][0] not in STRING_CHARS:
        field = splitted_compare[0]
        value = splitted_compare[2]
    else:
        field = splitted_compare[2]
        value = splitted_compare[0]
    stix_type = field.split(':')[0]
    try:
        return stix_modifiers[stix_type].eval(field, operator, value)
    except (FieldCanNotBeCalculatedBy, KeyError):
        field = 'f.' + '.'.join(field.split(':')[1:])
        if operator == '=':
            operator = '=='
        return field + ' ' + operator + ' ' + value

def request_compil(expression):
    current_word = ''
    is_in_string = False
    string_opener = ''
    result = ''
    current_compare = ''
    l_compare = []
    type = None
    for c in expression:
        if c in STRING_CHARS:
            if not is_in_string:
                is_in_string = True
                string_opener = c
            elif c == string_opener:
                is_in_string = False
        if (c not in SPECIAL_CHARS and c not in SEPARATOR_CHARS) \
                or is_in_string:
            current_word += c
        if c in SPECIAL_CHARS + SEPARATOR_CHARS and not is_in_string:
            word, calculated_type = word_compil(current_word)
            result += word
            word, _ = word_compil(c) 
            result += word
            if current_word in ['(',')','[',']','AND', 'OR']:
                l_compare.append(current_compare)
                current_compare = ''
            else:
                current_compare += current_word
            if c not in ['(',')','[',']'] and not is_in_string:
                current_compare += c
            current_word = ''
            if calculated_type:
                if type and type != calculated_type:
                    raise PatternAlreadyContainsType(type, calculated_type)
                type = calculated_type
    result += current_word
    l_compare.append(current_compare)
    l_compare_compiled = [compare_compile(c) for c in l_compare]
    for compare, compiled_compare in zip(l_compare, l_compare_compiled):
        # ! quick fix : remove space before and after
        while len(compare) > 0 and compare[0] in SEPARATOR_CHARS:
            compare = compare[1:]
        while len(compare) > 0 and compare[-1] in SEPARATOR_CHARS:
            compare = compare[:-1]
        result = result.replace(compare, compiled_compare)
    return result + ' AND f.type == "' + type + '"'

class Request:
    def __init__(self, db_conn, date):
        self.db_conn = db_conn
        self.date = date


    def remove_arango_fields(self, object):
        return {k:v for k, v in object.items() if not k.startswith('_')}


    def request_one_feed(self, feed, pattern, max_depth=5):
        col_name = get_collection_name(feed)
        aql_prefix = 'FOR f IN {}  '.format(col_name)
        aql_suffix = ' RETURN f'
        aql_middle = 'FILTER ' + request_compil(pattern)

        aql = aql_prefix + aql_middle + aql_suffix
        print(aql)
        matched_results = self.db_conn.AQLQuery(aql, raw_results=True)
        results = []
        for r in matched_results:
            vertexes = self.graph_traversal(r['_id'], max_depth=max_depth)
            for vertex in vertexes:
                vertex = vertex.getStore()
                vertex = self.remove_arango_fields(vertex)
                vertex['x_feed'] = feed.feed_name
                results.append(self.remove_arango_fields(vertex))
        return results


    def request(self, pattern, tags=[], max_depth=5):
        feeds = Feed.get_last_feeds(self.db_conn, self.date)
        feeds = [feed for feed in feeds if set(tags).issubset(set(feed.tags))]
        results = []
        for feed in feeds:
            results.extend(self.request_one_feed(feed, pattern, max_depth=max_depth))
        return results
    

    def graph_traversal(self, id, max_depth=5):
        col_name = id.split('/')[0]
        aql = """FOR v, e in 0..{} ANY '{}' {} RETURN v""".format(max_depth, id, 'edge_' + col_name)
        return self.db_conn.AQLQuery(aql, raw_results=True)