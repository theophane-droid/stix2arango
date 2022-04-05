from threading import Thread

from pyArango.query import AQLQuery

from stix2arango.feed import Feed
from stix2arango.exceptions import (
    PatternAlreadyContainsType,
    MalformatedExpression,
    FieldCanNotBeCalculatedBy)
from stix2arango import stix_modifiers
from stix2arango.utils import (
    remove_redondant_parenthesis,
    check_if_expression_is_balanced,
    remove_unused_space,
    merge_obj_list)
import uuid

SPECIAL_CHARS = '[()]=<>'
STRING_CHARS = '"\''
SEPARATOR_CHARS = ' \t'


def splitter(expression):
    """Split a string into a list of words depending on the separator chars

    Args:
        expression (str): the stix expression to split

    Returns:
        list: the list of words
    """
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
    """Compile word in AQL syntax

    Args:
        word (str): the word to compile

    Returns:
        str: the word compiled
    """
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


def compare_compile(compare_string, operator_list=None):
    """Compile a comparaison expression from stix to AQL

    Args:
        compare_string (str): stix expression to compile
        operator_list (list): an object to save used operator

    Raises:
        MalformatedExpression: if the expression is malformated

    Returns:
        str: AQL expression
    """
    splitted_compare = splitter(compare_string)
    if len(splitted_compare) == 3:
        operator = splitted_compare[1]
    elif len(splitted_compare) == 4:
        operator = splitted_compare[1] + splitted_compare[2]
    else:
        raise MalformatedExpression(compare_string)
    if ':' in splitted_compare[0] and \
            splitted_compare[0][0] not in STRING_CHARS:
        field = splitted_compare[0]
        value = splitted_compare[-1]
    else:
        field = splitted_compare[-1]
        value = splitted_compare[0]
    stix_type = field.split(':')[0]
    if operator_list != None:
        operator_list.append(operator)
    try:
        return stix_modifiers[stix_type].eval(field, operator, value)
    except (FieldCanNotBeCalculatedBy, KeyError):
        field = 'f.' + '.'.join(field.split(':')[1:])
        if operator == '=':
            operator = '=='
        return field + ' ' + operator + ' ' + value


def pattern_compil(expression, return_type=False, operator_list=None):
    """Compile a stix pattern to AQL comparaison expression

    Args:
        expression (str): stix pattern to compile
        return_type (bool) : return_type of requested object
        operator_list (list): an object to save used operator, default to None

    Raises:
        PatternAlreadyContainsType: when contains different types of SDOs

    Returns:
        str: AQL expression or type of requested object is return_type is True
    """
    if not(check_if_expression_is_balanced(expression)):
        raise MalformatedExpression(expression)
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
            if current_word in ['(', ')', '[', ']', 'AND', 'OR']:
                l_compare.append(current_compare)
                current_compare = ''
            else:
                current_compare += current_word
            if c not in ['(', ')', '[', ']'] and not is_in_string:
                current_compare += c
            current_word = ''
            if calculated_type:
                if type and type != calculated_type:
                    raise PatternAlreadyContainsType(type, calculated_type)
                type = calculated_type
    result += current_word
    l_compare.append(current_compare)
    l_compare_compiled = [compare_compile(c, operator_list=operator_list) for c in l_compare]
    for compare, compiled_compare in zip(l_compare, l_compare_compiled):
        # ! quick fix : remove space before and after
        while len(compare) > 0 and compare[0] in SEPARATOR_CHARS:
            compare = compare[1:]
        while len(compare) > 0 and compare[-1] in SEPARATOR_CHARS:
            compare = compare[:-1]
        result = result.replace(compare, compiled_compare)
    if not type:
        raise MalformatedExpression(expression)
    aql_expression = result + ' AND f.type == "' + type + '"'
    if return_type:
        return type
    else:
        return remove_unused_space(remove_redondant_parenthesis(aql_expression))


class ThreadedRequestFeed(Thread):
    def __init__(
        self, 
        request, 
        feed,
        pattern,
        max_depth=1,
        create_index=True,
        limit=-1):
        Thread.__init__(self)
        self.request = request
        self.feed = feed
        self.pattern = pattern
        self.max_depth = max_depth
        self.create_index = create_index
        self.limit = limit

    def run(self):
        self.results = self.request.request_one_feed(
            self.feed,
            self.pattern,
            self.max_depth,
            self.create_index,
            self.limit
        )

class Request:
    """Class to manage a request to the database"""

    def __init__(self, db_conn, date):
        """Initialize the request

        Args:
            db_conn (pyArango database): the database connection
            date (datetime): timestamp of request
        """
        self.db_conn = db_conn
        self.date = date

    def __remove_arango_fields(self, object):
        """Remove the fields created by arangoDB from the object

        Args:
            object (dict): the object to clean

        Returns:
            dict: the cleaned object
        """
        return {k: v for k, v in object.items() if not k.startswith('_')}

    def request_one_feed(
            self,
            feed,
            pattern,
            max_depth=1,
            create_index=True,
            limit=-1
            ):
        """Request the objects from a feed

        Args:
            feed (stix2arango.feed.Feed): the feed to request
            pattern (str): the stix2.1 pattern
            max_depth (int, optional): graph traversal depth limit.\
                Defaults to 1.
            create_index (bool, optional): create index based on search.\
                Defaults to True.
            limit (int, optional): limit the number of results.
                if set to -1, no limit.
                Defaults to -1.

        Returns:
            list: objects matching pattern and their related(depth limited)
        """
        col_name = feed.storage_paradigm.get_collection_name(feed)
        aql_prefix = 'FOR f IN {}  '.format(col_name)
        if limit != -1:
            aql_suffix = ' LIMIT %d RETURN f' % (limit)
        else:
            aql_suffix = ' RETURN f'
        operator_list = list()
        aql_middle = 'FILTER ' + pattern_compil(pattern, operator_list=operator_list)

        aql = aql_prefix + aql_middle + aql_suffix
        matched_results = self.db_conn.AQLQuery(aql, raw_results=True)
        if create_index :
            if operator_list.count('=') == len(operator_list):
                self._create_index_from_query(col_name, aql)
        # create
        results = []
        for r in matched_results:
            r = r.getStore()
            vertexes = self._graph_traversal(r['_id'], max_depth=max_depth)
            r['x_feed'] = feed.feed_name
            r['x_tags'] = feed.tags
            results.append(r)
            for vertex in vertexes:
                vertex = vertex.getStore()
                vertex = self.__remove_arango_fields(vertex)
                vertex['x_feed'] = feed.feed_name
                vertex['x_tags'] = feed.tags
                results.append(self.__remove_arango_fields(vertex))
                
        return results

    def request_one_feed_threaded(
            self,
            feed,
            pattern,
            max_depth=5,
            create_index=True,
            limit=-1
            ):
        """Request objects from a feed using a thread

        Args:
            Cf request_one_feed method

        Returns:
            ThreadedRequestFeed: the thread in which request is launched
        """
        thread = ThreadedRequestFeed(
            self,
            feed,
            pattern,
            max_depth,
            create_index,
            limit
        )
        thread.start()
        return thread

    def request(
            self,
            pattern,
            tags=[],
            max_depth=1,
            create_index=True
            ):
        """Request the objects from the database

        Args:
            pattern (str): the stix2.1 pattern
            tags (list, optional): query feeds carrying all tags. \
                Defaults to [].
            max_depth (int, optional): graph traversal depth limit. \
                Defaults to 1.
            create_index (bool, optional): create index based on search.\
                Defaults to True.
        Returns:
            list: objects matching pattern and their related(depth limited)
        """
        feeds = Feed.get_last_feeds(self.db_conn, self.date)
        request_obj_type = pattern_compil(pattern, return_type=True)
        feeds = [feed for feed in feeds if set(tags).issubset(set(feed.tags)) and \
                (int(feed.version.split('.')[0]) == 0 or request_obj_type in feed.inserted_stix_types)]
        results = []
        l_threads = []
        for feed in feeds:
            threaded_request = self.request_one_feed_threaded(
                feed,
                pattern,
                max_depth=max_depth,
                create_index=create_index
            )
            l_threads.append(threaded_request)
        for thread in l_threads:
            thread.join()
            results += thread.results
        merge_obj_list(results)
        return results        

    def _create_index_from_query(self, col_name, query):
        """Create an index from a query

        Args:
            query (str): the query to create the index from

        Returns:
            str: the created index
        """
        index_name = 'stix2arango_idx_' + str(uuid.uuid4())
        fields = self._extract_field_from_query(query)
        if len(fields):
            self.db_conn[col_name].ensureIndex(
                index_type='persistent',
                fields=fields,
                in_background=True
            )
        return index_name

    def _graph_traversal(self, id, max_depth=1):
        """Traverse the graph to get the related objects

        Args:
            id (str): the id of the object to start the traversal
            max_depth (int, optional): graph traversal depth limit. \
                Defaults to 1.

        Returns:
            list: the related objects
        """
        col_name = id.split('/')[0]
        aql = """FOR v, e, p in 1..2 ANY '{}' {} 
                PRUNE COUNT(p.vertices) == 2 and p.vertices[1].type!="relationship"
                RETURN v"""\
            .format(id, 'edge_' + col_name, id)
        return self.db_conn.AQLQuery(aql, raw_results=True)

    def _extract_field_path(self, node):
        result = []
        for sub in node['subNodes']:
            if sub['type'] == 'attribute access' or sub['type'] == 'reference':
                result += [sub['name']]
                if 'subNodes' in sub:
                    result += self._extract_field_path(sub)
        return result

    def _extract_nodes(self, node):
        results = []
        if 'compare' in node['type']:
            path = self._extract_field_path(node)
            results.append('.'.join(path[::-1][1:]))
        elif 'subNodes' in node:
            for sub in node['subNodes']:
                results += self._extract_nodes(sub)
        return results

    def _extract_field_from_query(self, aql):
        query = self.db_conn.AQLQuery(aql)
        explanation = query.explain()
        fields = []
        for v in explanation['plan']['nodes']:
            if 'filter' in v:
                fields = self._extract_nodes(v['filter'])
        # remove duplicates
        fields = list(set(fields))
        return fields
