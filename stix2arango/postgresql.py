import ipaddress
import copy
from nis import match
from typing import Dict

from stix2arango.exceptions import InvalidObjectForOptimizer
from stix2arango.utils import deep_dict_update

import psycopg2
from psycopg2.errors import DuplicateTable
import uuid

type_map = {
    'str' : 'TEXT',
    'int' : 'INT'
}

operator_map = {
    '>' : '>',
    '<' : '<',
    '=' : '=',
    '!=' : '!=',
    '<=' : '<=',
    '>=' : '>=',
    'like' : 'LIKE',
}

def convert_type(type_, value_):
    """Map between python type/postgresql type
    Args:
        type_ (str): python field type
        value_ (str): python field value
    Raises:
        RuntimeError: When a type can't be converted
    Returns:
        str: the postgresql associed type
    """
    if type_ in type_map:
        return type_map[type_]
    
    raise RuntimeError("%s type not found" % (type_))


class PGResult(dict):
    def __init__(
        self,
        arango_conn,
        arango_id=None,
        field0_value=None,
        optimizer=None
        ):
        self.arango_conn = arango_conn
        self.arango_id = arango_id
        self.field0_value = field0_value
        self.optimizer = optimizer
        super().__init__()
    
    def __call__(self):
        if self.field0_value:
            pass


class PostgresOptimizer:
    postgres_conn = None
    count = 0
    db_name = None
    db_host = None
    db_user = None
    db_pass = None
    db_port = None
    def __init__(self, field):
        self.uuid = str(uuid.uuid4()).replace('-','_')
        self.table_name = None
        self.field = field
        self.table_created = False
        if not(PostgresOptimizer.postgres_conn):
            raise RuntimeError('PostgresOptimizer.postgres_conn is not set')
    
    def insert_stix_obj(self, stix_object, arango_id, feed):
        if not self.table_created:
            self.__create_table(feed, stix_object)
        arango_id = int(arango_id.split('/')[-1])
        if self.field != 'ipv4-addr:x_ip':
            value = self.__extract_field_value(self.field, stix_object)
        elif stix_object['type'] == 'ipv4-addr':
            value = stix_object['value']
        else:
            raise InvalidObjectForOptimizer(stix_object['type'])
        sql = "INSERT INTO " + self.table_name + " values (%s, %s, %s);"
        with PostgresOptimizer.postgres_conn.cursor() as cursor:
            cursor.execute(sql, [value, arango_id, stix_object['id']])
        if self.count % 1000 == 0:
            PostgresOptimizer.postgres_conn.commit()
        self.count += 1
        return stix_object

    def craft_obj_from_request(self, stix_id, field0):
        type = self.field.split(':')[0]
        if self.field == 'ipv4-addr:x_ip':
            obj = {'id' : stix_id, 'type' : type, 'value' : field0}
            return obj
        path = self.field.split(':')[1:-1]
        value_name = self.field.split(':')[-1]
        obj = {'id' : stix_id, 'type' : type}
        current_obj = obj

        for step in path:
            current_obj[step] = {}
            current_obj = current_obj[step]
        current_obj[value_name] = field0
        return obj

    def present_results(self, results):
        dict_results = {}
        for arango_id, stix_id, field0 in results:
            if not str(arango_id) in dict_results:
                dict_results[str(arango_id)] = []
            dict_results[str(arango_id)] += [self.craft_obj_from_request(stix_id, field0)]
        return dict_results


    def query(self, operator, value, feed):
        if value[0] == '"' and value[-1] == '"':
            value = "'" + value[1:-1] + "'"
        self.table_name = feed.storage_paradigm.get_collection_name(feed) + self.uuid
        if self.field == 'ipv4-addr:x_ip':
            middle_sql = 'field0 >> ' + value
            middle_sql += ' OR field0 = ' + value
        else:
            middle_sql = 'field0 ' + operator_map[operator] + ' ' + value
        sql = 'select arango_id, stix_id, field0 from ' + self.table_name + ' where ' + middle_sql + ';'
        with PostgresOptimizer.postgres_conn.cursor() as cursor:
            cursor.execute(sql)
            results = cursor.fetchall()
        return self.present_results(results)

    def query_from_arango_results(self, col_name, results, arango_conn):
        # ! BUG : 2 times the same results
        self.table_name = col_name + self.uuid
        pg_results = []
        for r in results:
            try:
                r = r.getStore()
            except:
                pass
            if r['type'] == self.field.split(':')[0] or\
                (self.field == 'ipv4-addr:x_ip' and r['type'] == 'ipv4-addr'):
                sql = 'select arango_id, stix_id, field0 from ' + self.table_name + ' where arango_id = \'' + r['_key'] + '\''
                cursor = PostgresOptimizer.postgres_conn.cursor()
                cursor.execute(sql)
                pg_results += cursor.fetchall()
        pg_results = self.present_results(pg_results)
        cross = []
        for r in results:
            if r['_key'] in pg_results:
                for x in pg_results[r['_key']]:
                    r = copy.deepcopy(r)
                    cross.append(deep_dict_update(r, x))
        return cross


    def crosses_results_with_arango(self, results, arango_conn, col_name) -> list:
        aql2 = 'for el in %s filter el._key in %s return el' % (col_name, str(list(results.keys())))
        aql_results = [result.getStore() for result in 
                       arango_conn.AQLQuery(aql2, raw_results=True)]
        matched_results = []
        for m in aql_results:
            obj = copy.deepcopy(m)
            for pg_obj in results[m['_key']]:
                deep_dict_update(obj, pg_obj)
                matched_results.append(obj)
        return matched_results


    def __del__(self):
        if self.table_created and PostgresOptimizer.postgres_conn:
            idx_name = 'idx_' + self.uuid
            sql = 'CREATE INDEX ' + idx_name + ' ON ' + self.table_name + '(field0)'
            try:
                with PostgresOptimizer.postgres_conn.cursor() as cursor:
                    cursor.execute(sql)
            except:
                pass
        if PostgresOptimizer.postgres_conn:
            PostgresOptimizer.postgres_conn.commit()

    def __dict__(self):
        return {
            'class': str(self.__class__.__name__).lower(),
            'field' : self.field,
            'uuid' : self.uuid
        }

    def __extract_field_type(self, field, stix_object):
        object = copy.deepcopy(stix_object)
        if field.split(':')[0] == object['type']:
            for f in field.split(':')[1:]:
                try:
                    object = object[f]
                except (TypeError, KeyError):
                    raise InvalidObjectForOptimizer(stix_object['type'])
            return type(object).__name__
        else:
            raise InvalidObjectForOptimizer(stix_object['type'])


    def __extract_field_value(self, field, stix_object):
        object = copy.deepcopy(stix_object)
        if field.split(':')[0] == object['type']:
            for f in field.split(':')[1:]:
                try:
                    object = object[f]
                except (TypeError, KeyError):
                    raise InvalidObjectForOptimizer(stix_object['type'])
            return object
        else:
            raise InvalidObjectForOptimizer(stix_object['type'])

    def __create_table(self, feed, stix_object):
        try:
            if self.field != 'ipv4-addr:x_ip':
                type_ = self.__extract_field_type(self.field, stix_object)
                value = self.__extract_field_value(self.field, stix_object)
                type_ = convert_type(type_, value)
            else:
                type_ = 'inet'
            content = 'field0 ' + type_ + ', arango_id int, stix_id text'
            self.table_name = feed.storage_paradigm.get_collection_name(feed) + self.uuid
            cursor = PostgresOptimizer.postgres_conn.cursor()
            base_query = 'create table ' + self.table_name + ' (%s);'
            query = base_query % (content)
            cursor.execute(query)
            cursor.close()
            PostgresOptimizer.postgres_conn.commit()

        except DuplicateTable: 
            pass
        self.table_created = True

    def list_all_table(self):
        s = "SELECT"
        s += " table_schema"
        s += ", table_name"
        s += " FROM information_schema.tables"
        s += " WHERE"
        s += " ("
        s += " table_schema = 'public'"
        s += " )"
        s += " ORDER BY table_schema, table_name;"
        with PostgresOptimizer.postgres_conn.cursor() as cursor:
            cursor.execute(s)
            results = cursor.fetchall()
        return [list(r)[1] for r in results]

    def drop_table(self, feed_name) -> bool:
        try:
            for table_name in self.list_all_table():
                if table_name.startswith(feed_name):
                    sql = 'drop table ' + table_name
                    with PostgresOptimizer.postgres_conn.cursor() as cursor :
                        cursor.execute(sql)
            PostgresOptimizer.postgres_conn.commit()
            self.table_created = False
            return True
        except Exception:
            return False
    

    def delete_fields_in_object(self, object):
        object = copy.deepcopy(object)
        object_type = self.field.split(':')[0]
        field_path = self.field.split(':')[1:-1]
        last_field = self.field.split(':')[-1]
        if object['type'] == object_type:
            dict_to_remove = object
            for f in field_path:
                if f in dict_to_remove:
                    dict_to_remove = dict_to_remove[f]
                else:
                    break
            if last_field in dict_to_remove:
                del dict_to_remove[last_field]
        if self.field == 'ipv4-addr:x_ip':
            if 'value' in object:
                del object['value']
        if 'id' in object:
            del object['id']
        return object

    @staticmethod
    def connect_db():
        auth = "dbname='%s' user='%s' host='%s' password='%s' port='%s'"
        auth = auth % (
            PostgresOptimizer.db_name,
            PostgresOptimizer.db_user,
            PostgresOptimizer.db_host,
            PostgresOptimizer.db_pass,
            PostgresOptimizer.db_port
        )
        PostgresOptimizer.postgres_conn = psycopg2.connect(auth)