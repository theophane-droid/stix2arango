import ipaddress

from stix2arango.exceptions import InvalidObjectForOptimizer

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

class PostgresOptimizer:
    postgres_conn = None
    count = 0
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
        else:
            value = stix_object['value']
        sql = "INSERT INTO " + self.table_name + " values (%s, %s, %s);"
        with PostgresOptimizer.postgres_conn.cursor() as cursor:
            cursor.execute(sql, [value, arango_id, stix_object['id']])
        if self.count % 1000 == 0:
            PostgresOptimizer.postgres_conn.commit()
        self.count += 1
        return stix_object

    def craft_obj_from_request(self, stix_id, field0):
        type = self.field.split(':')[0]
        path = self.field.split(':')[1:-1]
        value_name = self.field.split(':')[-1]
        obj = {'id' : stix_id, 'type' : type}
        current_obj = obj
        for step in path:
            current_obj[step] = {}
            current_obj = current_obj[step]
        current_obj[value_name] = field0
        return obj


    def query(self, operator, value, feed):
        self.table_name = feed.storage_paradigm.get_collection_name(feed) + self.uuid
        if self.field == 'ipv4-addr:x_ip':
            middle_sql = 'field0 >> ' + value
            middle_sql += 'OR field0 = ' + value
        else:
            middle_sql = 'field0 ' + operator_map[operator] + ' ' + value
        sql = 'select arango_id, stix_id, field0 from ' + self.table_name + ' where ' + middle_sql + ';'
        with PostgresOptimizer.postgres_conn.cursor() as cursor:
            cursor.execute(sql)
            results = cursor.fetchall()
        results = {
            str(arango_id):self.craft_obj_from_request(stix_id, field0)\
                for arango_id, stix_id, field0 in results
            } 
        return results

    def __del__(self):
        if self.table_created and PostgresOptimizer.postgres_conn:
            idx_name = 'idx_' + self.uuid
            sql = 'CREATE INDEX ' + idx_name + ' ON ' + self.table_name + '(field0)'
            try:
                with PostgresOptimizer.postgres_conn.cursor() as cursor:
                    cursor.execute(sql)
            except:
                pass 
        PostgresOptimizer.postgres_conn.commit()

    def __dict__(self):
        return {
            'class': str(self.__class__.__name__).lower(),
            'field' : self.field,
            'uuid' : self.uuid
        }

    def __extract_field_type(self, field, stix_object):
        object = dict(stix_object)
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
        object = dict(stix_object)
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

    def drop_table(self, feed_name):
        for table_name in self.list_all_table():
            if table_name.startswith(feed_name):
                sql = 'drop table ' + table_name
                with PostgresOptimizer.postgres_conn.cursor() as cursor :
                    cursor.execute(sql)
        PostgresOptimizer.postgres_conn.commit()
        self.table_created = False
    

    def delete_fields_in_object(self, object):
        object = dict(object)
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