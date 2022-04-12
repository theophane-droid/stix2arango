import ipaddress

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
    try:
        ipaddress.ip_address(value_)
        return 'inet'
    except:
        pass
    try:
        ipaddress.ip_network(value_)
        return 'inet'
    except:
        pass
    if type_ in type_map:
        return type_map(type_)
    
    raise RuntimeError("%s type not found" % (type_))

class PostgresOptimizer:
    postgres_conn = None
    def __init__(self, field):
        self.uuid = str(uuid.uuid4()).replace('-','_')
        self.table_name = None
        self.field = field
        self.table_created = False
        self.count = 0
        if not(PostgresOptimizer.postgres_conn):
            raise RuntimeError('PostgresOptimizer.postgres_conn is not set')
    
    def insert_stix_obj(self, stix_object, arango_id, feed):
        print('insert obj !')
        print(stix_object)
        try:
            if not self.table_created:
                self.__create_table(feed, stix_object)
        except RuntimeError as e:
            pass
        arango_id = int(arango_id.split('/')[-1])
        value = self.__extract_field_value(self.field, stix_object)
        sql = "INSERT INTO " + self.table_name + " values (%s, %s)"
        print(sql)
        cursor = PostgresOptimizer.postgres_conn.cursor()
        cursor.execute(sql, [value, arango_id])
        if self.count % 1000 == 0:
            PostgresOptimizer.postgres_conn.commit()
        self.count += 1

    def query(self, operator, value, feed):
        self.table_name = feed.storage_paradigm.get_collection_name(feed) + self.uuid
        middle_sql = 'field0 ' + operator_map[operator] + ' ' + value
        sql = 'select arango_id from ' + self.table_name + ' where ' + middle_sql 
        cursor = PostgresOptimizer.postgres_conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        results = [list(r)[0] for r in results]
        return results

    def __del__(self):
        if self.table_created and PostgresOptimizer.postgres_conn:
            idx_name = 'idx_' + self.uuid
            sql = 'CREATE INDEX ' + idx_name + ' ON ' + self.table_name + '(field0)'
            try:
                cursor = PostgresOptimizer.postgres_conn.cursor()
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
        object = dict(stix_object)
        if field.split(':')[0] == object['type']:
            for f in field.split(':')[1:]:
                try:
                    object = object[f]
                except TypeError:
                    raise RuntimeError('Object cannot be index by current optimizer')
            return type(object).__name__
        else:
            raise RuntimeError('Object cannot be index by current optimizer')

    def __extract_field_value(self, field, stix_object):
        object = dict(stix_object)
        if field.split(':')[0] == object['type']:
            for f in field.split(':')[1:]:
                try:
                    object = object[f]
                except TypeError:
                    raise RuntimeError('Object cannot be index by current optimizer')
            return object
        else:
            raise RuntimeError('Object cannot be index by current optimizer')

    def __create_table(self, feed, stix_object):
        print('create table !')
        try:
            type_ = self.__extract_field_type(self.field, stix_object)
            value = self.__extract_field_value(self.field, stix_object)
            type_ = convert_type(type_, value)
            content = 'field0 ' + type_ + ', arango_id int'
            self.table_name = feed.storage_paradigm.get_collection_name(feed) + self.uuid
            cursor = PostgresOptimizer.postgres_conn.cursor()
            base_query = 'create table ' + self.table_name + ' (%s)'
            query = base_query % (content)
            cursor.execute(query)
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
        cursor = PostgresOptimizer.postgres_conn.cursor()
        cursor.execute(s)
        results = cursor.fetchall()
        return [list(r)[1] for r in results]

    def drop_table(self):
        if self.table_created:
            col_name = self.table_name[:len(self.table_name)-len(self.uuid)]
            for table_name in self.list_all_table():
                if table_name.startswith(col_name):
                    sql = 'drop table ' + table_name
                    cursor = PostgresOptimizer.postgres_conn.cursor()
                    cursor.execute(sql)
            PostgresOptimizer.postgres_conn.commit()
            self.table_created = False
            