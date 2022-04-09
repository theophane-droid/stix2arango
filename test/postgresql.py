import psycopg2

db = 'stix2arango'
user = 'root'
pass_ = 'changeme'
host = 'localhost'

print('> Test postgres optimizer')

auth = "dbname='%s' user='%s' host='%s' password='%s'" % (db, user, host, pass_)
print(auth)

psycopg2.connect(auth)


exit()