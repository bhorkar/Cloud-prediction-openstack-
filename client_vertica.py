import vertica_python

conn_info = {'host': 'verticapride.it.att.com',
             'port': 5433,
             'user': 'ab981s',
             'password': 'GaRbAgE^2013',
             'database': 'PDW',
             'read_timeout': 600,
             'unicode_error': 'strict',
             'ssl': False,
             'connection_timeout': 5 }

# simple connection, with manual close
connection = vertica_python.connect(**conn_info)
# do things
connection.close()

# using with for auto connection closing after usage
with vertica_python.connect(**conn_info) as connection:
        print "connected"
# do things
