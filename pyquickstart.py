from cassandra.auth import PlainTextAuthProvider
import config as cfg
from cassandra.query import BatchStatement, SimpleStatement
from prettytable import PrettyTable
import time
import ssl
import cassandra
from cassandra.cluster import Cluster
from cassandra.policies import *
from ssl import PROTOCOL_TLSv1_2
from requests.utils import DEFAULT_CA_BUNDLE_PATH
import numpy as np

def PrintTable(rows):
    t = PrettyTable(['UserID', 'Name', 'City'])
    for r in rows:
        t.add_row([r.user_id, r.user_name, r.user_bcity])
    print(t)

ssl_opts = {
            'ca_certs': DEFAULT_CA_BUNDLE_PATH,
            'ssl_version': PROTOCOL_TLSv1_2,
            }

if 'certpath' in cfg.config:
    ssl_opts['ca_certs'] = cfg.config['certpath']

auth_provider = PlainTextAuthProvider(
        username=cfg.config['username'], password=cfg.config['password'])
cluster = Cluster([cfg.config['contactPoint']], port = cfg.config['port'], auth_provider=auth_provider, ssl_options=ssl_opts
)
session = cluster.connect()
print(session)
print("session connected")
# Use session to upload db files


print("\nCreating Keyspace")
session.execute('CREATE KEYSPACE IF NOT EXISTS movie_stuff WITH replication = {\'class\': \'NetworkTopologyStrategy\', \'datacenter\' : \'1\' }');
print ("\nCreating Table")
session.execute('CREATE TABLE IF NOT EXISTS movie_stuff.ratings (user_id int PRIMARY KEY, item_id int, rating int, date int)');

ratings_data = np.fromfile('ratings.dat', dtype=int, sep="\t")
ratings_data = np.reshape(ratings_data, (100000,4))
print(ratings_data)

for rating in ratings_data:
	session.execute(
		"""
		INSERT INTO movie_stuff.ratings(user_id, item_id, rating, date)
		VALUES(%s,%s,%s,%s)
		""",
		(rating[0],rating[1],rating[2],rating[3])
	)

# Test query
rows = session.execute('SELECT user_id, item_id, rating, date from movie_stuff.ratings')
for row in rows:
	print(row.user_id, row.item_id, row.rating, row.date)


# insert_data = session.prepare("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (?,?,?)")
# batch = BatchStatement()
# batch.add(insert_data, (1, 'LyubovK', 'Dubai'))
# batch.add(insert_data, (2, 'JiriK', 'Toronto'))
# batch.add(insert_data, (3, 'IvanH', 'Mumbai'))
# batch.add(insert_data, (4, 'YuliaT', 'Seattle'))
# batch.add(insert_data, (5, 'IvanaV', 'Belgaum'))
# batch.add(insert_data, (6, 'LiliyaB', 'Seattle'))
# batch.add(insert_data, (7, 'JindrichH', 'Buenos Aires'))
# batch.add(insert_data, (8, 'AdrianaS', 'Seattle'))
# batch.add(insert_data, (9, 'JozefM', 'Seattle'))
# batch.add(insert_data, (10, 'EmmaH', 'Seattle'))
# batch.add(insert_data, (11, 'GrzegorzM', 'Seattle'))
# batch.add(insert_data, (12, 'FryderykK', 'Seattle' ))
# batch.add(insert_data, (13, 'DesislavaL', 'Seattle'))
# session.execute(batch)

# print("\nSelecting All")
# rows = session.execute('SELECT * FROM uprofile.user')
# PrintTable(rows)

 
# print("\nSelecting Id=1")
# rows = session.execute('SELECT * FROM uprofile.user where user_id=1')
# PrintTable(rows)

cluster.shutdown()
