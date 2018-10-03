import cassandra
import time
import ssl
from cassandra.auth import PlainTextAuthProvider
import config as cfg
from cassandra.query import BatchStatement, SimpleStatement
from prettytable import PrettyTable
from cassandra.cluster import Cluster
from cassandra.policies import *
from ssl import PROTOCOL_TLSv1_2
from requests.utils import DEFAULT_CA_BUNDLE_PATH
import numpy as np

# def PrintTable(rows):
#     t = PrettyTable(['UserID', 'Name', 'City'])
#     for r in rows:
#         t.add_row([r.user_id, r.user_name, r.user_bcity])
#     print(t)

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
session.execute('SELECT count(user_id) AS Row_count FROM movie_stuff.ratings3')

print("Copy completed")
cluster.shutdown()
