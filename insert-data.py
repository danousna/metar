import cassandra.cluster
import generator
from cql_utils import *

c = cassandra.cluster.Cluster(['localhost'])
ks = c.connect('chembise_metar_1_12')

data = generator.loadata()

i = 0
for row in data:
    i = i + 1
    print(i)
    row = split_daytime(row)
    query = format_insert_query("date_by_location", row)

    ks.execute(query)
