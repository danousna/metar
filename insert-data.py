import cassandra.cluster
import generator
from cql_tils import format_insert_query

c = cassandra.cluster.Cluster(['localhost'])
ks = c.connect('chembise_metar_1_12')

data = generator.loadata(300)

for row in data:
    query = format_insert_query("date_by_location", row, {
        'valid': 'daytime',
    })

    ks.execute(query)
