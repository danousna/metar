import cassandra.cluster
import generator

c = cassandra.cluster.Cluster(['localhost'])
ks = c.connect('chembise_metar_1_12')

data = generator.loadata(100)

for row in data:
    print(format_insert_query("date_by_location", row, {
        'valid': 'daytime'
    }))