from cassandra.cluster import Cluster

cluster = Cluster(['localhost'])
cluster = cluster.connect('chembise_metar_1_12')

query = "SELECT * FROM data_by_datetime LIMIT 1000000;"
results = cluster.execute(query)

stations = {}

for item in results:
    if item.station not in stations:
        stations[item.station] = 0
    stations[item.station] = stations[item.station] + 1

print(stations)
