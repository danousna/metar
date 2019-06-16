import matplotlib.pyplot as plt
import numpy
from datetime import datetime
from cassandra.cluster import Cluster


def get_points(c, year):
    query = "SELECT month, day, AVG(tmpf) AS tmpf FROM date_by_location WHERE lat = 52.5644 AND lon = 13.3088 AND year = {} GROUP BY month, day;".format(year)
    print(query)
    results = c.execute(query)

    data = [numpy.nan for i in range(365)]

    for result in results:
        day = datetime.strptime(str(year) + '-' + str(result.month) + '-' + str(result.day), '%Y-%m-%d').timetuple().tm_yday
        data[day] = result.tmpf

    return data


cluster = Cluster(['localhost'])
cluster = cluster.connect('chembise_metar_1_12')

data = get_points(cluster, 2011)

for i in range(365):
    print(str(i) + ' : ' + str(data[i]))

plt.scatter(data)
plt.xticks(rotation = 90)
plt.savefig('temperatures.png')
