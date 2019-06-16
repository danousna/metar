import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from cassandra.cluster import Cluster
from statistics import mean


cluster = Cluster(['localhost'])
cluster = cluster.connect('chembise_metar_1_12')


def get_points(datetime):
    global cluster
    query = "SELECT month, day, AVG(tmpf) AS tmpf FROM date_by_location WHERE lat = 52.5644 AND lon = 13.3088 AND year = {} GROUP BY month, day;".format(year)
    print(query)
    results = cluster.execute(query)

    data = [None for i in range(366)]

    for result in results:
        day = datetime.strptime(str(year) + '-' + str(result.month) + '-' + str(result.day), '%Y-%m-%d').timetuple().tm_yday
        try:
            data[day - 1] = float((result.tmpf - 32) * 5/9)  # Convert to Celsius
        except IndexError:
            print("Could not insert day {} with value {}".format(day, (result.tmpf - 32) * 5/9))

    return data


data = get_all()
plt.plot(data['avg'])
plt.plot(data['2011'])
plt.savefig('temperatures.png')
