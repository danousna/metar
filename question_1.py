import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from cassandra.cluster import Cluster
from statistics import mean


cluster = Cluster(['localhost'])
cluster = cluster.connect('chembise_metar_1_12')


def get_points(year):
    global cluster
    query = "SELECT month, day, AVG(tmpf) AS tmpf FROM date_by_location WHERE lat = 52.5644 AND lon = 13.3088 AND year = {} GROUP BY month, day;".format(year)
    print(query)
    results = cluster.execute(query)

    data = [np.nan for i in range(365)]

    for result in results:
        day = datetime.strptime(str(year) + '-' + str(result.month) + '-' + str(result.day), '%Y-%m-%d').timetuple().tm_yday
        data[day] = float((result.tmpf - 32) * 5/9)  # Convert to Celsius

    return data


def get_all():
    data = { '2009': [], '2010': [], '2011': [], '2012': [], '2013': [], '2014': [], '2015': [], '2016': [], '2017': [], '2018': [] }
    avg = [np.nan for i in range(365)]

    for year in data.keys():
        data[year] = get_points(year)
    
    data_flattened = [data[year] for year in data.keys()]
    tmp = np.array([avg, data_flattened])
    print(tmp)
    avg = np.nanmean(tmp, axis = 0).tolist()

    print(avg)
    data['avg'] = avg

    return data
    

data = get_all()
plt.plot(data['avg'])
plt.plot(data['2009'])
plt.savefig('temperatures.png')
