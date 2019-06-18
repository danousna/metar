import matplotlib.pyplot as plt
from datetime import datetime
from cassandra.cluster import Cluster


cluster = Cluster(['localhost'])
cluster = cluster.connect('chembise_metar_1_12')


def get_points(year):
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


def get_all():
    data = { '2009': [], '2010': [], '2011': [], '2012': [], '2013': [], '2014': [], '2015': [], '2016': [], '2017': [], '2018': [] }
    avg = [None for i in range(366)]

    for year in data.keys():
        data[year] = get_points(year)
    
    for i in range(366):
        tmp_len = 0
        tmp_sum = 0

        for year in data.keys():
            if data[year][i] != None:
                tmp_len = tmp_len + 1
                tmp_sum = tmp_sum + data[year][i]

        avg[i] = tmp_sum / tmp_len

    data['avg'] = avg

    return data
    

data = get_all()
plt.plot(data['avg'])
plt.plot(data['2011'])
plt.savefig('temperatures.png')
