import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from cassandra.cluster import Cluster
from statistics import mean
from haversine import haversine

cluster = Cluster(['localhost'])
cluster = cluster.connect('chembise_metar_1_12')

def get_nearest_station(lat, lon):
    global cluster
    query = "select distinct lat, lon from date_by_location;"
    stations = cluster.execute(query)

    nearest_station = None
    min_dist = None
    for station in stations:
        dist = haversine((lat, lon), (station.lat, station.lon))

        if nearest_station is None or min_dist > dist:
            nearest_station = station
            min_dist = dist

    return nearest_station


def get_points(year, lat, lon):
    global cluster
    query = "SELECT month, day, AVG(tmpf) AS tmpf FROM date_by_location WHERE lat = {} AND lon = {} AND year = {} GROUP BY month, day;".format(lat, lon, year)
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


def get_all(lat, lon):
    data = { '2009': [], '2010': [], '2011': [], '2012': [], '2013': [], '2014': [], '2015': [], '2016': [], '2017': [], '2018': [] }
    avg = [None for i in range(366)]

    station = get_nearest_station(lat, lon)
    for year in data.keys():
        data[year] = get_points(year, station.lat, station.lon)

    for i in range(366):
        tmp_len = 0
        tmp_sum = 0

        for year in data.keys():
            if data[year][i] is not None:
                tmp_len = tmp_len + 1
                tmp_sum = tmp_sum + data[year][i]

        avg[i] = tmp_sum / tmp_len

    data['avg'] = avg

    return data


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 4:
        raise RuntimeError("Utiliser ce programme avec 3 arguments : l'année, la latitude et la longitude.\n\rex:\tquestion_1.py 2011 52.5644 13.3088")

    year = sys.argv[1]
    lon = float(sys.argv[2])
    lat = float(sys.argv[3])

    data = get_all(lat, lon)
    plt.plot(data['avg'])
    try:
        plt.plot(data[year])
    except KeyError:
        raise KeyError("L'année '"+year+"' n'existe pas.")
    plt.savefig('temperatures.png')
