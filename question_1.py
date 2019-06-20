import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from cassandra.cluster import Cluster

from haversine import haversine

cluster = Cluster(['localhost'])
cluster = cluster.connect('chembise_metar_1_12')


def month_list():
    return [
        {'name': 'janv', 'value': None},
        {'name': 'févr', 'value': None},
        {'name': 'mars', 'value': None},
        {'name': 'avr', 'value': None},
        {'name': 'mai', 'value': None},
        {'name': 'juin', 'value': None},
        {'name': 'juill', 'value': None},
        {'name': 'août', 'value': None},
        {'name': 'sept', 'value': None},
        {'name': 'oct', 'value': None},
        {'name': 'nov', 'value': None},
        {'name': 'déc', 'value': None},
    ]  

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


def get_points_by_day(year, lat, lon):
    global cluster
    query = "SELECT month, day, AVG(tmpf) AS tmpf FROM date_by_location WHERE lat = {} AND lon = {} AND year = {} GROUP BY month, day;".format(lat, lon, year)
    print(query)
    results = cluster.execute(query)

    data = [{'name': i, 'value': None} for i in range(366)]

    for result in results:
        day = datetime.strptime(str(year) + '-' + str(result.month) + '-' + str(result.day), '%Y-%m-%d').timetuple().tm_yday
        try:
            data[day - 1]['value'] = float((result.tmpf - 32) * 5/9)  # Convert to Celsius
        except IndexError:
            print("Could not insert day {} with value {}".format(day, (result.tmpf - 32) * 5/9))

    return data


def get_points_by_month(year, lat, lon, indicator):
    global cluster
    query = "SELECT month, AVG({}) AS data FROM date_by_location WHERE lat = {} AND lon = {} AND year = {} GROUP BY month;".format(indicator, lat, lon, year)
    print(query)
    results = cluster.execute(query)

    data = month_list()

    for result in results:
        try:
            if indicator == 'tmpf':
                value = float((result.data - 32) * 5/9)  # Convert to Celsius
            else:
                value = result.data

            data[result.month - 1]['value'] = value
        except IndexError:
            print("Could not insert month {} with value {}".format(result.month, result.data))

    return data


def get_all(lat, lon, indicator, grain = 'day'):
    data = { '2009': [], '2010': [], '2011': [], '2012': [], '2013': [], '2014': [], '2015': [], '2016': [], '2017': [], '2018': [] }
    n = 366
    if grain == 'month':
        n = 12

    avg = month_list()

    station = get_nearest_station(lat, lon)
    for year in data.keys():
        if grain == 'month':
            data[year] = get_points_by_month(year, station.lat, station.lon, indicator)
        else:
            data[year] = get_points_by_day(year, station.lat, station.lon)

    for i in range(n):
        tmp_len = 0
        tmp_sum = 0

        for year in data.keys():
            if data[year][i]['value'] is not None:
                tmp_len = tmp_len + 1
                tmp_sum = tmp_sum + data[year][i]['value']

        avg[i]['value'] = tmp_sum / tmp_len

    data['avg'] = avg

    return data


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 5:
        print("\nUtiliser ce programme avec 4 arguments : l'année, la latitude et la longitude et l'indicateur.")
        print("ex: question_1.py 2011 52.5644 13.3088 tmpf \n")
        exit()

    year = sys.argv[1]
    lon = float(sys.argv[2])
    lat = float(sys.argv[3])
    indicator = str(sys.argv[4])

    data = get_all(lat, lon, indicator, grain = 'month')
    avg_curve = plt.plot([item['name'] for item in data['avg']], [item['value'] for item in data['avg']])
    try:
        year_curve = plt.plot([item['name'] for item in data[year]], [item['value'] for item in data[year]])
    except KeyError:
        raise KeyError("L'année '"+year+"' n'existe pas.")

    labels = {
        'tmpf': {
            'ylabel': '°C',
            'title': 'Températures moyennes par mois sur l\'année {}'.format(year),
        },
        'relh': {
            'ylabel': '%',
            'title': 'Humidité moyenne par mois sur l\'année {}'.format(year),
        },
        'p01i': {
            'ylabel': 'Pouces',
            'title': 'Précipations sur 1 heure, moyennées par mois sur l\'année {}'.format(year),
        }
    }

    plt.ylabel(labels[indicator]['ylabel'])
    plt.legend((avg_curve[0], year_curve[0]), ('2009-2018', year))
    plt.title(labels[indicator]['title'])
    plt.savefig('{}.png'.format(indicator))
