import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap, cm
from datetime import datetime
from cassandra.cluster import Cluster
from scipy.interpolate import griddata


cluster = Cluster(['localhost'])
cluster = cluster.connect('chembise_metar_1_12')


def floatNone(val):
    if val is not None:
        return float(val)
    else:
        return None


def get_points(dt):
    global cluster
    query = "SELECT * FROM data_by_datetime WHERE year = {} AND month = {} AND day = {} AND hour = {};".format(
        dt.strftime('%Y'),
        dt.strftime('%-m'),
        dt.strftime('%-d'),
        dt.strftime('%-H')
    )
    print(query)
    results = cluster.execute(query)

    data = []

    for result in results:
        data.append({
            'lat': floatNone(result.lat),
            'lon': floatNone(result.lon),
            'tmpf': floatNone(result.tmpf),
            'relh': floatNone(result.relh),
            'sknt': floatNone(result.sknt),
            'p01i': floatNone(result.p01i),
            'alti': floatNone(result.alti),
            'mslp': floatNone(result.mslp),
            'vsby': floatNone(result.vsby),
            'gust': floatNone(result.gust)
        })

    return data

data = get_points(datetime(2015, 3, 15, 12))

lats = sorted([item['lat'] for item in data])
lons = sorted([item['lon'] for item in data])
tmpf = []

for i, lat in enumerate(lats):
    tmpf.append([])
    for j, lon in enumerate(lons):
        tmpf[i].append(np.nan)
        for item in data:
            if item['lat'] == lat and item['lon'] == lon:
                tmpf[i][j] = item['tmpf']

print(tmpf)

m = Basemap(projection = 'mill',
            llcrnrlat = 46.90,
            llcrnrlon = 5.75,
            urcrnrlat = 55,
            urcrnrlon = 15.21,
            resolution = 'l')

m.drawcoastlines()
m.drawcountries()
m.drawstates()

x, y = m(*np.meshgrid(lons, lats))

m.pcolormesh(x, y, tmpf, shading='flat', cmap=plt.cm.jet)
m.colorbar(location='right')

plt.savefig('map.png')
