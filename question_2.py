import glob
from pykrige.ok import OrdinaryKriging
from pykrige.kriging_tools import write_asc_grid
import pykrige.kriging_tools as kt
import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.basemap import Basemap
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Path, PathPatch
from datetime import datetime
from cassandra.cluster import Cluster

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
            'tmpf': float((result.tmpf - 32) * 5/9) if result.tmpf != None else None,
            'relh': floatNone(result.relh),
            'sknt': floatNone(result.sknt),
            'p01i': floatNone(result.p01i),
            'alti': floatNone(result.alti),
            'mslp': floatNone(result.mslp),
            'vsby': floatNone(result.vsby),
            'gust': floatNone(result.gust)
        })

    return data


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 6:
        print("\nUtiliser ce programme avec 5 arguments : l'année, le mois, le jour, l'heure et un indicateur.")
        print("ex: question_2.py 2015 1 15 14 tmpf \n")
        exit()

    year = int(sys.argv[1])
    month = int(sys.argv[2])
    day = int(sys.argv[3])
    hour = int(sys.argv[4])
    indicator = str(sys.argv[5])

    data = get_points(datetime(year, month, day, hour))

    # Transform to np.array
    lons = np.array([item['lon'] for item in data])
    lats = np.array([item['lat'] for item in data])
    indicators = {
        'tmpf': np.array([item['tmpf'] for item in data]),
        'relh': np.array([item['relh'] for item in data]),
        'p01i': np.array([item['p01i'] for item in data])
    }

    # Make grid of lat and lons
    grid_lon = np.arange(lons.min(), lons.max(), 0.5)
    grid_lat = np.arange(lats.min(), lats.max(), 0.5)

    # Krigging
    OK = OrdinaryKriging(lons, lats, indicators[indicator], variogram_model='gaussian', verbose=True, enable_plotting=False, nlags=20)
    z1, ss1 = OK.execute('grid', grid_lon, grid_lat)

    xintrp, yintrp = np.meshgrid(grid_lon, grid_lat)
    fix, ax = plt.subplots(figsize=(10, 10))

    m = Basemap(llcrnrlon = lons.min() - 0.3,
                llcrnrlat = lats.min() - 0.3,
                urcrnrlon = lons.max() + 0.3,
                urcrnrlat = lats.max() + 0.3,
                projection = 'merc',
                resolution = 'h',
                area_thresh = 1000,
                ax = ax)
    m.drawcoastlines()
    m.drawcountries()

    labels = {
        'tmpf': {
            'min': -5,
            'max': 30,
            'title': 'Carte des températures en °C',
        },
        'relh': {
            'min': 0,
            'max': 100,
            'title': 'Carte l\'humidité en \%',
        },
        'p01i': {
            'min': 0,
            'max': 100,
            'title': 'Précipations en pouces',
        }
    }

    x, y = m(xintrp, yintrp)
    ln, lt = m(lons, lats)
    cs = ax.contourf(x, y, z1, np.linspace(
        labels[indicator]['min'], 
        labels[indicator]['max'], 
        labels[indicator]['min'] + abs(labels[indicator]['max'])
    ), extend='both', cmap='jet')
    cbar = m.colorbar(cs, location='right', pad='7%')

    # Plot station markers
    for item in data:
        x, y = m(item['lon'], item['lat'])
        m.plot(x, y, 'bo', markersize=10)

    # plt.ylabel(labels[indicator]['ylabel'])
    # plt.legend((avg_curve[0], year_curve[0]), ('2009-2018', year))
    plt.title(labels[indicator]['title'] + ' au {}/{}/{}'.format(day, month, year))
    plt.savefig('map.png')
