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
            'tmpf': float((result.tmpf - 32) * 5/9),
            'relh': floatNone(result.relh),
            'sknt': floatNone(result.sknt),
            'p01i': floatNone(result.p01i),
            'alti': floatNone(result.alti),
            'mslp': floatNone(result.mslp),
            'vsby': floatNone(result.vsby),
            'gust': floatNone(result.gust)
        })

    return data

data = get_points(datetime(2015, 1, 15, 14))

lons = np.array([item['lon'] for item in data])
lats = np.array([item['lat'] for item in data])
tmpf = np.array([item['tmpf'] for item in data])

print(tmpf)

grid_lon = np.arange(lons.min(), lons.max(), 0.5)
grid_lat = np.arange(lats.min(), lats.max(), 0.5)

OK = OrdinaryKriging(lons, lats, tmpf, variogram_model='gaussian', verbose=True, enable_plotting=False, nlags=20)
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

x, y = m(xintrp, yintrp)
ln, lt = m(lons, lats)
cs = ax.contourf(x, y, z1, np.linspace(-5, 30, 35), extend='both', cmap='jet')
cbar = m.colorbar(cs, location='right', pad='7%')


# # Limits of map
# x0, x1 = ax.get_xlim()
# y0, y1 = ax.get_ylim()
# map_edges = np.array([[x0, y0], [x1, y0], [x1, y1], [x0, y1]])
# # Get all polygons used to draw the countries
# polys = [p.boundary for p in m.landpolygons]

# # Combine with map edge
# polys = [map_edges] + polys[:]

# # Create a PathPatch
# codes = [
#     [Path.MOVETO] + [Path.LINETO for p in p[1:]]
#     for p in polys
# ]

# polys_lin = [v for p in polys for v in p]
# codes_lin = [c for c in codes]

# path = Path(polys_lin, codes_lin)
# patch = PathPatch(path, facecolor='white', lw=0)

# ax.add_patch(patch)

for item in data:
    x, y = m(item['lon'], item['lat'])
    m.plot(x, y, 'bo', markersize=10)

plt.savefig('map.png')
