import csv
import re
import glob, os
from datetime import datetime

def customFloat(data):
    if data == '':
        return None

    return float(data)

def parseData(data):

    data['valid'] = datetime.strptime(data['valid'], '%Y-%m-%d %H:%M')
    data['tmpf'] = customFloat(data['tmpf'])
    data['dwpf'] = customFloat(data['dwpf'])
    data['relh'] = customFloat(data['relh'])
    data['drct'] = customFloat(data['drct'])
    data['sknt'] = customFloat(data['sknt'])
    data['p01i'] = customFloat(data['p01i'])
    data['alti'] = customFloat(data['alti'])
    data['mslp'] = customFloat(data['mslp'])
    data['vsby'] = customFloat(data['vsby'])
    data['gust'] = customFloat(data['gust'])
    data['skyl1'] = customFloat(data['skyl1'])
    data['skyl2'] = customFloat(data['skyl2'])
    data['skyl3'] = customFloat(data['skyl3'])
    data['skyl4'] = customFloat(data['skyl4'])
    data['feel'] = customFloat(data['feel'])

    data['lon'] = customFloat(data['lon'])
    data['lat'] = customFloat(data['lat'])

    data['ice_accretion_1hr'] = customFloat(data['ice_accretion_1hr'])
    data['ice_accretion_3hr'] = customFloat(data['ice_accretion_3hr'])
    data['ice_accretion_6hr'] = customFloat(data['ice_accretion_6hr'])
    data['peak_wind_gust'] = customFloat(data['peak_wind_gust'])
    data['peak_wind_drct'] = customFloat(data['peak_wind_drct'])

    return data

def loadata(limit = False):
    i = 0
    os.chdir("data")
    for file in glob.glob("*.txt"):
        with open(file) as f:
            for row in csv.DictReader(f):
                if not limit or i < limit:
                    i += 1
                    yield parseData(row)
                else:
                    break

for r in loadata(300):
    print(r)
