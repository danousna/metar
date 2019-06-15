"""
Example script that scrapes data from the IEM ASOS download service
"""
import json
import time
import pandas as pd
from datetime import datetime

import urllib.request as urllib
import urllib.request as urlRequest
import urllib.parse as urlParse

# Number of attempts to download data
MAX_ATTEMPTS = 6
# HTTPS here can be problematic for installs that don't have Lets Encrypt CA
SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"


def download_data(uri):
    """Fetch the data from the IEM
    The IEM download service has some protections in place to keep the number
    of inbound requests in check.  This function implements an exponential
    backoff to keep individual downloads from erroring.
    Args:
      uri (string): URL to fetch
    Returns:
      string data
    """
    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            return pd.read_csv(uri).T.to_dict()
        except Exception as exp:
            print("download_data(%s) failed with %s" % (uri, exp))
            time.sleep(5)
        attempt += 1

    print("Exhausted attempts to download, returning empty data")
    return ""


def get_stations_from_networks(states):
    """Build a station list by using a bunch of IEM networks."""
    stations = []
    # IEM quirk to have Iowa AWOS sites in its own labeled network
    networks = []
    for state in states.split():
        networks.append("%s__ASOS" % (state,))

    for network in networks:
        # Get metadata
        uri = ("https://mesonet.agron.iastate.edu/"
               "geojson/network/%s.geojson") % (network,)
        req = urlRequest.Request(uri)
        # open the url
        data = urlRequest.urlopen(req)
        #data = x.read().decode('utf-8')

        jdict = json.load(data)
        for site in jdict['features']:
            stations.append(site['properties']['sid'])
    return stations


def floatCast(data):
    if data == '':
        return None

    return float(data)


def parseData(data):
    data['valid'] = datetime.strptime(data['valid'], '%Y-%m-%d %H:%M')
    data['tmpf'] = floatCast(data['tmpf'])
    data['dwpf'] = floatCast(data['dwpf'])
    data['relh'] = floatCast(data['relh'])
    data['drct'] = floatCast(data['drct'])
    data['sknt'] = floatCast(data['sknt'])
    data['p01i'] = floatCast(data['p01i'])
    data['alti'] = floatCast(data['alti'])
    data['mslp'] = floatCast(data['mslp'])
    data['vsby'] = floatCast(data['vsby'])
    data['gust'] = floatCast(data['gust'])
    data['skyl1'] = floatCast(data['skyl1'])
    data['skyl2'] = floatCast(data['skyl2'])
    data['skyl3'] = floatCast(data['skyl3'])
    data['skyl4'] = floatCast(data['skyl4'])
    data['feel'] = floatCast(data['feel'])

    data['lon'] = floatCast(data['lon'])
    data['lat'] = floatCast(data['lat'])

    data['ice_accretion_1hr'] = floatCast(data['ice_accretion_1hr'])
    data['ice_accretion_3hr'] = floatCast(data['ice_accretion_3hr'])
    data['ice_accretion_6hr'] = floatCast(data['ice_accretion_6hr'])
    data['peak_wind_gust'] = floatCast(data['peak_wind_gust'])
    data['peak_wind_drct'] = floatCast(data['peak_wind_drct'])

    return data

def download(startts, endts, states):
    """Our main method"""

    service = SERVICE + "data=all&tz=Etc/UTC&format=onlycomma&latlon=yes&missing=empty&"

    service += startts.strftime('year1=%Y&month1=%m&day1=%d&')
    service += endts.strftime('year2=%Y&month2=%m&day2=%d&')

    stations = get_stations_from_networks(states)
    i = 0
    for station in stations:
        i+=1
        uri = '%s&station=%s' % (service, station)
        print('Downloading (%s/%s): %s' % (i,len(stations),station, ))
        data = download_data(uri)

        yield data[0]

data = download(datetime(2006, 1, 1), datetime(2015, 12, 31), "IE")

for item in data:
    print(item)
