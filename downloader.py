"""
Example script that scrapes data from the IEM ASOS download service
"""
from __future__ import print_function
import json
import time
import datetime
import csv
import re
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
            #data = urlopen(uri, timeout=300).read().decode('utf-8')

            req = urlRequest.Request(uri)
            # open the url
            x = urlRequest.urlopen(req)
            data = x.read().decode('utf-8')

            return data.splitlines()

        except Exception as exp:
            print("download_data(%s) failed with %s" % (uri, exp))
            time.sleep(5)
        attempt += 1

    print("Exhausted attempts to download, returning empty data")
    return ""


def get_stations_from_filelist(filename):
    """Build a listing of stations from a simple file listing the stations.
    The file should simply have one station per line.
    """
    stations = []
    for line in open(filename):
        stations.append(line.strip())
    return stations


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

OrderedDict([('station', 'EDAC'), ('valid', '2013-04-09 07:50'), ('lon', '12.5064'), ('lat', '50.9818'), ('tmpf', '42.80'), ('dwpf', '32.00'), ('relh', '65.38'), ('drct', '150.00'), ('sknt', '5.00'), ('p01i', '0.00'), ('alti', '29.68'), ('mslp', ''), ('vsby', '6.21'), ('gust', ''), ('skyc1', 'BKN'), ('skyc2', ''), ('skyc3', ''), ('skyc4', ''), ('skyl1', '4800.00'), ('skyl2', ''), ('skyl3', ''), ('skyl4', ''), ('wxcodes', ''), ('ice_accretion_1hr', ''), ('ice_accretion_3hr', ''), ('ice_accretion_6hr', ''), ('peak_wind_gust', ''), ('peak_wind_drct', ''), ('peak_wind_time', ''), ('feel', '39.22'), ('metar', 'EDAC 090750Z 15005KT 120V190 9999 BKN048 06/M00 Q1005')])

dateparser = re.compile(
    "(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+):(?P<seconds>\d+\.?\d*)"
)

def parseData(data):

    data['valid'] = datetime.strptime(data['valid'], '%Y-%m-%d %H:%M')
    data['lon'] = float(data['lon'])
    data['lat'] = float(data['lat'])
    data['feel'] = float(data['feel'])
    data['tmpf'] = float(data['tmpf'])
    data['dwpf'] = float(data['dwpf'])
    data['relh'] = float(data['relh'])
    data['drct'] = float(data['drct'])
    data['sknt'] = float(data['sknt'])
    data['p01i'] = float(data['p01i'])
    data['alti'] = float(data['alti'])

    return data

def download(startts, endts, states):
    """Our main method"""

    service = SERVICE + "data=all&tz=Etc/UTC&format=onlycomma&latlon=yes&missing=empty&"

    service += startts.strftime('year1=%Y&month1=%m&day1=%d&')
    service += endts.strftime('year2=%Y&month2=%m&day2=%d&')

    # Two examples of how to specify a list of stations
    stations = get_stations_from_networks(states)
    # stations = get_stations_from_filelist("mystations.txt")
    i = 0
    for station in stations:
        i+=1
        uri = '%s&station=%s' % (service, station)
        print('Downloading (%s/%s): %s' % (i,len(stations),station, ))
        data = download_data(uri)
        cr = csv.DictReader(data, quoting=csv.QUOTE_NONNUMERIC)
        for row in cr:
            yield row

        #outfn = 'data/%s_%s_%s.txt' % (station, startts.strftime("%Y%m%d%H%M"),
        #                          endts.strftime("%Y%m%d%H%M"))
        #out = open(outfn, 'w')
        #out.write(data)
        #out.close()


#if __name__ == '__main__':
#    main(datetime.datetime(2009, 1, 1), datetime.datetime(2018, 12, 31), "DE")
