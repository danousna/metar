"""
Example script that scrapes data from the IEM ASOS download service
"""
from __future__ import print_function
import json
import time
import datetime
import csv

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
        x = urlRequest.urlopen(req)
        data = x.read().decode('utf-8')
        
        jdict = json.load(data)
        for site in jdict['features']:
            stations.append(site['properties']['sid'])
    return stations


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
        cr = csv.DictReader(data)
        for row in cr:
            yield row

        #outfn = 'data/%s_%s_%s.txt' % (station, startts.strftime("%Y%m%d%H%M"),
        #                          endts.strftime("%Y%m%d%H%M"))
        #out = open(outfn, 'w')
        #out.write(data)
        #out.close()


#if __name__ == '__main__':
#    main(datetime.datetime(2009, 1, 1), datetime.datetime(2018, 12, 31), "DE")
