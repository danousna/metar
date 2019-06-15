# !!! Execute this script on the server.

import cassandra
import cassandra.cluster
import downloader
import datetime

data = downloader.download(datetime.datetime(2001, 1, 1), datetime.datetime(2010, 12, 31), "DE")

i = 0
for row in data:
    if i < 10:
        print(row)
    else:
        break
