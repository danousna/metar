import downloader
import datetime

rows = downloader.download(datetime.datetime(2009, 1, 1), datetime.datetime(2018, 12, 31), "DE")

for row in rows:
    print(row)
