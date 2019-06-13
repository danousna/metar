import csv
import re
import glob, os

def loadata(limit = False):
    i = 0
    os.chdir("data")
    for file in glob.glob("*.txt"):
        with open(file) as f:
            for row in csv.DictReader(f):
                if not limit or i < limit:
                    i += 1
                    yield row
                else:
                    break

for r in loadata(300):
    print(r)
