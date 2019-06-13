import csv
import re
import glob, os

def loadata():
    os.chdir("data")
    for file in glob.glob("*.csv"):
        with open(file) as f:
            for row in csv.DictReader(f):
                yield row

for r in loadata():
    print(r)
