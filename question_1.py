import matplotlib.pyplot as plt
from fixtures import data

plt.plot([1, 2, 3, 4, 5], [1, 1, 10, 4, 5])
plt.ylabel('some numbers')
plt.savefig('temperatures.png')

def get_points(station, year = '2001'):
    return {
        'temperature': [],
        'day': []
    }

def avg_temperatures():
    years = ['2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010']

    avg_points = {
        'temperature': [],
        'day': []
    }

    for year in years:
        points = get_points(year)
