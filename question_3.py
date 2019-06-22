from cassandra.cluster import Cluster
from cassandra.query import tuple_factory
import numpy as np
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt

cluster = Cluster(['localhost'])
cluster = cluster.connect('chembise_metar_1_12')


def aggregate_by_stations(year_start, year_end, month_start=None, month_end=None, metrics=None):
    query_stations = "select distinct lat, lon from date_by_location"
    stations = cluster.execute(query_stations)

    station_values = []
    if metrics is None:
        metrics = ['tmpf', 'dwpf', 'relh', 'drct', 'sknt', 'p01i', 'alti', 'mslp', 'vsby', 'gust', 'skyl1', 'skyl2',
               'skyl3', 'skyl4', 'ice_accretion_1hr', 'ice_accretion_3hr', 'ice_accretion_6hr', 'peak_wind_gust',
               'peak_wind_drct', 'feel']

    avg_select = "AVG(" + "), AVG(".join(metrics) + ")"
    min_select = "MIN(" + "), MIN(".join(metrics) + ")"
    max_select = "MAX(" + "), MAX(".join(metrics) + ")"

    cluster.row_factory = tuple_factory

    if month_start is not None and month_end is not None:
        where_clause = "(year, month) >= ({}, {}) and (year, month) <= ({}, {})".format(year_start, month_start, year_end, month_end)
    else:
        where_clause = "(year >= {} and year <= {})".format(year_start, year_end)

    stations_list = []
    for station in stations:
        query = "SELECT COUNT(*), {}, {}, {} from date_by_location where {} and lat = {} and lon = {}".format(
            avg_select, min_select, max_select, where_clause, station.lat, station.lon)

        results = cluster.execute(query)
        aggregated = results.one()

        if aggregated[0] > 0: # is count(*) > 0 ?
            stations_list.append(station)

            values = ()
            for itup in aggregated[1:]: # because first value is count(*)
                if itup is None:
                    values += (np.nan,)
                else:
                    values += (float(itup),)

            station_values.append(values)

    print("{} stations".format(len(stations_list)))

    return stations_list, station_values

def clustering(values):
    kmeans = KMeans(n_clusters=4, random_state=0).fit(values)

    return kmeans


def kmeans_missing(X, n_clusters, max_iter=10, random_state=None):
    """This is from SO : https://stackoverflow.com/questions/35611465/python-scikit-learn-clustering-with-missing-data
    Perform K-Means clustering on data with missing values.

    Args:
      X: An [n_samples, n_features] array of data to cluster.
      n_clusters: Number of clusters to form.
      max_iter: Maximum number of EM iterations to perform.

    Returns:
      labels: An [n_samples] vector of integer labels.
      centroids: An [n_clusters, n_features] array of cluster centroids.
      X_hat: Copy of X with the missing values filled in.
    """

    # Remove columns with only nan values
    X = np.array(X)
    X = X[:, ~np.all(np.isnan(X), axis=0)]

    # Initialize missing values to their column means
    missing = ~np.isfinite(X)
    mu = np.nanmean(X, 0, keepdims=1)

    X_hat = np.where(missing, mu, X)

    for i in range(max_iter):
        if i > 0:
            # initialize KMeans with the previous set of centroids. this is much
            # faster and makes it easier to check convergence (since labels
            # won't be permuted on every iteration), but might be more prone to
            # getting stuck in local minima.
            cls = KMeans(n_clusters, init=prev_centroids, random_state=random_state)
        else:
            # do multiple random initializations in parallel
            cls = KMeans(n_clusters, n_jobs=-1, random_state=random_state)

        # perform clustering on the filled-in data
        labels = cls.fit_predict(X_hat)
        centroids = cls.cluster_centers_

        # fill in the missing values based on their cluster centroids
        X_hat[missing] = centroids[labels][missing]

        # when the labels have stopped changing then we have converged
        if i > 0 and np.all(labels == prev_labels):
            break

        prev_labels = labels
        prev_centroids = cls.cluster_centers_

    return cls, labels, X_hat


def main(year_start, year_end, month_start=None, month_end=None, n_clusters=6, metrics=None):
    """
    Clusterise l'espace et produit une map
    :param year_start: année de début de la période
    :param year_end: année de fin de la période
    :param month_start: mois de début de la période optionnel
    :param month_end: mois de fin de la période optionnel
    :param n_clusters: nombre de clusters à identifier (par défaut 6)
    :param metrics: list de métrique à considérer (par défaut toute)
    """
    print("1/3 : Aggregation over period {} - {}".format(year_start, year_end))
    stations, station_values = aggregate_by_stations(year_start, year_end, month_start=month_start, month_end=month_end, metrics=metrics)
    print("2/3 : Running K-means")
    clustering, labels, X_hat = kmeans_missing(station_values, n_clusters)

    print("3/3 : Plot of the map")
    # Plot the map
    lons = np.array([float(station.lon) for station in stations])
    lats = np.array([float(station.lat) for station in stations])

    fix, ax = plt.subplots(figsize=(10, 10))
    m = Basemap(llcrnrlon=lons.min() - 0.3,
                llcrnrlat=lats.min() - 0.3,
                urcrnrlon=lons.max() + 0.3,
                urcrnrlat=lats.max() + 0.3,
                projection='merc',
                resolution='h',
                area_thresh=1000,
                ax=ax)
    m.drawcoastlines()
    m.drawcountries()

    colors = ['C' + str(label) for label in labels]
    for index, station in enumerate(stations):
        x, y = m(float(station.lon), float(station.lat))
        m.plot(x, y, 'bo', markersize=10, c=colors[index])

    plt.savefig('map-cluster.png')


if __name__ == '__main__':
    import sys

    if len(sys.argv) != 4 and len(sys.argv) != 6:
        raise RuntimeError(
            "Utiliser ce programme avec 3 ou 5 arguments : le nombre de clusters, l'année de début et l'année de fin.\nex:\tquestion_3.py 3 2011 10 2012 6\nou:\tquestion_3.py 6 2011 2012")

    n_clusters = int(sys.argv[1])
    if len(sys.argv) == 3:
        year_start = int(sys.argv[2])
        year_end = int(sys.argv[3])
        month_start = None
        month_end = None
    else:
        year_start = int(sys.argv[2])
        month_start = int(sys.argv[3])
        year_end = int(sys.argv[4])
        month_end = int(sys.argv[5])

    main(year_start, year_end, month_start=month_start, month_end=month_end, n_clusters=n_clusters)
