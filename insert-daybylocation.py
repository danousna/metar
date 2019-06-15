import cassandra.cluster
import generator

c = cassandra.cluster.Cluster(['localhost'])
ks = c.connect('chembise_metar_1_12')

data = generator.loadata(100)

for row in data:
    cql = '''
    INSERT INTO date_by_location
    (station, daytime, lon, lat, tmpf, dwpf, relh, drct, sknt, p01i, alti, mslp, vsby, gust, skyc1, skyc2, skyc3, skyc4, skyl1, skyl2, skyl3, skyl4, wxcodes, ice_accretion_1hr, ice_accretion_3hr, ice_accretion_6hr, peak_wind_gust, peak_wind_drct, peak_wind_time, feel, metar)
    VALUES ('{}', '{}', {} , {} , {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}', '{}', '{}', '{}', {}, {}, {}, {}, '{}', {}, {}, {}, {}, {}, '{}', {}, '{}')
    ''').format(row['station'], row['valid'], row['lon'], row['lat'], row['tmpf'], row['dwpf'], row['relh'], row['drct'], row['sknt'], row['p01i'], row['alti'], row['mslp'], row['vsby'], row['gust'], row['skyc1'], row['skyc2'], row['skyc3'], row['skyc4'], row['skyl1'], row['skyl2'], row['skyl3'], row['skyl4'], row['wxcodes'], row['ice_accretion_1hr'], row['ice_accretion_3hr'], row['ice_accretion_6hr'], row['peak_wind_gust'], row['peak_wind_drct'], row['peak_wind_time'], row['feel'], row['metar'])

    print(cql)
    break

    ks.execute(cql)

