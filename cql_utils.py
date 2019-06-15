import datetime


def split_daytime(row):
    row['year'] = int(row['valid'].strftime("%Y"))
    row['month'] = int(row['valid'].strftime("%m"))
    row['day'] = int(row['valid'].strftime("%d"))
    row['hour'] = int(row['valid'].strftime("%H"))
    row['minute'] = int(row['valid'].strftime("%M"))
    row['second'] = int(row['valid'].strftime("%S"))

    row['valid'] = None

    return row


def format_insert_query(table, data, mapping = {}):
    columns = []
    values = []
    for col, value in data.items():
        if value is not None and value != '':
            if col in mapping:
                columns.append(mapping[col])
            else:
                columns.append(col)

            if isinstance(value, str):
                values.append("'"+value+"'")
            elif isinstance(value, datetime.datetime):
                values.append("'"+value.strftime("%Y-%m-%d %H:%M:%S")+"'")
            else:
                values.append(str(value))


    return "INSERT INTO {} ({}) VALUES ({})".format(table, ', '.join(columns), ', '.join(values))



"""
for row in data:

    cql = ""INSERT INTO date_by_location
      (station, daytime, lon, lat, tmpf, dwpf, relh, drct, sknt, p01i, alti, mslp, vsby, gust, skyc1, skyc2, skyc3, skyc4, skyl1, skyl2, skyl3, skyl4, wxcodes, ice_accretion_1hr, ice_accretion_3hr, ice_accretion_6hr, peak_wind_gust, peak_wind_drct, peak_wind_time, feel, metar)
      VALUES ('{}', '{}', {} , {} , {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}', '{}', '{}', '{}', {}, {}, {}, {}, '{}', {}, {}, {}, {}, {}, '{}', {}, '{}')""

    cql = cql.format(row['station'], row['valid'], row['lon'], row['lat'], row['tmpf'], row['dwpf'], row['relh'], row['drct'], row['sknt'], row['p01i'], row['alti'], row['mslp'], row['vsby'], row['gust'], row['skyc1'], row['skyc2'], row['skyc3'], row['skyc4'], row['skyl1'], row['skyl2'], row['skyl3'], row['skyl4'], row['wxcodes'], row['ice_accretion_1hr'], row['ice_accretion_3hr'], row['ice_accretion_6hr'], row['peak_wind_gust'], row['peak_wind_drct'], row['peak_wind_time'], row['feel'], row['metar'])

    print(cql)
    break

    ks.execute(cql)
"""