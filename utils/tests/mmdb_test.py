import maxminddb

with maxminddb.open_database('utils/ipinfo/ipinfo_lite.mmdb') as reader:

    print(reader.get('31.181.118.136'))
    print(type(reader.get('31.181.118.136')))

    reader.close()
