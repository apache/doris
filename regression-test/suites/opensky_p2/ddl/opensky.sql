Create table if not exists opensky (
        origin varchar(128),
        destination varchar(128),
        callsign varchar(128),
        number String,
        icao24 String,
        registration String,
        typecode String,
        firstseen DateTime,
        lastseen DateTime,
        day DateTime,
        latitude_1 double,
        longitude_1 double,
        altitude_1 double,
        latitude_2 double,
        longitude_2 double,
        altitude_2 double )
    DUPLICATE KEY (origin, destination, callsign) DISTRIBUTED BY HASH(origin, destination, callsign) BUCKETS 4 properties ("replication_num"="1");
