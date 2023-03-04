--from clickbench table hits
CREATE TABLE IF NOT EXISTS  orc_s3_case5 (
    counterid int not null, 
    eventdate int not null, 
    userid bigint not null, 
    eventtime int not null, 
    watchid bigint not null, 
    javaenable smallint not null,
    title string not null,
    goodevent smallint not null,
    clientip int not null,
    regionid int not null,
    counterclass smallint not null,
    os smallint not null,
    useragent smallint not null,
    url string not null,
    referer string not null,
    isrefresh smallint not null,
    referercategoryid smallint not null,
    refererregionid int not null,
    urlcategoryid smallint not null,
    urlregionid int not null
)  
DUPLICATE KEY (CounterID, EventDate, UserID, EventTime, WatchID) 
DISTRIBUTED BY HASH(UserID) BUCKETS 16
PROPERTIES ("replication_num"="1");
