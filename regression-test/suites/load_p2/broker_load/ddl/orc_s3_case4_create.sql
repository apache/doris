--from clickbench table hits
CREATE TABLE IF NOT EXISTS  orc_s3_case4 (
    counterid int not null, 
    eventdate int not null, 
    userid bigint not null, 
    eventtime int not null, 
    watchid bigint not null, 
    javaenable smallint not null,
    title string not null,
    goodevent smallint not null
)  
DUPLICATE KEY (CounterID, EventDate, UserID, EventTime, WatchID) 
DISTRIBUTED BY HASH(UserID) BUCKETS 16
PROPERTIES ("replication_num"="1");
