CREATE TABLE IF NOT EXISTS join_t1_dk (
    venue_id int(11) default null,
    venue_text varchar(255) default null,
    dt datetime default null
)
DUPLICATE KEY(venue_id)
DISTRIBUTED BY HASH(venue_id) BUCKETS 3
PROPERTIES ( 
    "replication_num" = "1" 
);