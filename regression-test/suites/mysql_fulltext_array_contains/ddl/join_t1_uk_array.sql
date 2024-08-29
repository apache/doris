CREATE TABLE IF NOT EXISTS join_t1_uk_array (
    venue_id int(11) default null,
    venue_text varchar(255) default null,
    dt datetime default null
)
UNIQUE KEY(venue_id)
DISTRIBUTED BY HASH(venue_id) BUCKETS 3
PROPERTIES ( 
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

