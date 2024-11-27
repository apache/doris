CREATE TABLE IF NOT EXISTS join_t2_dk_array (
    entity_id int(11) not null,
    name ARRAY<varchar(255)> not null default '[]',
    INDEX name_idx (name) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'name_idx'
)
DUPLICATE KEY(entity_id)
DISTRIBUTED BY HASH(entity_id) BUCKETS 3
PROPERTIES ( 
    "replication_num" = "1" 
);
