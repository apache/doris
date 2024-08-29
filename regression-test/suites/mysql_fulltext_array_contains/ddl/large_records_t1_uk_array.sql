CREATE TABLE IF NOT EXISTS large_records_t1_uk_array (
    FTS_DOC_ID BIGINT NOT NULL,
	a TEXT,
    b ARRAY<TEXT>,
    INDEX a_idx (a) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'a_idx',
    INDEX b_idx (b) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'b_idx'
)
UNIQUE KEY(FTS_DOC_ID)
DISTRIBUTED BY HASH(FTS_DOC_ID) BUCKETS 3
PROPERTIES ( 
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);