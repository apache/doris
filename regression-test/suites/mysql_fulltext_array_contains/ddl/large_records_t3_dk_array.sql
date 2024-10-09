CREATE TABLE IF NOT EXISTS large_records_t3_dk_array (
    FTS_DOC_ID BIGINT NOT NULL,
	a TEXT,
    b ARRAY<TEXT>,
    INDEX a_idx (a) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'a_idx',
    INDEX b_idx (b) USING INVERTED PROPERTIES("parser"="none") COMMENT 'b_idx'
)
DUPLICATE KEY(FTS_DOC_ID)
DISTRIBUTED BY HASH(FTS_DOC_ID) BUCKETS 3
PROPERTIES ( 
    "replication_num" = "1" 
);