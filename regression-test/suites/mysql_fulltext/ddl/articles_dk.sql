CREATE TABLE IF NOT EXISTS articles_dk (
    id INT NOT NULL,
	title VARCHAR(200),
	body TEXT,
    INDEX title_idx (title) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'title_idx',
    INDEX body_idx (body) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'body_idx'
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ( 
    "replication_num" = "1" 
);