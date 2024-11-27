CREATE TABLE IF NOT EXISTS articles_dk_array (
    id INT NOT NULL,
	title ARRAY<VARCHAR(200)>,
	body ARRAY<TEXT>,
    INDEX title_idx (title) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'title_idx',
    INDEX body_idx (body) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'body_idx'
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ( 
    "replication_num" = "1" 
);