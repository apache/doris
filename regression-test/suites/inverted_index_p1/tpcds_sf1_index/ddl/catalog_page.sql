CREATE TABLE IF NOT EXISTS catalog_page (
    cp_catalog_page_sk bigint,
    cp_catalog_page_id char(16),
    cp_start_date_sk integer,
    cp_end_date_sk integer,
    cp_department varchar(50),
    cp_catalog_number integer,
    cp_catalog_page_number integer,
    cp_description varchar(100),
    cp_type varchar(100),
    INDEX cp_catalog_page_sk_idx(cp_catalog_page_sk) USING INVERTED COMMENT "cp_catalog_page_sk index"
)
DUPLICATE KEY(cp_catalog_page_sk, cp_catalog_page_id)
DISTRIBUTED BY HASH(cp_catalog_page_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)

