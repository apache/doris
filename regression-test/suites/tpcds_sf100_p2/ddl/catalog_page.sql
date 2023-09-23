CREATE TABLE IF NOT EXISTS catalog_page (
  cp_catalog_page_sk bigint not null,
  cp_catalog_page_id char(16) not null,
  cp_start_date_sk integer,
  cp_end_date_sk integer,
  cp_department varchar(50),
  cp_catalog_number integer,
  cp_catalog_page_number integer,
  cp_description varchar(100),
  cp_type varchar(100)
)
DUPLICATE KEY(cp_catalog_page_sk)
DISTRIBUTED BY HASH(cp_catalog_page_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
);