CREATE TABLE IF NOT EXISTS web_site (
    web_site_sk bigint,
    web_site_id char(16),
    web_rec_start_date date,
    web_rec_end_date date,
    web_name varchar(50),
    web_open_date_sk bigint,
    web_close_date_sk bigint,
    web_class varchar(50),
    web_manager varchar(40),
    web_mkt_id integer,
    web_mkt_class varchar(50),
    web_mkt_desc varchar(100),
    web_market_manager varchar(40),
    web_company_id integer,
    web_company_name char(50),
    web_street_number char(10),
    web_street_name varchar(60),
    web_street_type char(15),
    web_suite_number char(10),
    web_city varchar(60),
    web_county varchar(30),
    web_state char(2),
    web_zip char(10),
    web_country varchar(20),
    web_gmt_offset decimal(5,2),
    web_tax_percentage decimal(5,2),
    INDEX web_site_sk_idx(web_site_sk) USING INVERTED COMMENT "web_site_sk index",
    INDEX web_site_id_idx(web_site_id) USING INVERTED COMMENT "web_site_id index",
    INDEX web_rec_start_date_idx(web_rec_start_date) USING INVERTED COMMENT "web_rec_start_date index",
    INDEX web_name_idx(web_name) USING INVERTED PROPERTIES("parser"="unicode") COMMENT "web_name index",
    INDEX web_class_idx(web_class) USING INVERTED COMMENT "web_class index",
    INDEX web_manager_idx(web_manager) USING INVERTED PROPERTIES("parser"="standard") COMMENT "web_manager index",
    INDEX web_market_manager_idx(web_market_manager) USING INVERTED PROPERTIES("parser"="none") COMMENT "web_market_manager index",
    INDEX web_city_idx(web_city) USING INVERTED COMMENT "web_city index"
)
DUPLICATE KEY(web_site_sk, web_site_id)
DISTRIBUTED BY HASH(web_site_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)



