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
    web_tax_percentage decimal(5,2)
)
UNIQUE KEY(web_site_sk)
CLUSTER BY(web_company_id, web_site_id)
DISTRIBUTED BY HASH(web_site_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
)



