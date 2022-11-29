CREATE TABLE IF NOT EXISTS store (
    s_store_sk bigint,
    s_store_id char(16),
    s_rec_start_date datev2,
    s_rec_end_date datev2,
    s_closed_date_sk bigint,
    s_store_name varchar(50),
    s_number_employees integer,
    s_floor_space integer,
    s_hours char(20),
    s_manager varchar(40),
    s_market_id integer,
    s_geography_class varchar(100),
    s_market_desc varchar(100),
    s_market_manager varchar(40),
    s_division_id integer,
    s_division_name varchar(50),
    s_company_id integer,
    s_company_name varchar(50),
    s_street_number varchar(10),
    s_street_name varchar(60),
    s_street_type char(15),
    s_suite_number char(10),
    s_city varchar(60),
    s_county varchar(30),
    s_state char(2),
    s_zip char(10),
    s_country varchar(20),
    s_gmt_offset decimalv3(5,2),
    s_tax_precentage decimalv3(5,2)
)
DUPLICATE KEY(s_store_sk, s_store_id)
DISTRIBUTED BY HASH(s_store_id) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)
