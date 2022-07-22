CREATE TABLE IF NOT EXISTS call_center (
    cc_call_center_sk bigint,
    cc_call_center_id char(16),
    cc_rec_start_date date,
    cc_rec_end_date date,
    cc_closed_date_sk integer,
    cc_open_date_sk integer,
    cc_name varchar(50),
    cc_class varchar(50),
    cc_employees integer,
    cc_sq_ft integer,
    cc_hours char(20),
    cc_manager varchar(40),
    cc_mkt_id integer,
    cc_mkt_class char(50),
    cc_mkt_desc varchar(100),
    cc_market_manager varchar(40),
    cc_division integer,
    cc_division_name varchar(50),
    cc_company integer,
    cc_company_name char(50),
    cc_street_number char(10),
    cc_street_name varchar(60),
    cc_street_type char(15),
    cc_suite_number char(10),
    cc_city varchar(60),
    cc_county varchar(30),
    cc_state char(2),
    cc_zip char(10),
    cc_country varchar(20),
    cc_gmt_offset decimal(5,2),
    cc_tax_percentage decimal(5,2)
 )
DUPLICATE KEY(cc_call_center_sk, cc_call_center_id)
DISTRIBUTED BY HASH(cc_call_center_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)
