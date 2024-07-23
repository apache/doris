-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
-- 
--   http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

CREATE TABLE IF NOT EXISTS customer_demographics (
    cd_demo_sk bigint not null,
    cd_gender char(1),
    cd_marital_status char(1),
    cd_education_status char(20),
    cd_purchase_estimate integer,
    cd_credit_rating char(10),
    cd_dep_count integer,
    cd_dep_employed_count integer,
    cd_dep_college_count integer
)
DUPLICATE KEY(cd_demo_sk)
DISTRIBUTED BY HASH(cd_demo_sk) BUCKETS 12
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE IF NOT EXISTS reason (
    r_reason_sk bigint not null,
    r_reason_id char(16) not null,
    r_reason_desc char(100)
 )
DUPLICATE KEY(r_reason_sk)
DISTRIBUTED BY HASH(r_reason_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE IF NOT EXISTS date_dim (
    d_date_sk bigint not null,
    d_date_id char(16) not null,
    d_date date,
    d_month_seq integer,
    d_week_seq integer,
    d_quarter_seq integer,
    d_year integer,
    d_dow integer,
    d_moy integer,
    d_dom integer,
    d_qoy integer,
    d_fy_year integer,
    d_fy_quarter_seq integer,
    d_fy_week_seq integer,
    d_day_name char(9),
    d_quarter_name char(6),
    d_holiday char(1),
    d_weekend char(1),
    d_following_holiday char(1),
    d_first_dom integer,
    d_last_dom integer,
    d_same_day_ly integer,
    d_same_day_lq integer,
    d_current_day char(1),
    d_current_week char(1),
    d_current_month char(1),
    d_current_quarter char(1),
    d_current_year char(1)
)
DUPLICATE KEY(d_date_sk)
DISTRIBUTED BY HASH(d_date_sk) BUCKETS 12
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE IF NOT EXISTS warehouse (
    w_warehouse_sk bigint not null,
    w_warehouse_id char(16) not null,
    w_warehouse_name varchar(20),
    w_warehouse_sq_ft integer,
    w_street_number char(10),
    w_street_name varchar(60),
    w_street_type char(15),
    w_suite_number char(10),
    w_city varchar(60),
    w_county varchar(30),
    w_state char(2),
    w_zip char(10),
    w_country varchar(20),
    w_gmt_offset decimal(5,2)
)
DUPLICATE KEY(w_warehouse_sk)
DISTRIBUTED BY HASH(w_warehouse_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE IF NOT EXISTS catalog_sales (
    cs_item_sk bigint not null,
    cs_order_number bigint not null,
    cs_sold_date_sk bigint,
    cs_sold_time_sk bigint,
    cs_ship_date_sk bigint,
    cs_bill_customer_sk bigint,
    cs_bill_cdemo_sk bigint,
    cs_bill_hdemo_sk bigint,
    cs_bill_addr_sk bigint,
    cs_ship_customer_sk bigint,
    cs_ship_cdemo_sk bigint,
    cs_ship_hdemo_sk bigint,
    cs_ship_addr_sk bigint,
    cs_call_center_sk bigint,
    cs_catalog_page_sk bigint,
    cs_ship_mode_sk bigint,
    cs_warehouse_sk bigint,
    cs_promo_sk bigint,
    cs_quantity integer,
    cs_wholesale_cost decimal(7,2),
    cs_list_price decimal(7,2),
    cs_sales_price decimal(7,2),
    cs_ext_discount_amt decimal(7,2),
    cs_ext_sales_price decimal(7,2),
    cs_ext_wholesale_cost decimal(7,2),
    cs_ext_list_price decimal(7,2),
    cs_ext_tax decimal(7,2),
    cs_coupon_amt decimal(7,2),
    cs_ext_ship_cost decimal(7,2),
    cs_net_paid decimal(7,2),
    cs_net_paid_inc_tax decimal(7,2),
    cs_net_paid_inc_ship decimal(7,2),
    cs_net_paid_inc_ship_tax decimal(7,2),
    cs_net_profit decimal(7,2)
)
DUPLICATE KEY(cs_item_sk, cs_order_number)
DISTRIBUTED BY HASH(cs_item_sk, cs_order_number) BUCKETS 32
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "catalog"
);
CREATE TABLE IF NOT EXISTS call_center (
  cc_call_center_sk bigint not null,
  cc_call_center_id char(16) not null,
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
DUPLICATE KEY(cc_call_center_sk)
DISTRIBUTED BY HASH(cc_call_center_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);

CREATE TABLE IF NOT EXISTS inventory (
    inv_date_sk bigint not null,
    inv_item_sk bigint not null,
    inv_warehouse_sk bigint,
    inv_quantity_on_hand integer
)
DUPLICATE KEY(inv_date_sk, inv_item_sk, inv_warehouse_sk)
DISTRIBUTED BY HASH(inv_date_sk, inv_item_sk, inv_warehouse_sk) BUCKETS 32
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE IF NOT EXISTS catalog_returns (
  cr_item_sk bigint not null,
  cr_order_number bigint not null,
  cr_returned_date_sk bigint,
  cr_returned_time_sk bigint,
  cr_refunded_customer_sk bigint,
  cr_refunded_cdemo_sk bigint,
  cr_refunded_hdemo_sk bigint,
  cr_refunded_addr_sk bigint,
  cr_returning_customer_sk bigint,
  cr_returning_cdemo_sk bigint,
  cr_returning_hdemo_sk bigint,
  cr_returning_addr_sk bigint,
  cr_call_center_sk bigint,
  cr_catalog_page_sk bigint,
  cr_ship_mode_sk bigint,
  cr_warehouse_sk bigint,
  cr_reason_sk bigint,
  cr_return_quantity integer,
  cr_return_amount decimal(7,2),
  cr_return_tax decimal(7,2),
  cr_return_amt_inc_tax decimal(7,2),
  cr_fee decimal(7,2),
  cr_return_ship_cost decimal(7,2),
  cr_refunded_cash decimal(7,2),
  cr_reversed_charge decimal(7,2),
  cr_store_credit decimal(7,2),
  cr_net_loss decimal(7,2)
)
DUPLICATE KEY(cr_item_sk, cr_order_number)
DISTRIBUTED BY HASH(cr_item_sk, cr_order_number) BUCKETS 32
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "catalog"
);

CREATE TABLE IF NOT EXISTS household_demographics (
    hd_demo_sk bigint not null,
    hd_income_band_sk bigint,
    hd_buy_potential char(15),
    hd_dep_count integer,
    hd_vehicle_count integer
)
DUPLICATE KEY(hd_demo_sk)
DISTRIBUTED BY HASH(hd_demo_sk) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE IF NOT EXISTS customer_address (
    ca_address_sk bigint not null,
    ca_address_id char(16) not null,
    ca_street_number char(10),
    ca_street_name varchar(60),
    ca_street_type char(15),
    ca_suite_number char(10),
    ca_city varchar(60),
    ca_county varchar(30),
    ca_state char(2),
    ca_zip char(10),
    ca_country varchar(20),
    ca_gmt_offset decimal(5,2),
    ca_location_type char(20)
)
DUPLICATE KEY(ca_address_sk)
DISTRIBUTED BY HASH(ca_address_sk) BUCKETS 12
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE IF NOT EXISTS income_band (
    ib_income_band_sk bigint not null,
    ib_lower_bound integer,
    ib_upper_bound integer
)
DUPLICATE KEY(ib_income_band_sk)
DISTRIBUTED BY HASH(ib_income_band_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);
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
CREATE TABLE IF NOT EXISTS item (
    i_item_sk bigint not null,
    i_item_id char(16) not null,
    i_rec_start_date date,
    i_rec_end_date date,
    i_item_desc varchar(200),
    i_current_price decimal(7,2),
    i_wholesale_cost decimal(7,2),
    i_brand_id integer,
    i_brand char(50),
    i_class_id integer,
    i_class char(50),
    i_category_id integer,
    i_category char(50),
    i_manufact_id integer,
    i_manufact char(50),
    i_size char(20),
    i_formulation char(20),
    i_color char(20),
    i_units char(10),
    i_container char(10),
    i_manager_id integer,
    i_product_name char(50)
)
DUPLICATE KEY(i_item_sk)
DISTRIBUTED BY HASH(i_item_sk) BUCKETS 12
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE IF NOT EXISTS web_returns (
    wr_item_sk bigint not null,
    wr_order_number bigint not null,
    wr_returned_date_sk bigint,
    wr_returned_time_sk bigint,
    wr_refunded_customer_sk bigint,
    wr_refunded_cdemo_sk bigint,
    wr_refunded_hdemo_sk bigint,
    wr_refunded_addr_sk bigint,
    wr_returning_customer_sk bigint,
    wr_returning_cdemo_sk bigint,
    wr_returning_hdemo_sk bigint,
    wr_returning_addr_sk bigint,
    wr_web_page_sk bigint,
    wr_reason_sk bigint,
    wr_return_quantity integer,
    wr_return_amt decimal(7,2),
    wr_return_tax decimal(7,2),
    wr_return_amt_inc_tax decimal(7,2),
    wr_fee decimal(7,2),
    wr_return_ship_cost decimal(7,2),
    wr_refunded_cash decimal(7,2),
    wr_reversed_charge decimal(7,2),
    wr_account_credit decimal(7,2),
    wr_net_loss decimal(7,2)
)
DUPLICATE KEY(wr_item_sk, wr_order_number)
DISTRIBUTED BY HASH(wr_item_sk, wr_order_number) BUCKETS 32
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "web"
);
CREATE TABLE IF NOT EXISTS web_site (
    web_site_sk bigint not null,
    web_site_id char(16) not null,
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
DUPLICATE KEY(web_site_sk)
DISTRIBUTED BY HASH(web_site_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE IF NOT EXISTS promotion (
    p_promo_sk bigint not null,
    p_promo_id char(16) not null,
    p_start_date_sk bigint,
    p_end_date_sk bigint,
    p_item_sk bigint,
    p_cost decimal(15,2),
    p_response_targe integer,
    p_promo_name char(50),
    p_channel_dmail char(1),
    p_channel_email char(1),
    p_channel_catalog char(1),
    p_channel_tv char(1),
    p_channel_radio char(1),
    p_channel_press char(1),
    p_channel_event char(1),
    p_channel_demo char(1),
    p_channel_details varchar(100),
    p_purpose char(15),
    p_discount_active char(1)
)
DUPLICATE KEY(p_promo_sk)
DISTRIBUTED BY HASH(p_promo_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE IF NOT EXISTS web_sales (
    ws_item_sk bigint not null,
    ws_order_number bigint not null,
    ws_sold_date_sk bigint,
    ws_sold_time_sk bigint,
    ws_ship_date_sk bigint,
    ws_bill_customer_sk bigint,
    ws_bill_cdemo_sk bigint,
    ws_bill_hdemo_sk bigint,
    ws_bill_addr_sk bigint,
    ws_ship_customer_sk bigint,
    ws_ship_cdemo_sk bigint,
    ws_ship_hdemo_sk bigint,
    ws_ship_addr_sk bigint,
    ws_web_page_sk bigint,
    ws_web_site_sk bigint,
    ws_ship_mode_sk bigint,
    ws_warehouse_sk bigint,
    ws_promo_sk bigint,
    ws_quantity integer,
    ws_wholesale_cost decimal(7,2),
    ws_list_price decimal(7,2),
    ws_sales_price decimal(7,2),
    ws_ext_discount_amt decimal(7,2),
    ws_ext_sales_price decimal(7,2),
    ws_ext_wholesale_cost decimal(7,2),
    ws_ext_list_price decimal(7,2),
    ws_ext_tax decimal(7,2),
    ws_coupon_amt decimal(7,2),
    ws_ext_ship_cost decimal(7,2),
    ws_net_paid decimal(7,2),
    ws_net_paid_inc_tax decimal(7,2),
    ws_net_paid_inc_ship decimal(7,2),
    ws_net_paid_inc_ship_tax decimal(7,2),
    ws_net_profit decimal(7,2)
)
DUPLICATE KEY(ws_item_sk, ws_order_number)
DISTRIBUTED BY HASH(ws_item_sk, ws_order_number) BUCKETS 32
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "web"
);
CREATE TABLE IF NOT EXISTS store (
    s_store_sk bigint not null,
    s_store_id char(16) not null,
    s_rec_start_date date,
    s_rec_end_date date,
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
    s_gmt_offset decimal(5,2),
    s_tax_precentage decimal(5,2)
)
DUPLICATE KEY(s_store_sk)
DISTRIBUTED BY HASH(s_store_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE IF NOT EXISTS time_dim (
    t_time_sk bigint not null,
    t_time_id char(16) not null,
    t_time integer,
    t_hour integer,
    t_minute integer,
    t_second integer,
    t_am_pm char(2),
    t_shift char(20),
    t_sub_shift char(20),
    t_meal_time char(20)
)
DUPLICATE KEY(t_time_sk)
DISTRIBUTED BY HASH(t_time_sk) BUCKETS 12
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE IF NOT EXISTS web_page (
        wp_web_page_sk bigint not null,
        wp_web_page_id char(16) not null,
        wp_rec_start_date date,
        wp_rec_end_date date,
        wp_creation_date_sk bigint,
        wp_access_date_sk bigint,
        wp_autogen_flag char(1),
        wp_customer_sk bigint,
        wp_url varchar(100),
        wp_type char(50),
        wp_char_count integer,
        wp_link_count integer,
        wp_image_count integer,
        wp_max_ad_count integer
)
DUPLICATE KEY(wp_web_page_sk)
DISTRIBUTED BY HASH(wp_web_page_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE IF NOT EXISTS store_returns (
    sr_item_sk bigint not null,
    sr_ticket_number bigint not null,
    sr_returned_date_sk bigint,
    sr_return_time_sk bigint,
    sr_customer_sk bigint,
    sr_cdemo_sk bigint,
    sr_hdemo_sk bigint,
    sr_addr_sk bigint,
    sr_store_sk bigint,
    sr_reason_sk bigint,
    sr_return_quantity integer,
    sr_return_amt decimal(7,2),
    sr_return_tax decimal(7,2),
    sr_return_amt_inc_tax decimal(7,2),
    sr_fee decimal(7,2),
    sr_return_ship_cost decimal(7,2),
    sr_refunded_cash decimal(7,2),
    sr_reversed_charge decimal(7,2),
    sr_store_credit decimal(7,2),
    sr_net_loss decimal(7,2)
)
duplicate key(sr_item_sk, sr_ticket_number)
distributed by hash (sr_item_sk, sr_ticket_number) buckets 32
properties (
  "replication_num" = "1",
  "colocate_with" = "store"
);
CREATE TABLE IF NOT EXISTS store_sales (
    ss_item_sk bigint not null,
    ss_ticket_number bigint not null,
    ss_sold_date_sk bigint,
    ss_sold_time_sk bigint,
    ss_customer_sk bigint,
    ss_cdemo_sk bigint,
    ss_hdemo_sk bigint,
    ss_addr_sk bigint,
    ss_store_sk bigint,
    ss_promo_sk bigint,
    ss_quantity integer,
    ss_wholesale_cost decimal(7,2),
    ss_list_price decimal(7,2),
    ss_sales_price decimal(7,2),
    ss_ext_discount_amt decimal(7,2),
    ss_ext_sales_price decimal(7,2),
    ss_ext_wholesale_cost decimal(7,2),
    ss_ext_list_price decimal(7,2),
    ss_ext_tax decimal(7,2),
    ss_coupon_amt decimal(7,2),
    ss_net_paid decimal(7,2),
    ss_net_paid_inc_tax decimal(7,2),
    ss_net_profit decimal(7,2)
)
DUPLICATE KEY(ss_item_sk, ss_ticket_number)
DISTRIBUTED BY HASH(ss_item_sk, ss_ticket_number) BUCKETS 32
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "store"
);
CREATE TABLE IF NOT EXISTS ship_mode (
    sm_ship_mode_sk bigint not null,
    sm_ship_mode_id char(16) not null,
    sm_type char(30),
    sm_code char(10),
    sm_carrier char(20),
    sm_contract char(20)
)
DUPLICATE KEY(sm_ship_mode_sk)
DISTRIBUTED BY HASH(sm_ship_mode_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE IF NOT EXISTS customer (
    c_customer_sk bigint not null,
    c_customer_id char(16) not null,
    c_current_cdemo_sk bigint,
    c_current_hdemo_sk bigint,
    c_current_addr_sk bigint,
    c_first_shipto_date_sk bigint,
    c_first_sales_date_sk bigint,
    c_salutation char(10),
    c_first_name char(20),
    c_last_name char(30),
    c_preferred_cust_flag char(1),
    c_birth_day integer,
    c_birth_month integer,
    c_birth_year integer,
    c_birth_country varchar(20),
    c_login char(13),
    c_email_address char(50),
    c_last_review_date_sk bigint
)
DUPLICATE KEY(c_customer_sk)
DISTRIBUTED BY HASH(c_customer_id) BUCKETS 12
PROPERTIES (
  "replication_num" = "1"
);
CREATE TABLE IF NOT EXISTS dbgen_version
(
    dv_version                varchar(16)                   ,
    dv_create_date            date                          ,
    dv_create_time            datetime                      ,
    dv_cmdline_args           varchar(200)                  
)
DUPLICATE KEY(dv_version)
DISTRIBUTED BY HASH(dv_version) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);
