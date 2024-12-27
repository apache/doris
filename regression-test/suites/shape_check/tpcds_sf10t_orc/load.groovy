// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("load") {
    String database = context.config.getDbNameByFile(context.file)
    sql "drop database if exists ${database}"
    sql "create database ${database}"
    sql "use ${database}"
    
    sql """
drop table if exists customer_demographics;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists reason;
CREATE TABLE IF NOT EXISTS reason (
    r_reason_sk bigint not null,
    r_reason_id char(16) not null,
    r_reason_desc char(100)
 )
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists date_dim;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists warehouse;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists catalog_sales;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists call_center;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);

drop table if exists inventory;
CREATE TABLE IF NOT EXISTS inventory (
    inv_date_sk bigint not null,
    inv_item_sk bigint not null,
    inv_warehouse_sk bigint,
    inv_quantity_on_hand integer
)
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists catalog_returns;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists household_demographics;
CREATE TABLE IF NOT EXISTS household_demographics (
    hd_demo_sk bigint not null,
    hd_income_band_sk bigint,
    hd_buy_potential char(15),
    hd_dep_count integer,
    hd_vehicle_count integer
)
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists customer_address;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists income_band;
CREATE TABLE IF NOT EXISTS income_band (
    ib_income_band_sk bigint not null,
    ib_lower_bound integer,
    ib_upper_bound integer
)
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists catalog_page;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists item;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists web_returns;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists web_site;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists promotion;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists web_sales;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists store;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists time_dim;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists web_page;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists store_returns;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists store_sales;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists ship_mode;
CREATE TABLE IF NOT EXISTS ship_mode (
    sm_ship_mode_sk bigint not null,
    sm_ship_mode_id char(16) not null,
    sm_type char(30),
    sm_code char(10),
    sm_carrier char(20),
    sm_contract char(20)
)
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists customer;
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
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);
drop table if exists dbgen_version;
CREATE TABLE IF NOT EXISTS dbgen_version
(
    dv_version                varchar(16)                   ,
    dv_create_date            date                        ,
    dv_create_time            datetime                      ,
    dv_cmdline_args           varchar(200)                  
)
DISTRIBUTED BY RANDOM
PROPERTIES (
  "replication_num" = "1"
);

    """

    sql """
    
alter table catalog_returns modify column cr_reason_sk set stats ('row_count'='1440033112', 'ndv'='55', 'min_value'='1', 'max_value'='55', 'avg_size'='115234992', 'max_size'='115234992' );
alter table catalog_sales modify column cs_wholesale_cost set stats ('row_count'='14399964710', 'ndv'='100', 'min_value'='1.00', 'max_value'='100.00', 'avg_size'='575988260', 'max_size'='575988260' );
alter table catalog_page modify column cp_catalog_page_sk set stats ('row_count'='40000', 'ndv'='20554', 'min_value'='1', 'max_value'='20400', 'avg_size'='163200', 'max_size'='163200' );
alter table call_center modify column cc_sq_ft set stats ('row_count'='54', 'ndv'='22', 'min_value'='1670015', 'max_value'='31896816', 'avg_size'='120', 'max_size'='120' );
alter table customer_address modify column ca_country set stats ('row_count'='32500000', 'ndv'='2', 'min_value'='', 'max_value'='United States', 'avg_size'='12608739', 'max_size'='12608739' );
alter table income_band modify column ib_income_band_sk set stats ('row_count'='20', 'ndv'='20', 'min_value'='1', 'max_value'='20', 'avg_size'='160', 'max_size'='160' );
alter table inventory modify column inv_item_sk set stats ('row_count'='1311525000', 'ndv'='205012', 'min_value'='1', 'max_value'='204000', 'avg_size'='3194640000', 'max_size'='3194640000' );
alter table promotion modify column p_promo_sk set stats ('row_count'='2000', 'ndv'='986', 'min_value'='1', 'max_value'='1000', 'avg_size'='8000', 'max_size'='8000' );
alter table reason modify column r_reason_id set stats ('row_count'='70', 'ndv'='55', 'min_value'='AAAAAAAAABAAAAAA', 'max_value'='AAAAAAAAPCAAAAAA', 'avg_size'='880', 'max_size'='880' );
alter table ship_mode modify column sm_code set stats ('row_count'='20', 'ndv'='4', 'min_value'='AIR', 'max_value'='SURFACE', 'avg_size'='87', 'max_size'='87' );
alter table time_dim modify column t_am_pm set stats ('row_count'='86400', 'ndv'='2', 'min_value'='AM', 'max_value'='PM', 'avg_size'='172800', 'max_size'='172800' );
alter table store_sales modify column ss_ticket_number set stats ('row_count'='28800002049', 'ndv'='23905324', 'min_value'='1', 'max_value'='24000000', 'avg_size'='2303976192', 'max_size'='2303976192' );
alter table web_site modify column web_close_date_sk set stats ('row_count'='78', 'ndv'='8', 'min_value'='2443328', 'max_value'='2447131', 'avg_size'='192', 'max_size'='192' );

alter table web_sales modify column ws_web_site_sk set stats ('row_count'='7199963324', 'ndv'='24', 'min_value'='1', 'max_value'='24', 'avg_size'='576009896', 'max_size'='576009896' );
alter table web_returns modify column wr_item_sk set stats ('row_count'='720020485', 'ndv'='205012', 'min_value'='1', 'max_value'='204000', 'avg_size'='57581360', 'max_size'='57581360' );
alter table customer modify column c_birth_country set stats ('row_count'='65000000', 'ndv'='211', 'min_value'='', 'max_value'='ZIMBABWE', 'avg_size'='16787900', 'max_size'='16787900' );
alter table web_page modify column wp_rec_start_date set stats ('row_count'='4002', 'ndv'='4', 'min_value'='1997-09-03', 'max_value'='2001-09-03', 'avg_size'='8160', 'max_size'='8160' );
alter table store_returns modify column sr_store_credit set stats ('row_count'='2879991693', 'ndv'='9907', 'min_value'='0.00', 'max_value'='15642.11', 'avg_size'='115180320', 'max_size'='115180320' );
alter table warehouse modify column w_county set stats ('row_count'='25', 'ndv'='8', 'min_value'='Barrow County', 'max_value'='Ziebach County', 'avg_size'='207', 'max_size'='207' );
alter table customer_demographics modify column cd_gender set stats ('row_count'='1920800', 'ndv'='2', 'min_value'='F', 'max_value'='M', 'avg_size'='1920800', 'max_size'='1920800' );
alter table item modify column i_size set stats ('row_count'='402000', 'ndv'='8', 'min_value'='', 'max_value'='small', 'avg_size'='880961', 'max_size'='880961' );
alter table date_dim modify column d_week_seq set stats ('row_count'='73049', 'ndv'='10448', 'min_value'='1', 'max_value'='10436', 'avg_size'='292196', 'max_size'='292196' );
alter table store modify column s_country set stats ('row_count'='1500', 'ndv'='2', 'min_value'='', 'max_value'='United States', 'avg_size'='5174', 'max_size'='5174' );
alter table household_demographics modify column hd_income_band_sk set stats ('row_count'='7200', 'ndv'='20', 'min_value'='1', 'max_value'='20', 'avg_size'='57600', 'max_size'='57600' );
"""
}
