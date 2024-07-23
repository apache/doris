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
    if (isCloudMode()) {
        return
    }
    String database = context.config.getDbNameByFile(context.file)
    sql "drop database if exists ${database}"
    sql "create database ${database}"
    sql "use ${database}"
    
    sql '''
    drop table if exists customer_demographics
    '''

    sql '''
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
    DISTRIBUTED BY HASH(cd_gender) BUCKETS 12
    PROPERTIES (
    "replication_num" = "1"
    )
    '''

    sql '''
    drop table if exists reason
    '''

    sql '''
    CREATE TABLE IF NOT EXISTS reason (
        r_reason_sk bigint not null,
        r_reason_id char(16) not null,
        r_reason_desc char(100)
    )
    DUPLICATE KEY(r_reason_sk)
    DISTRIBUTED BY HASH(r_reason_sk) BUCKETS 1
    PROPERTIES (
    "replication_num" = "1"
    )
    '''

    sql '''
    drop table if exists date_dim
    '''

    sql '''
    CREATE TABLE IF NOT EXISTS date_dim (
        d_date_sk bigint not null,
        d_date_id char(16) not null,
        d_date datev2,
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
    PARTITION BY RANGE(d_date_sk)
    (
    PARTITION `ppast` values less than("2450815"),
    PARTITION `p1998` values less than("2451180"),
    PARTITION `p1999` values less than("2451545"),
    PARTITION `p2000` values less than("2451911"),
    PARTITION `p2001` values less than("2452276"),
    PARTITION `p2002` values less than("2452641"),
    PARTITION `p2003` values less than("2453006"),
    PARTITION `pfuture` values less than("9999999")
    )
    DISTRIBUTED BY HASH(d_date_sk) BUCKETS 12
    PROPERTIES (
    "replication_num" = "1"
    )
    '''

    sql '''
    drop table if exists warehouse
    '''

    sql '''
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
        w_gmt_offset decimalv3(5,2)
    )
    DUPLICATE KEY(w_warehouse_sk)
    DISTRIBUTED BY HASH(w_warehouse_sk) BUCKETS 1
    PROPERTIES (
    "replication_num" = "1"
    )
    '''

    sql '''
    drop table if exists catalog_sales
    '''

    sql '''
    CREATE TABLE IF NOT EXISTS catalog_sales (
        cs_sold_date_sk bigint,
        cs_item_sk bigint not null,
        cs_order_number bigint not null,
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
        cs_wholesale_cost decimalv3(7,2),
        cs_list_price decimalv3(7,2),
        cs_sales_price decimalv3(7,2),
        cs_ext_discount_amt decimalv3(7,2),
        cs_ext_sales_price decimalv3(7,2),
        cs_ext_wholesale_cost decimalv3(7,2),
        cs_ext_list_price decimalv3(7,2),
        cs_ext_tax decimalv3(7,2),
        cs_coupon_amt decimalv3(7,2),
        cs_ext_ship_cost decimalv3(7,2),
        cs_net_paid decimalv3(7,2),
        cs_net_paid_inc_tax decimalv3(7,2),
        cs_net_paid_inc_ship decimalv3(7,2),
        cs_net_paid_inc_ship_tax decimalv3(7,2),
        cs_net_profit decimalv3(7,2)
    )
    DUPLICATE KEY(cs_sold_date_sk, cs_item_sk)
    DISTRIBUTED BY HASH(cs_item_sk, cs_order_number) BUCKETS 32
    PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "catalog"
    )
    '''

    sql '''
    drop table if exists call_center
    '''

    sql '''
    CREATE TABLE IF NOT EXISTS call_center (
    cc_call_center_sk bigint not null,
    cc_call_center_id char(16) not null,
    cc_rec_start_date datev2,
    cc_rec_end_date datev2,
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
    cc_gmt_offset decimalv3(5,2),
    cc_tax_percentage decimalv3(5,2)
    )
    DUPLICATE KEY(cc_call_center_sk)
    DISTRIBUTED BY HASH(cc_call_center_sk) BUCKETS 1
    PROPERTIES (
    "replication_num" = "1"
    )
    '''

    sql '''
    drop table if exists inventory
    '''

    sql '''
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
    )
    '''

    sql '''
    drop table if exists catalog_returns
    '''

    sql '''
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
    cr_return_amount decimalv3(7,2),
    cr_return_tax decimalv3(7,2),
    cr_return_amt_inc_tax decimalv3(7,2),
    cr_fee decimalv3(7,2),
    cr_return_ship_cost decimalv3(7,2),
    cr_refunded_cash decimalv3(7,2),
    cr_reversed_charge decimalv3(7,2),
    cr_store_credit decimalv3(7,2),
    cr_net_loss decimalv3(7,2)
    )
    DUPLICATE KEY(cr_item_sk, cr_order_number)
    DISTRIBUTED BY HASH(cr_item_sk, cr_order_number) BUCKETS 32
    PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "catalog"
    )
    '''

    sql '''
    drop table if exists household_demographics
    '''

    sql '''
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
    )
    '''

    sql '''
    drop table if exists customer_address
    '''

    sql '''
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
        ca_gmt_offset decimalv3(5,2),
        ca_location_type char(20)
    )
    DUPLICATE KEY(ca_address_sk)
    DISTRIBUTED BY HASH(ca_address_sk) BUCKETS 12
    PROPERTIES (
    "replication_num" = "1"
    )
    '''

    sql '''
    drop table if exists income_band
    '''

    sql '''
    CREATE TABLE IF NOT EXISTS income_band (
        ib_income_band_sk bigint not null,
        ib_lower_bound integer,
        ib_upper_bound integer
    )
    DUPLICATE KEY(ib_income_band_sk)
    DISTRIBUTED BY HASH(ib_income_band_sk) BUCKETS 1
    PROPERTIES (
    "replication_num" = "1"
    )
    '''

    sql '''
    drop table if exists catalog_page
    '''

    sql '''
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
    )
    '''

    sql '''
    drop table if exists item
    '''

    sql '''
    CREATE TABLE IF NOT EXISTS item (
        i_item_sk bigint not null,
        i_item_id char(16) not null,
        i_rec_start_date datev2,
        i_rec_end_date datev2,
        i_item_desc varchar(200),
        i_current_price decimalv3(7,2),
        i_wholesale_cost decimalv3(7,2),
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
    )
    '''

    sql '''
    drop table if exists web_returns
    '''

    sql '''
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
        wr_return_amt decimalv3(7,2),
        wr_return_tax decimalv3(7,2),
        wr_return_amt_inc_tax decimalv3(7,2),
        wr_fee decimalv3(7,2),
        wr_return_ship_cost decimalv3(7,2),
        wr_refunded_cash decimalv3(7,2),
        wr_reversed_charge decimalv3(7,2),
        wr_account_credit decimalv3(7,2),
        wr_net_loss decimalv3(7,2)
    )
    DUPLICATE KEY(wr_item_sk, wr_order_number)
    DISTRIBUTED BY HASH(wr_item_sk, wr_order_number) BUCKETS 32
    PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "web"
    )
    '''

    sql '''
    drop table if exists web_site
    '''

    sql '''
    CREATE TABLE IF NOT EXISTS web_site (
        web_site_sk bigint not null,
        web_site_id char(16) not null,
        web_rec_start_date datev2,
        web_rec_end_date datev2,
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
        web_gmt_offset decimalv3(5,2),
        web_tax_percentage decimalv3(5,2)
    )
    DUPLICATE KEY(web_site_sk)
    DISTRIBUTED BY HASH(web_site_sk) BUCKETS 1
    PROPERTIES (
    "replication_num" = "1"
    )
    '''

    sql '''
    drop table if exists promotion
    '''

    sql '''
    CREATE TABLE IF NOT EXISTS promotion (
        p_promo_sk bigint not null,
        p_promo_id char(16) not null,
        p_start_date_sk bigint,
        p_end_date_sk bigint,
        p_item_sk bigint,
        p_cost decimalv3(15,2),
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
    )
    '''

    sql '''
    drop table if exists web_sales
    '''

    sql '''
    CREATE TABLE IF NOT EXISTS web_sales (
        ws_sold_date_sk bigint,
        ws_item_sk bigint not null,
        ws_order_number bigint not null,
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
        ws_wholesale_cost decimalv3(7,2),
        ws_list_price decimalv3(7,2),
        ws_sales_price decimalv3(7,2),
        ws_ext_discount_amt decimalv3(7,2),
        ws_ext_sales_price decimalv3(7,2),
        ws_ext_wholesale_cost decimalv3(7,2),
        ws_ext_list_price decimalv3(7,2),
        ws_ext_tax decimalv3(7,2),
        ws_coupon_amt decimalv3(7,2),
        ws_ext_ship_cost decimalv3(7,2),
        ws_net_paid decimalv3(7,2),
        ws_net_paid_inc_tax decimalv3(7,2),
        ws_net_paid_inc_ship decimalv3(7,2),
        ws_net_paid_inc_ship_tax decimalv3(7,2),
        ws_net_profit decimalv3(7,2)
    )
    DUPLICATE KEY(ws_sold_date_sk, ws_item_sk)
    DISTRIBUTED BY HASH(ws_item_sk, ws_order_number) BUCKETS 32
    PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "web"
    )
    '''

    sql '''
    drop table if exists store
    '''

    sql '''
    CREATE TABLE IF NOT EXISTS store (
        s_store_sk bigint not null,
        s_store_id char(16) not null,
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
    DUPLICATE KEY(s_store_sk)
    DISTRIBUTED BY HASH(s_store_sk) BUCKETS 1
    PROPERTIES (
    "replication_num" = "1"
    )
    '''

    sql '''
    drop table if exists time_dim
    '''

    sql '''
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
    )
    '''

    sql '''
    drop table if exists web_page
    '''

    sql '''
    CREATE TABLE IF NOT EXISTS web_page (
            wp_web_page_sk bigint not null,
            wp_web_page_id char(16) not null,
            wp_rec_start_date datev2,
            wp_rec_end_date datev2,
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
    )
    '''

    sql '''
    drop table if exists store_returns
    '''

    sql '''
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
        sr_return_amt decimalv3(7,2),
        sr_return_tax decimalv3(7,2),
        sr_return_amt_inc_tax decimalv3(7,2),
        sr_fee decimalv3(7,2),
        sr_return_ship_cost decimalv3(7,2),
        sr_refunded_cash decimalv3(7,2),
        sr_reversed_charge decimalv3(7,2),
        sr_store_credit decimalv3(7,2),
        sr_net_loss decimalv3(7,2)
    )
    duplicate key(sr_item_sk, sr_ticket_number)
    distributed by hash (sr_item_sk, sr_ticket_number) buckets 32
    properties (
    "replication_num" = "1",
    "colocate_with" = "store"
    )
    '''

    sql '''
    drop table if exists store_sales
    '''

    sql '''
    CREATE TABLE IF NOT EXISTS store_sales (
        ss_sold_date_sk bigint,
        ss_item_sk bigint not null,
        ss_ticket_number bigint not null,
        ss_sold_time_sk bigint,
        ss_customer_sk bigint,
        ss_cdemo_sk bigint,
        ss_hdemo_sk bigint,
        ss_addr_sk bigint,
        ss_store_sk bigint,
        ss_promo_sk bigint,
        ss_quantity integer,
        ss_wholesale_cost decimalv3(7,2),
        ss_list_price decimalv3(7,2),
        ss_sales_price decimalv3(7,2),
        ss_ext_discount_amt decimalv3(7,2),
        ss_ext_sales_price decimalv3(7,2),
        ss_ext_wholesale_cost decimalv3(7,2),
        ss_ext_list_price decimalv3(7,2),
        ss_ext_tax decimalv3(7,2),
        ss_coupon_amt decimalv3(7,2),
        ss_net_paid decimalv3(7,2),
        ss_net_paid_inc_tax decimalv3(7,2),
        ss_net_profit decimalv3(7,2)
    )
    DUPLICATE KEY(ss_sold_date_sk, ss_item_sk)
    DISTRIBUTED BY HASH(ss_item_sk, ss_ticket_number) BUCKETS 32
    PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "store"
    )
    '''

    sql '''
    drop table if exists ship_mode
    '''

    sql '''
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
    )
    '''

    sql '''
    drop table if exists customer
    '''

    sql '''
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
    )
    '''

    sql '''
    drop table if exists dbgen_version
    '''

    sql '''
    CREATE TABLE IF NOT EXISTS dbgen_version
    (
        dv_version                varchar(16)                   ,
        dv_create_date            datev2                        ,
        dv_create_time            datetime                      ,
        dv_cmdline_args           varchar(200)                  
    )
    DUPLICATE KEY(dv_version)
    DISTRIBUTED BY HASH(dv_version) BUCKETS 1
    PROPERTIES (
    "replication_num" = "1"
    )
    '''

sql '''
alter table customer add constraint customer_pk primary key (c_customer_sk);
'''

sql '''
alter table customer add constraint customer_uk unique (c_customer_id);
'''
   
sql '''
alter table store_sales add constraint ss_fk foreign key(ss_customer_sk) references customer(c_customer_sk);
'''

sql '''
alter table web_sales add constraint ws_fk foreign key(ws_bill_customer_sk) references customer(c_customer_sk);
'''

sql '''
alter table catalog_sales add constraint cs_fk foreign key(cs_bill_customer_sk) references customer(c_customer_sk);
'''

sql """
alter table web_sales modify column ws_web_site_sk set stats ('row_count'='72001237', 'ndv'='24', 'min_value'='1', 'max_value'='24', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table web_returns modify column wr_item_sk set stats ('row_count'='7197670', 'ndv'='205012', 'min_value'='1', 'max_value'='204000', 'avg_size'='57581360', 'max_size'='57581360' )
"""

sql """
alter table customer modify column c_birth_country set stats ('row_count'='2000000', 'ndv'='211', 'min_value'='', 'max_value'='ZIMBABWE', 'avg_size'='16787900', 'max_size'='16787900' )
"""

sql """
alter table web_page modify column wp_rec_start_date set stats ('row_count'='2040', 'ndv'='4', 'min_value'='1997-09-03', 'max_value'='2001-09-03', 'avg_size'='8160', 'max_size'='8160' )
"""

sql """
alter table store_returns modify column sr_store_credit set stats ('row_count'='28795080', 'ndv'='9907', 'min_value'='0.00', 'max_value'='15642.11', 'avg_size'='115180320', 'max_size'='115180320' )
"""

sql """
alter table warehouse modify column w_county set stats ('row_count'='15', 'ndv'='8', 'min_value'='Barrow County', 'max_value'='Ziebach County', 'avg_size'='207', 'max_size'='207' )
"""

sql """
alter table customer_demographics modify column cd_gender set stats ('row_count'='1920800', 'ndv'='2', 'min_value'='F', 'max_value'='M', 'avg_size'='1920800', 'max_size'='1920800' )
"""

sql """
alter table web_returns modify column wr_refunded_cdemo_sk set stats ('row_count'='7197670', 'ndv'='1868495', 'min_value'='1', 'max_value'='1920800', 'avg_size'='57581360', 'max_size'='57581360' )
"""

sql """
alter table warehouse modify column w_warehouse_id set stats ('row_count'='15', 'ndv'='15', 'min_value'='AAAAAAAABAAAAAAA', 'max_value'='AAAAAAAAPAAAAAAA', 'avg_size'='240', 'max_size'='240' )
"""

sql """
alter table item modify column i_size set stats ('row_count'='204000', 'ndv'='8', 'min_value'='', 'max_value'='small', 'avg_size'='880961', 'max_size'='880961' )
"""

sql """
alter table web_sales modify column ws_sales_price set stats ('row_count'='72001237', 'ndv'='302', 'min_value'='0.00', 'max_value'='300.00', 'avg_size'='288004948', 'max_size'='288004948' )
"""

sql """
alter table date_dim modify column d_week_seq set stats ('row_count'='73049', 'ndv'='10448', 'min_value'='1', 'max_value'='10436', 'avg_size'='292196', 'max_size'='292196' )
"""

sql """
alter table store modify column s_country set stats ('row_count'='402', 'ndv'='2', 'min_value'='', 'max_value'='United States', 'avg_size'='5174', 'max_size'='5174' )
"""

sql """
alter table household_demographics modify column hd_income_band_sk set stats ('row_count'='7200', 'ndv'='20', 'min_value'='1', 'max_value'='20', 'avg_size'='57600', 'max_size'='57600' )
"""

sql """
alter table web_page modify column wp_creation_date_sk set stats ('row_count'='2040', 'ndv'='134', 'min_value'='2450672', 'max_value'='2450815', 'avg_size'='16320', 'max_size'='16320' )
"""

sql """
alter table catalog_returns modify column cr_reason_sk set stats ('row_count'='14404374', 'ndv'='55', 'min_value'='1', 'max_value'='55', 'avg_size'='115234992', 'max_size'='115234992' )
"""

sql """
alter table web_site modify column web_city set stats ('row_count'='24', 'ndv'='11', 'min_value'='Centerville', 'max_value'='Salem', 'avg_size'='232', 'max_size'='232' )
"""

sql """
alter table item modify column i_class_id set stats ('row_count'='204000', 'ndv'='16', 'min_value'='1', 'max_value'='16', 'avg_size'='816000', 'max_size'='816000' )
"""

sql """
alter table catalog_returns modify column cr_refunded_hdemo_sk set stats ('row_count'='14404374', 'ndv'='7251', 'min_value'='1', 'max_value'='7200', 'avg_size'='115234992', 'max_size'='115234992' )
"""

sql """
alter table web_page modify column wp_customer_sk set stats ('row_count'='2040', 'ndv'='475', 'min_value'='711', 'max_value'='1996257', 'avg_size'='16320', 'max_size'='16320' )
"""

sql """
alter table customer_demographics modify column cd_marital_status set stats ('row_count'='1920800', 'ndv'='5', 'min_value'='D', 'max_value'='W', 'avg_size'='1920800', 'max_size'='1920800' )
"""

sql """
alter table call_center modify column cc_suite_number set stats ('row_count'='30', 'ndv'='14', 'min_value'='Suite 0', 'max_value'='Suite W', 'avg_size'='234', 'max_size'='234' )
"""

sql """
alter table web_page modify column wp_url set stats ('row_count'='2040', 'ndv'='2', 'min_value'='', 'max_value'='http://www.foo.com', 'avg_size'='36270', 'max_size'='36270' )
"""

sql """
alter table web_sales modify column ws_wholesale_cost set stats ('row_count'='72001237', 'ndv'='100', 'min_value'='1.00', 'max_value'='100.00', 'avg_size'='288004948', 'max_size'='288004948' )
"""

sql """
alter table catalog_returns modify column cr_return_quantity set stats ('row_count'='14404374', 'ndv'='100', 'min_value'='1', 'max_value'='100', 'avg_size'='57617496', 'max_size'='57617496' )
"""

sql """
alter table catalog_sales modify column cs_wholesale_cost set stats ('row_count'='143997065', 'ndv'='100', 'min_value'='1.00', 'max_value'='100.00', 'avg_size'='575988260', 'max_size'='575988260' )
"""

sql """
alter table store_sales modify column ss_quantity set stats ('row_count'='287997024', 'ndv'='100', 'min_value'='1', 'max_value'='100', 'avg_size'='1151988096', 'max_size'='1151988096' )
"""

sql """
alter table date_dim modify column d_quarter_seq set stats ('row_count'='73049', 'ndv'='801', 'min_value'='1', 'max_value'='801', 'avg_size'='292196', 'max_size'='292196' )
"""

sql """
alter table date_dim modify column d_current_week set stats ('row_count'='73049', 'ndv'='1', 'min_value'='N', 'max_value'='N', 'avg_size'='73049', 'max_size'='73049' )
"""

sql """
alter table web_returns modify column wr_reason_sk set stats ('row_count'='7197670', 'ndv'='55', 'min_value'='1', 'max_value'='55', 'avg_size'='57581360', 'max_size'='57581360' )
"""

sql """
alter table promotion modify column p_channel_catalog set stats ('row_count'='1000', 'ndv'='2', 'min_value'='', 'max_value'='N', 'avg_size'='986', 'max_size'='986' )
"""

sql """
alter table catalog_sales modify column cs_net_paid_inc_ship_tax set stats ('row_count'='143997065', 'ndv'='38890', 'min_value'='0.00', 'max_value'='45460.80', 'avg_size'='575988260', 'max_size'='575988260' )
"""

sql """
alter table catalog_sales modify column cs_order_number set stats ('row_count'='143997065', 'ndv'='16050730', 'min_value'='1', 'max_value'='16000000', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table time_dim modify column t_am_pm set stats ('row_count'='86400', 'ndv'='2', 'min_value'='AM', 'max_value'='PM', 'avg_size'='172800', 'max_size'='172800' )
"""

sql """
alter table promotion modify column p_promo_name set stats ('row_count'='1000', 'ndv'='11', 'min_value'='', 'max_value'='pri', 'avg_size'='3924', 'max_size'='3924' )
"""

sql """
alter table web_site modify column web_manager set stats ('row_count'='24', 'ndv'='19', 'min_value'='Adam Stonge', 'max_value'='Tommy Jones', 'avg_size'='297', 'max_size'='297' )
"""

sql """
alter table store modify column s_gmt_offset set stats ('row_count'='402', 'ndv'='2', 'min_value'='-6.00', 'max_value'='-5.00', 'avg_size'='1608', 'max_size'='1608' )
"""

sql """
alter table web_sales modify column ws_quantity set stats ('row_count'='72001237', 'ndv'='100', 'min_value'='1', 'max_value'='100', 'avg_size'='288004948', 'max_size'='288004948' )
"""

sql """
alter table date_dim modify column d_weekend set stats ('row_count'='73049', 'ndv'='2', 'min_value'='N', 'max_value'='Y', 'avg_size'='73049', 'max_size'='73049' )
"""

sql """
alter table store modify column s_number_employees set stats ('row_count'='402', 'ndv'='97', 'min_value'='200', 'max_value'='300', 'avg_size'='1608', 'max_size'='1608' )
"""

sql """
alter table call_center modify column cc_mkt_desc set stats ('row_count'='30', 'ndv'='22', 'min_value'='As existing eyebrows miss as the matters. Realistic stories may not face almost by a ', 'max_value'='Young tests could buy comfortable, local users o', 'avg_size'='1766', 'max_size'='1766' )
"""

sql """
alter table web_sales modify column ws_net_paid_inc_ship set stats ('row_count'='72001237', 'ndv'='36553', 'min_value'='0.00', 'max_value'='43468.92', 'avg_size'='288004948', 'max_size'='288004948' )
"""

sql """
alter table item modify column i_item_sk set stats ('row_count'='204000', 'ndv'='205012', 'min_value'='1', 'max_value'='204000', 'avg_size'='1632000', 'max_size'='1632000' )
"""

sql """
alter table web_sales modify column ws_bill_addr_sk set stats ('row_count'='72001237', 'ndv'='998891', 'min_value'='1', 'max_value'='1000000', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table customer modify column c_salutation set stats ('row_count'='2000000', 'ndv'='7', 'min_value'='', 'max_value'='Sir', 'avg_size'='6257882', 'max_size'='6257882' )
"""

sql """
alter table web_sales modify column ws_net_paid set stats ('row_count'='72001237', 'ndv'='26912', 'min_value'='0.00', 'max_value'='29810.00', 'avg_size'='288004948', 'max_size'='288004948' )
"""

sql """
alter table time_dim modify column t_time set stats ('row_count'='86400', 'ndv'='86684', 'min_value'='0', 'max_value'='86399', 'avg_size'='345600', 'max_size'='345600' )
"""

sql """
alter table web_site modify column web_mkt_id set stats ('row_count'='24', 'ndv'='6', 'min_value'='1', 'max_value'='6', 'avg_size'='96', 'max_size'='96' )
"""

sql """
alter table store_returns modify column sr_hdemo_sk set stats ('row_count'='28795080', 'ndv'='7251', 'min_value'='1', 'max_value'='7200', 'avg_size'='230360640', 'max_size'='230360640' )
"""

sql """
alter table catalog_page modify column cp_catalog_page_sk set stats ('row_count'='20400', 'ndv'='20554', 'min_value'='1', 'max_value'='20400', 'avg_size'='163200', 'max_size'='163200' )
"""

sql """
alter table customer_address modify column ca_address_id set stats ('row_count'='1000000', 'ndv'='999950', 'min_value'='AAAAAAAAAAAABAAA', 'max_value'='AAAAAAAAPPPPOAAA', 'avg_size'='16000000', 'max_size'='16000000' )
"""

sql """
alter table date_dim modify column d_year set stats ('row_count'='73049', 'ndv'='202', 'min_value'='1900', 'max_value'='2100', 'avg_size'='292196', 'max_size'='292196' )
"""

sql """
alter table web_returns modify column wr_net_loss set stats ('row_count'='7197670', 'ndv'='11012', 'min_value'='0.50', 'max_value'='15068.96', 'avg_size'='28790680', 'max_size'='28790680' )
"""

sql """
alter table store modify column s_closed_date_sk set stats ('row_count'='402', 'ndv'='69', 'min_value'='2450823', 'max_value'='2451313', 'avg_size'='3216', 'max_size'='3216' )
"""

sql """
alter table customer_address modify column ca_city set stats ('row_count'='1000000', 'ndv'='977', 'min_value'='', 'max_value'='Zion', 'avg_size'='8681993', 'max_size'='8681993' )
"""

sql """
alter table customer modify column c_customer_id set stats ('row_count'='2000000', 'ndv'='1994557', 'min_value'='AAAAAAAAAAAAABAA', 'max_value'='AAAAAAAAPPPPPAAA', 'avg_size'='32000000', 'max_size'='32000000' )
"""

sql """
alter table web_page modify column wp_access_date_sk set stats ('row_count'='2040', 'ndv'='101', 'min_value'='2452548', 'max_value'='2452648', 'avg_size'='16320', 'max_size'='16320' )
"""

sql """
alter table warehouse modify column w_gmt_offset set stats ('row_count'='15', 'ndv'='2', 'min_value'='-6.00', 'max_value'='-5.00', 'avg_size'='60', 'max_size'='60' )
"""

sql """
alter table warehouse modify column w_street_number set stats ('row_count'='15', 'ndv'='15', 'min_value'='', 'max_value'='957', 'avg_size'='40', 'max_size'='40' )
"""

sql """
alter table store_sales modify column ss_ticket_number set stats ('row_count'='287997024', 'ndv'='23905324', 'min_value'='1', 'max_value'='24000000', 'avg_size'='2303976192', 'max_size'='2303976192' )
"""

sql """
alter table catalog_returns modify column cr_fee set stats ('row_count'='14404374', 'ndv'='101', 'min_value'='0.50', 'max_value'='100.00', 'avg_size'='57617496', 'max_size'='57617496' )
"""

sql """
alter table date_dim modify column d_current_quarter set stats ('row_count'='73049', 'ndv'='2', 'min_value'='N', 'max_value'='Y', 'avg_size'='73049', 'max_size'='73049' )
"""

sql """
alter table store modify column s_store_name set stats ('row_count'='402', 'ndv'='11', 'min_value'='', 'max_value'='pri', 'avg_size'='1575', 'max_size'='1575' )
"""

sql """
alter table catalog_sales modify column cs_ext_wholesale_cost set stats ('row_count'='143997065', 'ndv'='10009', 'min_value'='1.00', 'max_value'='10000.00', 'avg_size'='575988260', 'max_size'='575988260' )
"""

sql """
alter table reason modify column r_reason_desc set stats ('row_count'='55', 'ndv'='54', 'min_value'='Did not fit', 'max_value'='unauthoized purchase', 'avg_size'='758', 'max_size'='758' )
"""

sql """
alter table date_dim modify column d_same_day_ly set stats ('row_count'='73049', 'ndv'='72450', 'min_value'='2414657', 'max_value'='2487705', 'avg_size'='292196', 'max_size'='292196' )
"""

sql """
alter table web_site modify column web_gmt_offset set stats ('row_count'='24', 'ndv'='2', 'min_value'='-6.00', 'max_value'='-5.00', 'avg_size'='96', 'max_size'='96' )
"""

sql """
alter table time_dim modify column t_sub_shift set stats ('row_count'='86400', 'ndv'='4', 'min_value'='afternoon', 'max_value'='night', 'avg_size'='597600', 'max_size'='597600' )
"""

sql """
alter table web_sales modify column ws_ship_customer_sk set stats ('row_count'='72001237', 'ndv'='1898561', 'min_value'='1', 'max_value'='2000000', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table web_site modify column web_close_date_sk set stats ('row_count'='24', 'ndv'='8', 'min_value'='2443328', 'max_value'='2447131', 'avg_size'='192', 'max_size'='192' )
"""

sql """
alter table call_center modify column cc_market_manager set stats ('row_count'='30', 'ndv'='24', 'min_value'='Charles Corbett', 'max_value'='Tom Root', 'avg_size'='373', 'max_size'='373' )
"""

sql """
alter table store modify column s_market_desc set stats ('row_count'='402', 'ndv'='311', 'min_value'='', 'max_value'='Years get acute years. Right likely players mus', 'avg_size'='23261', 'max_size'='23261' )
"""

sql """
alter table call_center modify column cc_sq_ft set stats ('row_count'='30', 'ndv'='22', 'min_value'='1670015', 'max_value'='31896816', 'avg_size'='120', 'max_size'='120' )
"""

sql """
alter table customer_address modify column ca_country set stats ('row_count'='1000000', 'ndv'='2', 'min_value'='', 'max_value'='United States', 'avg_size'='12608739', 'max_size'='12608739' )
"""

sql """
alter table promotion modify column p_promo_id set stats ('row_count'='1000', 'ndv'='1004', 'min_value'='AAAAAAAAAABAAAAA', 'max_value'='AAAAAAAAPPCAAAAA', 'avg_size'='16000', 'max_size'='16000' )
"""

sql """
alter table customer modify column c_preferred_cust_flag set stats ('row_count'='2000000', 'ndv'='3', 'min_value'='', 'max_value'='Y', 'avg_size'='1930222', 'max_size'='1930222' )
"""

sql """
alter table catalog_page modify column cp_catalog_page_id set stats ('row_count'='20400', 'ndv'='20341', 'min_value'='AAAAAAAAAAABAAAA', 'max_value'='AAAAAAAAPPPDAAAA', 'avg_size'='326400', 'max_size'='326400' )
"""

sql """
alter table household_demographics modify column hd_dep_count set stats ('row_count'='7200', 'ndv'='10', 'min_value'='0', 'max_value'='9', 'avg_size'='28800', 'max_size'='28800' )
"""

sql """
alter table store_sales modify column ss_ext_wholesale_cost set stats ('row_count'='287997024', 'ndv'='10009', 'min_value'='1.00', 'max_value'='10000.00', 'avg_size'='1151988096', 'max_size'='1151988096' )
"""

sql """
alter table promotion modify column p_end_date_sk set stats ('row_count'='1000', 'ndv'='571', 'min_value'='2450116', 'max_value'='2450967', 'avg_size'='8000', 'max_size'='8000' )
"""

sql """
alter table catalog_sales modify column cs_sold_date_sk set stats ('row_count'='143997065', 'ndv'='1835', 'min_value'='2450815', 'max_value'='2452654', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table web_returns modify column wr_return_quantity set stats ('row_count'='7197670', 'ndv'='100', 'min_value'='1', 'max_value'='100', 'avg_size'='28790680', 'max_size'='28790680' )
"""

sql """
alter table store_returns modify column sr_return_amt set stats ('row_count'='28795080', 'ndv'='15493', 'min_value'='0.00', 'max_value'='18973.20', 'avg_size'='115180320', 'max_size'='115180320' )
"""

sql """
alter table web_site modify column web_rec_start_date set stats ('row_count'='24', 'ndv'='4', 'min_value'='1997-08-16', 'max_value'='2001-08-16', 'avg_size'='96', 'max_size'='96' )
"""

sql """
alter table store_sales modify column ss_coupon_amt set stats ('row_count'='287997024', 'ndv'='16198', 'min_value'='0.00', 'max_value'='19225.00', 'avg_size'='1151988096', 'max_size'='1151988096' )
"""

sql """
alter table call_center modify column cc_company set stats ('row_count'='30', 'ndv'='6', 'min_value'='1', 'max_value'='6', 'avg_size'='120', 'max_size'='120' )
"""

sql """
alter table warehouse modify column w_state set stats ('row_count'='15', 'ndv'='8', 'min_value'='AL', 'max_value'='SD', 'avg_size'='30', 'max_size'='30' )
"""

sql """
alter table catalog_returns modify column cr_warehouse_sk set stats ('row_count'='14404374', 'ndv'='15', 'min_value'='1', 'max_value'='15', 'avg_size'='115234992', 'max_size'='115234992' )
"""

sql """
alter table catalog_returns modify column cr_returning_customer_sk set stats ('row_count'='14404374', 'ndv'='1991754', 'min_value'='1', 'max_value'='2000000', 'avg_size'='115234992', 'max_size'='115234992' )
"""

sql """
alter table customer_address modify column ca_state set stats ('row_count'='1000000', 'ndv'='52', 'min_value'='', 'max_value'='WY', 'avg_size'='1939752', 'max_size'='1939752' )
"""

sql """
alter table customer modify column c_customer_sk set stats ('row_count'='2000000', 'ndv'='1994393', 'min_value'='1', 'max_value'='2000000', 'avg_size'='16000000', 'max_size'='16000000' )
"""

sql """
alter table store_sales modify column ss_item_sk set stats ('row_count'='287997024', 'ndv'='205012', 'min_value'='1', 'max_value'='204000', 'avg_size'='2303976192', 'max_size'='2303976192' )
"""

sql """
alter table catalog_sales modify column cs_ship_customer_sk set stats ('row_count'='143997065', 'ndv'='1993190', 'min_value'='1', 'max_value'='2000000', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table web_returns modify column wr_refunded_cash set stats ('row_count'='7197670', 'ndv'='14621', 'min_value'='0.00', 'max_value'='26466.56', 'avg_size'='28790680', 'max_size'='28790680' )
"""

sql """
alter table customer modify column c_birth_day set stats ('row_count'='2000000', 'ndv'='31', 'min_value'='1', 'max_value'='31', 'avg_size'='8000000', 'max_size'='8000000' )
"""

sql """
alter table income_band modify column ib_income_band_sk set stats ('row_count'='20', 'ndv'='20', 'min_value'='1', 'max_value'='20', 'avg_size'='160', 'max_size'='160' )
"""

sql """
alter table web_returns modify column wr_fee set stats ('row_count'='7197670', 'ndv'='101', 'min_value'='0.50', 'max_value'='100.00', 'avg_size'='28790680', 'max_size'='28790680' )
"""

sql """
alter table item modify column i_class set stats ('row_count'='204000', 'ndv'='100', 'min_value'='', 'max_value'='womens watch', 'avg_size'='1585937', 'max_size'='1585937' )
"""

sql """
alter table customer modify column c_last_review_date_sk set stats ('row_count'='2000000', 'ndv'='366', 'min_value'='2452283', 'max_value'='2452648', 'avg_size'='16000000', 'max_size'='16000000' )
"""

sql """
alter table web_site modify column web_rec_end_date set stats ('row_count'='24', 'ndv'='3', 'min_value'='1999-08-16', 'max_value'='2001-08-15', 'avg_size'='96', 'max_size'='96' )
"""

sql """
alter table catalog_returns modify column cr_reversed_charge set stats ('row_count'='14404374', 'ndv'='12359', 'min_value'='0.00', 'max_value'='23801.24', 'avg_size'='57617496', 'max_size'='57617496' )
"""

sql """
alter table customer_address modify column ca_location_type set stats ('row_count'='1000000', 'ndv'='4', 'min_value'='', 'max_value'='single family', 'avg_size'='8728128', 'max_size'='8728128' )
"""

sql """
alter table warehouse modify column w_street_type set stats ('row_count'='15', 'ndv'='11', 'min_value'='', 'max_value'='Wy', 'avg_size'='58', 'max_size'='58' )
"""

sql """
alter table web_returns modify column wr_refunded_hdemo_sk set stats ('row_count'='7197670', 'ndv'='7251', 'min_value'='1', 'max_value'='7200', 'avg_size'='57581360', 'max_size'='57581360' )
"""

sql """
alter table call_center modify column cc_manager set stats ('row_count'='30', 'ndv'='22', 'min_value'='Alden Snyder', 'max_value'='Wayne Ray', 'avg_size'='368', 'max_size'='368' )
"""

sql """
alter table web_site modify column web_open_date_sk set stats ('row_count'='24', 'ndv'='12', 'min_value'='2450628', 'max_value'='2450807', 'avg_size'='192', 'max_size'='192' )
"""

sql """
alter table dbgen_version modify column dv_version set stats ('row_count'='1', 'ndv'='1', 'min_value'='3.2.0', 'max_value'='3.2.0', 'avg_size'='5', 'max_size'='5' )
"""

sql """
alter table catalog_sales modify column cs_sales_price set stats ('row_count'='143997065', 'ndv'='302', 'min_value'='0.00', 'max_value'='300.00', 'avg_size'='575988260', 'max_size'='575988260' )
"""

sql """
alter table catalog_page modify column cp_catalog_number set stats ('row_count'='20400', 'ndv'='109', 'min_value'='1', 'max_value'='109', 'avg_size'='81600', 'max_size'='81600' )
"""

sql """
alter table promotion modify column p_channel_press set stats ('row_count'='1000', 'ndv'='2', 'min_value'='', 'max_value'='N', 'avg_size'='985', 'max_size'='985' )
"""

sql """
alter table web_sales modify column ws_ship_addr_sk set stats ('row_count'='72001237', 'ndv'='997336', 'min_value'='1', 'max_value'='1000000', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table catalog_returns modify column cr_refunded_cash set stats ('row_count'='14404374', 'ndv'='16271', 'min_value'='0.00', 'max_value'='24544.84', 'avg_size'='57617496', 'max_size'='57617496' )
"""

sql """
alter table call_center modify column cc_mkt_class set stats ('row_count'='30', 'ndv'='25', 'min_value'='A bit narrow forms matter animals. Consist', 'max_value'='Yesterday new men can make moreov', 'avg_size'='1033', 'max_size'='1033' )
"""

sql """
alter table catalog_returns modify column cr_returned_date_sk set stats ('row_count'='14404374', 'ndv'='2105', 'min_value'='2450821', 'max_value'='2452921', 'avg_size'='115234992', 'max_size'='115234992' )
"""

sql """
alter table web_page modify column wp_max_ad_count set stats ('row_count'='2040', 'ndv'='5', 'min_value'='0', 'max_value'='4', 'avg_size'='8160', 'max_size'='8160' )
"""

sql """
alter table call_center modify column cc_closed_date_sk set stats ('row_count'='30', 'ndv'='0', 'min_value'='2415022', 'max_value'='2488070', 'avg_size'='120', 'max_size'='120' )
"""

sql """
alter table web_returns modify column wr_return_ship_cost set stats ('row_count'='7197670', 'ndv'='10429', 'min_value'='0.00', 'max_value'='13602.60', 'avg_size'='28790680', 'max_size'='28790680' )
"""

sql """
alter table warehouse modify column w_warehouse_name set stats ('row_count'='15', 'ndv'='15', 'min_value'='', 'max_value'='Rooms cook ', 'avg_size'='230', 'max_size'='230' )
"""

sql """
alter table web_page modify column wp_type set stats ('row_count'='2040', 'ndv'='8', 'min_value'='', 'max_value'='welcome', 'avg_size'='12856', 'max_size'='12856' )
"""

sql """
alter table store modify column s_division_name set stats ('row_count'='402', 'ndv'='2', 'min_value'='', 'max_value'='Unknown', 'avg_size'='2779', 'max_size'='2779' )
"""

sql """
alter table date_dim modify column d_dom set stats ('row_count'='73049', 'ndv'='31', 'min_value'='1', 'max_value'='31', 'avg_size'='292196', 'max_size'='292196' )
"""

sql """
alter table date_dim modify column d_fy_week_seq set stats ('row_count'='73049', 'ndv'='10448', 'min_value'='1', 'max_value'='10436', 'avg_size'='292196', 'max_size'='292196' )
"""

sql """
alter table web_returns modify column wr_return_tax set stats ('row_count'='7197670', 'ndv'='1820', 'min_value'='0.00', 'max_value'='2551.16', 'avg_size'='28790680', 'max_size'='28790680' )
"""

sql """
alter table catalog_sales modify column cs_ship_addr_sk set stats ('row_count'='143997065', 'ndv'='1000237', 'min_value'='1', 'max_value'='1000000', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table store modify column s_street_name set stats ('row_count'='402', 'ndv'='256', 'min_value'='', 'max_value'='Woodland ', 'avg_size'='3384', 'max_size'='3384' )
"""

sql """
alter table store_sales modify column ss_hdemo_sk set stats ('row_count'='287997024', 'ndv'='7251', 'min_value'='1', 'max_value'='7200', 'avg_size'='2303976192', 'max_size'='2303976192' )
"""

sql """
alter table web_sales modify column ws_web_page_sk set stats ('row_count'='72001237', 'ndv'='2032', 'min_value'='1', 'max_value'='2040', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table warehouse modify column w_warehouse_sq_ft set stats ('row_count'='15', 'ndv'='14', 'min_value'='73065', 'max_value'='977787', 'avg_size'='60', 'max_size'='60' )
"""

sql """
alter table ship_mode modify column sm_type set stats ('row_count'='20', 'ndv'='6', 'min_value'='EXPRESS', 'max_value'='TWO DAY', 'avg_size'='150', 'max_size'='150' )
"""

sql """
alter table date_dim modify column d_fy_year set stats ('row_count'='73049', 'ndv'='202', 'min_value'='1900', 'max_value'='2100', 'avg_size'='292196', 'max_size'='292196' )
"""

sql """
alter table catalog_sales modify column cs_catalog_page_sk set stats ('row_count'='143997065', 'ndv'='11515', 'min_value'='1', 'max_value'='17108', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table web_sales modify column ws_warehouse_sk set stats ('row_count'='72001237', 'ndv'='15', 'min_value'='1', 'max_value'='15', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table item modify column i_wholesale_cost set stats ('row_count'='204000', 'ndv'='89', 'min_value'='0.02', 'max_value'='88.91', 'avg_size'='816000', 'max_size'='816000' )
"""

sql """
alter table store_returns modify column sr_return_tax set stats ('row_count'='28795080', 'ndv'='1427', 'min_value'='0.00', 'max_value'='1611.71', 'avg_size'='115180320', 'max_size'='115180320' )
"""

sql """
alter table store_sales modify column ss_net_paid_inc_tax set stats ('row_count'='287997024', 'ndv'='20203', 'min_value'='0.00', 'max_value'='21344.38', 'avg_size'='1151988096', 'max_size'='1151988096' )
"""

sql """
alter table web_site modify column web_mkt_desc set stats ('row_count'='24', 'ndv'='15', 'min_value'='Acres see else children. Mutual too', 'max_value'='Well similar decisions used to keep hardly democratic, personal priorities.', 'avg_size'='1561', 'max_size'='1561' )
"""

sql """
alter table customer modify column c_current_cdemo_sk set stats ('row_count'='2000000', 'ndv'='1221921', 'min_value'='1', 'max_value'='1920798', 'avg_size'='16000000', 'max_size'='16000000' )
"""

sql """
alter table web_returns modify column wr_returning_customer_sk set stats ('row_count'='7197670', 'ndv'='1926139', 'min_value'='1', 'max_value'='2000000', 'avg_size'='57581360', 'max_size'='57581360' )
"""

sql """
alter table store_sales modify column ss_ext_sales_price set stats ('row_count'='287997024', 'ndv'='19105', 'min_value'='0.00', 'max_value'='19878.00', 'avg_size'='1151988096', 'max_size'='1151988096' )
"""

sql """
alter table catalog_sales modify column cs_item_sk set stats ('row_count'='143997065', 'ndv'='205012', 'min_value'='1', 'max_value'='204000', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table store modify column s_store_id set stats ('row_count'='402', 'ndv'='201', 'min_value'='AAAAAAAAAABAAAAA', 'max_value'='AAAAAAAAPNAAAAAA', 'avg_size'='6432', 'max_size'='6432' )
"""

sql """
alter table web_site modify column web_mkt_class set stats ('row_count'='24', 'ndv'='18', 'min_value'='About rural reasons shall no', 'max_value'='Wide, final representat', 'avg_size'='758', 'max_size'='758' )
"""

sql """
alter table customer modify column c_birth_month set stats ('row_count'='2000000', 'ndv'='12', 'min_value'='1', 'max_value'='12', 'avg_size'='8000000', 'max_size'='8000000' )
"""

sql """
alter table date_dim modify column d_last_dom set stats ('row_count'='73049', 'ndv'='2419', 'min_value'='2415020', 'max_value'='2488372', 'avg_size'='292196', 'max_size'='292196' )
"""

sql """
alter table web_sales modify column ws_bill_customer_sk set stats ('row_count'='72001237', 'ndv'='1899439', 'min_value'='1', 'max_value'='2000000', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table web_sales modify column ws_item_sk set stats ('row_count'='72001237', 'ndv'='205012', 'min_value'='1', 'max_value'='204000', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table call_center modify column cc_state set stats ('row_count'='30', 'ndv'='8', 'min_value'='AL', 'max_value'='TN', 'avg_size'='60', 'max_size'='60' )
"""

sql """
alter table promotion modify column p_start_date_sk set stats ('row_count'='1000', 'ndv'='574', 'min_value'='2450100', 'max_value'='2450915', 'avg_size'='8000', 'max_size'='8000' )
"""

sql """
alter table catalog_sales modify column cs_ship_date_sk set stats ('row_count'='143997065', 'ndv'='1933', 'min_value'='2450817', 'max_value'='2452744', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table store_sales modify column ss_sales_price set stats ('row_count'='287997024', 'ndv'='202', 'min_value'='0.00', 'max_value'='200.00', 'avg_size'='1151988096', 'max_size'='1151988096' )
"""

sql """
alter table promotion modify column p_channel_details set stats ('row_count'='1000', 'ndv'='992', 'min_value'='', 'max_value'='Young, valuable companies watch walls. Payments can flour', 'avg_size'='39304', 'max_size'='39304' )
"""

sql """
alter table item modify column i_rec_end_date set stats ('row_count'='204000', 'ndv'='3', 'min_value'='1999-10-27', 'max_value'='2001-10-26', 'avg_size'='816000', 'max_size'='816000' )
"""

sql """
alter table item modify column i_container set stats ('row_count'='204000', 'ndv'='2', 'min_value'='', 'max_value'='Unknown', 'avg_size'='1424430', 'max_size'='1424430' )
"""

sql """
alter table web_site modify column web_tax_percentage set stats ('row_count'='24', 'ndv'='1', 'min_value'='0.00', 'max_value'='0.12', 'avg_size'='96', 'max_size'='96' )
"""

sql """
alter table customer modify column c_email_address set stats ('row_count'='2000000', 'ndv'='1936613', 'min_value'='', 'max_value'='Zulma.Wright@AqokXsju9f2yj.org', 'avg_size'='53014147', 'max_size'='53014147' )
"""

sql """
alter table income_band modify column ib_lower_bound set stats ('row_count'='20', 'ndv'='20', 'min_value'='0', 'max_value'='190001', 'avg_size'='80', 'max_size'='80' )
"""

sql """
alter table web_returns modify column wr_account_credit set stats ('row_count'='7197670', 'ndv'='10868', 'min_value'='0.00', 'max_value'='23028.27', 'avg_size'='28790680', 'max_size'='28790680' )
"""

sql """
alter table web_sales modify column ws_bill_hdemo_sk set stats ('row_count'='72001237', 'ndv'='7251', 'min_value'='1', 'max_value'='7200', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table store_sales modify column ss_store_sk set stats ('row_count'='287997024', 'ndv'='200', 'min_value'='1', 'max_value'='400', 'avg_size'='2303976192', 'max_size'='2303976192' )
"""

sql """
alter table store_returns modify column sr_customer_sk set stats ('row_count'='28795080', 'ndv'='1994323', 'min_value'='1', 'max_value'='2000000', 'avg_size'='230360640', 'max_size'='230360640' )
"""

sql """
alter table call_center modify column cc_class set stats ('row_count'='30', 'ndv'='3', 'min_value'='large', 'max_value'='small', 'avg_size'='166', 'max_size'='166' )
"""

sql """
alter table time_dim modify column t_meal_time set stats ('row_count'='86400', 'ndv'='4', 'min_value'='', 'max_value'='lunch', 'avg_size'='248400', 'max_size'='248400' )
"""

sql """
alter table web_site modify column web_street_number set stats ('row_count'='24', 'ndv'='14', 'min_value'='184', 'max_value'='973', 'avg_size'='70', 'max_size'='70' )
"""

sql """
alter table catalog_sales modify column cs_promo_sk set stats ('row_count'='143997065', 'ndv'='986', 'min_value'='1', 'max_value'='1000', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table customer modify column c_last_name set stats ('row_count'='2000000', 'ndv'='4990', 'min_value'='', 'max_value'='Zuniga', 'avg_size'='11833714', 'max_size'='11833714' )
"""

sql """
alter table promotion modify column p_channel_event set stats ('row_count'='1000', 'ndv'='2', 'min_value'='', 'max_value'='N', 'avg_size'='986', 'max_size'='986' )
"""

sql """
alter table store_returns modify column sr_return_amt_inc_tax set stats ('row_count'='28795080', 'ndv'='16190', 'min_value'='0.00', 'max_value'='20002.89', 'avg_size'='115180320', 'max_size'='115180320' )
"""

sql """
alter table dbgen_version modify column dv_cmdline_args set stats ('row_count'='1', 'ndv'='1', 'min_value'='-SCALE 100 -PARALLEL 10 -CHILD 1 -TERMINATE N -DIR /mnt/datadisk0/doris/tools/tpcds-tools/bin/tpcds-data ', 'max_value'='-SCALE 100 -PARALLEL 10 -CHILD 1 -TERMINATE N -DIR /mnt/datadisk0/doris/tools/tpcds-tools/bin/tpcds-data ', 'avg_size'='105', 'max_size'='105' )
"""

sql """
alter table warehouse modify column w_street_name set stats ('row_count'='15', 'ndv'='15', 'min_value'='', 'max_value'='Wilson Elm', 'avg_size'='128', 'max_size'='128' )
"""

sql """
alter table call_center modify column cc_county set stats ('row_count'='30', 'ndv'='8', 'min_value'='Barrow County', 'max_value'='Ziebach County', 'avg_size'='423', 'max_size'='423' )
"""

sql """
alter table catalog_returns modify column cr_refunded_addr_sk set stats ('row_count'='14404374', 'ndv'='1000237', 'min_value'='1', 'max_value'='1000000', 'avg_size'='115234992', 'max_size'='115234992' )
"""

sql """
alter table catalog_returns modify column cr_returning_cdemo_sk set stats ('row_count'='14404374', 'ndv'='1913762', 'min_value'='1', 'max_value'='1920800', 'avg_size'='115234992', 'max_size'='115234992' )
"""

sql """
alter table web_sales modify column ws_ship_hdemo_sk set stats ('row_count'='72001237', 'ndv'='7251', 'min_value'='1', 'max_value'='7200', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table call_center modify column cc_mkt_id set stats ('row_count'='30', 'ndv'='6', 'min_value'='1', 'max_value'='6', 'avg_size'='120', 'max_size'='120' )
"""

sql """
alter table store modify column s_store_sk set stats ('row_count'='402', 'ndv'='398', 'min_value'='1', 'max_value'='402', 'avg_size'='3216', 'max_size'='3216' )
"""

sql """
alter table customer_demographics modify column cd_dep_employed_count set stats ('row_count'='1920800', 'ndv'='7', 'min_value'='0', 'max_value'='6', 'avg_size'='7683200', 'max_size'='7683200' )
"""

sql """
alter table catalog_sales modify column cs_ext_list_price set stats ('row_count'='143997065', 'ndv'='29336', 'min_value'='1.00', 'max_value'='29997.00', 'avg_size'='575988260', 'max_size'='575988260' )
"""

sql """
alter table web_sales modify column ws_bill_cdemo_sk set stats ('row_count'='72001237', 'ndv'='1835731', 'min_value'='1', 'max_value'='1920800', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table web_returns modify column wr_order_number set stats ('row_count'='7197670', 'ndv'='4249346', 'min_value'='1', 'max_value'='5999999', 'avg_size'='57581360', 'max_size'='57581360' )
"""

sql """
alter table web_site modify column web_country set stats ('row_count'='24', 'ndv'='1', 'min_value'='United States', 'max_value'='United States', 'avg_size'='312', 'max_size'='312' )
"""

sql """
alter table web_sales modify column ws_net_profit set stats ('row_count'='72001237', 'ndv'='27958', 'min_value'='-9997.00', 'max_value'='19840.00', 'avg_size'='288004948', 'max_size'='288004948' )
"""

sql """
alter table customer_demographics modify column cd_dep_college_count set stats ('row_count'='1920800', 'ndv'='7', 'min_value'='0', 'max_value'='6', 'avg_size'='7683200', 'max_size'='7683200' )
"""

sql """
alter table store modify column s_company_name set stats ('row_count'='402', 'ndv'='2', 'min_value'='', 'max_value'='Unknown', 'avg_size'='2793', 'max_size'='2793' )
"""

sql """
alter table web_site modify column web_zip set stats ('row_count'='24', 'ndv'='14', 'min_value'='28828', 'max_value'='78828', 'avg_size'='120', 'max_size'='120' )
"""

sql """
alter table warehouse modify column w_city set stats ('row_count'='15', 'ndv'='11', 'min_value'='Bethel', 'max_value'='Union', 'avg_size'='111', 'max_size'='111' )
"""

sql """
alter table catalog_sales modify column cs_net_paid_inc_tax set stats ('row_count'='143997065', 'ndv'='28777', 'min_value'='0.00', 'max_value'='31745.52', 'avg_size'='575988260', 'max_size'='575988260' )
"""

sql """
alter table store_returns modify column sr_return_quantity set stats ('row_count'='28795080', 'ndv'='100', 'min_value'='1', 'max_value'='100', 'avg_size'='115180320', 'max_size'='115180320' )
"""

sql """
alter table date_dim modify column d_date_id set stats ('row_count'='73049', 'ndv'='72907', 'min_value'='AAAAAAAAAAAAFCAA', 'max_value'='AAAAAAAAPPPPECAA', 'avg_size'='1168784', 'max_size'='1168784' )
"""

sql """
alter table store_sales modify column ss_net_profit set stats ('row_count'='287997024', 'ndv'='19581', 'min_value'='-10000.00', 'max_value'='9889.00', 'avg_size'='1151988096', 'max_size'='1151988096' )
"""

sql """
alter table call_center modify column cc_tax_percentage set stats ('row_count'='30', 'ndv'='1', 'min_value'='0.00', 'max_value'='0.12', 'avg_size'='120', 'max_size'='120' )
"""

sql """
alter table promotion modify column p_response_targe set stats ('row_count'='1000', 'ndv'='1', 'min_value'='1', 'max_value'='1', 'avg_size'='4000', 'max_size'='4000' )
"""

sql """
alter table time_dim modify column t_second set stats ('row_count'='86400', 'ndv'='60', 'min_value'='0', 'max_value'='59', 'avg_size'='345600', 'max_size'='345600' )
"""

sql """
alter table date_dim modify column d_first_dom set stats ('row_count'='73049', 'ndv'='2410', 'min_value'='2415021', 'max_value'='2488070', 'avg_size'='292196', 'max_size'='292196' )
"""

sql """
alter table web_returns modify column wr_return_amt set stats ('row_count'='7197670', 'ndv'='19263', 'min_value'='0.00', 'max_value'='28346.31', 'avg_size'='28790680', 'max_size'='28790680' )
"""

sql """
alter table web_site modify column web_site_sk set stats ('row_count'='24', 'ndv'='24', 'min_value'='1', 'max_value'='24', 'avg_size'='192', 'max_size'='192' )
"""

sql """
alter table catalog_returns modify column cr_ship_mode_sk set stats ('row_count'='14404374', 'ndv'='20', 'min_value'='1', 'max_value'='20', 'avg_size'='115234992', 'max_size'='115234992' )
"""

sql """
alter table warehouse modify column w_suite_number set stats ('row_count'='15', 'ndv'='14', 'min_value'='', 'max_value'='Suite X', 'avg_size'='111', 'max_size'='111' )
"""

sql """
alter table web_page modify column wp_web_page_sk set stats ('row_count'='2040', 'ndv'='2032', 'min_value'='1', 'max_value'='2040', 'avg_size'='16320', 'max_size'='16320' )
"""

sql """
alter table item modify column i_brand_id set stats ('row_count'='204000', 'ndv'='951', 'min_value'='1001001', 'max_value'='10016017', 'avg_size'='816000', 'max_size'='816000' )
"""

sql """
alter table store_sales modify column ss_customer_sk set stats ('row_count'='287997024', 'ndv'='1994393', 'min_value'='1', 'max_value'='2000000', 'avg_size'='2303976192', 'max_size'='2303976192' )
"""

sql """
alter table time_dim modify column t_minute set stats ('row_count'='86400', 'ndv'='60', 'min_value'='0', 'max_value'='59', 'avg_size'='345600', 'max_size'='345600' )
"""

sql """
alter table item modify column i_item_id set stats ('row_count'='204000', 'ndv'='103230', 'min_value'='AAAAAAAAAAAABAAA', 'max_value'='AAAAAAAAPPPPBAAA', 'avg_size'='3264000', 'max_size'='3264000' )
"""

sql """
alter table date_dim modify column d_current_day set stats ('row_count'='73049', 'ndv'='1', 'min_value'='N', 'max_value'='N', 'avg_size'='73049', 'max_size'='73049' )
"""

sql """
alter table item modify column i_manufact set stats ('row_count'='204000', 'ndv'='1004', 'min_value'='', 'max_value'='pripripri', 'avg_size'='2298787', 'max_size'='2298787' )
"""

sql """
alter table store modify column s_division_id set stats ('row_count'='402', 'ndv'='1', 'min_value'='1', 'max_value'='1', 'avg_size'='1608', 'max_size'='1608' )
"""

sql """
alter table dbgen_version modify column dv_create_date set stats ('row_count'='1', 'ndv'='1', 'min_value'='2023-03-16', 'max_value'='2023-03-16', 'avg_size'='4', 'max_size'='4' )
"""

sql """
alter table web_site modify column web_name set stats ('row_count'='24', 'ndv'='4', 'min_value'='site_0', 'max_value'='site_3', 'avg_size'='144', 'max_size'='144' )
"""

sql """
alter table customer_address modify column ca_suite_number set stats ('row_count'='1000000', 'ndv'='76', 'min_value'='', 'max_value'='Suite Y', 'avg_size'='7652799', 'max_size'='7652799' )
"""

sql """
alter table customer modify column c_first_sales_date_sk set stats ('row_count'='2000000', 'ndv'='3644', 'min_value'='2448998', 'max_value'='2452648', 'avg_size'='16000000', 'max_size'='16000000' )
"""

sql """
alter table web_sales modify column ws_order_number set stats ('row_count'='72001237', 'ndv'='6015811', 'min_value'='1', 'max_value'='6000000', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table store modify column s_zip set stats ('row_count'='402', 'ndv'='102', 'min_value'='', 'max_value'='79431', 'avg_size'='1980', 'max_size'='1980' )
"""

sql """
alter table promotion modify column p_item_sk set stats ('row_count'='1000', 'ndv'='970', 'min_value'='280', 'max_value'='203966', 'avg_size'='8000', 'max_size'='8000' )
"""

sql """
alter table web_sales modify column ws_ship_cdemo_sk set stats ('row_count'='72001237', 'ndv'='1822804', 'min_value'='1', 'max_value'='1920800', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table web_site modify column web_street_name set stats ('row_count'='24', 'ndv'='24', 'min_value'='11th ', 'max_value'='Wilson Ridge', 'avg_size'='219', 'max_size'='219' )
"""

sql """
alter table catalog_returns modify column cr_returning_hdemo_sk set stats ('row_count'='14404374', 'ndv'='7251', 'min_value'='1', 'max_value'='7200', 'avg_size'='115234992', 'max_size'='115234992' )
"""

sql """
alter table customer_demographics modify column cd_purchase_estimate set stats ('row_count'='1920800', 'ndv'='20', 'min_value'='500', 'max_value'='10000', 'avg_size'='7683200', 'max_size'='7683200' )
"""

sql """
alter table web_returns modify column wr_refunded_customer_sk set stats ('row_count'='7197670', 'ndv'='1923644', 'min_value'='1', 'max_value'='2000000', 'avg_size'='57581360', 'max_size'='57581360' )
"""

sql """
alter table catalog_sales modify column cs_ship_mode_sk set stats ('row_count'='143997065', 'ndv'='20', 'min_value'='1', 'max_value'='20', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table customer modify column c_birth_year set stats ('row_count'='2000000', 'ndv'='69', 'min_value'='1924', 'max_value'='1992', 'avg_size'='8000000', 'max_size'='8000000' )
"""

sql """
alter table catalog_returns modify column cr_return_tax set stats ('row_count'='14404374', 'ndv'='1926', 'min_value'='0.00', 'max_value'='2390.75', 'avg_size'='57617496', 'max_size'='57617496' )
"""

sql """
alter table web_sales modify column ws_ext_sales_price set stats ('row_count'='72001237', 'ndv'='27115', 'min_value'='0.00', 'max_value'='29810.00', 'avg_size'='288004948', 'max_size'='288004948' )
"""

sql """
alter table catalog_page modify column cp_catalog_page_number set stats ('row_count'='20400', 'ndv'='189', 'min_value'='1', 'max_value'='188', 'avg_size'='81600', 'max_size'='81600' )
"""

sql """
alter table date_dim modify column d_date_sk set stats ('row_count'='73049', 'ndv'='73042', 'min_value'='2415022', 'max_value'='2488070', 'avg_size'='584392', 'max_size'='584392' )
"""

sql """
alter table date_dim modify column d_month_seq set stats ('row_count'='73049', 'ndv'='2398', 'min_value'='0', 'max_value'='2400', 'avg_size'='292196', 'max_size'='292196' )
"""

sql """
alter table inventory modify column inv_item_sk set stats ('row_count'='399330000', 'ndv'='205012', 'min_value'='1', 'max_value'='204000', 'avg_size'='3194640000', 'max_size'='3194640000' )
"""

sql """
alter table call_center modify column cc_open_date_sk set stats ('row_count'='30', 'ndv'='15', 'min_value'='2450794', 'max_value'='2451146', 'avg_size'='120', 'max_size'='120' )
"""

sql """
alter table store_sales modify column ss_addr_sk set stats ('row_count'='287997024', 'ndv'='1000237', 'min_value'='1', 'max_value'='1000000', 'avg_size'='2303976192', 'max_size'='2303976192' )
"""

sql """
alter table web_returns modify column wr_returning_addr_sk set stats ('row_count'='7197670', 'ndv'='999584', 'min_value'='1', 'max_value'='1000000', 'avg_size'='57581360', 'max_size'='57581360' )
"""

sql """
alter table store modify column s_market_id set stats ('row_count'='402', 'ndv'='10', 'min_value'='1', 'max_value'='10', 'avg_size'='1608', 'max_size'='1608' )
"""

sql """
alter table catalog_sales modify column cs_bill_cdemo_sk set stats ('row_count'='143997065', 'ndv'='1915709', 'min_value'='1', 'max_value'='1920800', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table customer_address modify column ca_address_sk set stats ('row_count'='1000000', 'ndv'='1000237', 'min_value'='1', 'max_value'='1000000', 'avg_size'='8000000', 'max_size'='8000000' )
"""

sql """
alter table web_site modify column web_market_manager set stats ('row_count'='24', 'ndv'='21', 'min_value'='Albert Leung', 'max_value'='Zachery Oneil', 'avg_size'='294', 'max_size'='294' )
"""

sql """
alter table item modify column i_rec_start_date set stats ('row_count'='204000', 'ndv'='4', 'min_value'='1997-10-27', 'max_value'='2001-10-27', 'avg_size'='816000', 'max_size'='816000' )
"""

sql """
alter table web_sales modify column ws_ship_mode_sk set stats ('row_count'='72001237', 'ndv'='20', 'min_value'='1', 'max_value'='20', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table call_center modify column cc_street_type set stats ('row_count'='30', 'ndv'='9', 'min_value'='Avenue', 'max_value'='Way', 'avg_size'='140', 'max_size'='140' )
"""

sql """
alter table catalog_sales modify column cs_net_paid_inc_ship set stats ('row_count'='143997065', 'ndv'='37890', 'min_value'='0.00', 'max_value'='43725.00', 'avg_size'='575988260', 'max_size'='575988260' )
"""

sql """
alter table store_returns modify column sr_returned_date_sk set stats ('row_count'='28795080', 'ndv'='2010', 'min_value'='2450820', 'max_value'='2452822', 'avg_size'='230360640', 'max_size'='230360640' )
"""

sql """
alter table item modify column i_category set stats ('row_count'='204000', 'ndv'='11', 'min_value'='', 'max_value'='Women', 'avg_size'='1201703', 'max_size'='1201703' )
"""

sql """
alter table store modify column s_street_type set stats ('row_count'='402', 'ndv'='21', 'min_value'='', 'max_value'='Wy', 'avg_size'='1657', 'max_size'='1657' )
"""

sql """
alter table web_sales modify column ws_ext_list_price set stats ('row_count'='72001237', 'ndv'='29104', 'min_value'='1.02', 'max_value'='29997.00', 'avg_size'='288004948', 'max_size'='288004948' )
"""

sql """
alter table call_center modify column cc_city set stats ('row_count'='30', 'ndv'='12', 'min_value'='Bethel', 'max_value'='Shady Grove', 'avg_size'='282', 'max_size'='282' )
"""

sql """
alter table household_demographics modify column hd_buy_potential set stats ('row_count'='7200', 'ndv'='6', 'min_value'='0-500', 'max_value'='Unknown', 'avg_size'='54000', 'max_size'='54000' )
"""

sql """
alter table catalog_returns modify column cr_refunded_cdemo_sk set stats ('row_count'='14404374', 'ndv'='1900770', 'min_value'='1', 'max_value'='1920800', 'avg_size'='115234992', 'max_size'='115234992' )
"""

sql """
alter table item modify column i_manager_id set stats ('row_count'='204000', 'ndv'='100', 'min_value'='1', 'max_value'='100', 'avg_size'='816000', 'max_size'='816000' )
"""

sql """
alter table customer_address modify column ca_gmt_offset set stats ('row_count'='1000000', 'ndv'='6', 'min_value'='-10.00', 'max_value'='-5.00', 'avg_size'='4000000', 'max_size'='4000000' )
"""

sql """
alter table store modify column s_state set stats ('row_count'='402', 'ndv'='10', 'min_value'='', 'max_value'='TN', 'avg_size'='800', 'max_size'='800' )
"""

sql """
alter table catalog_returns modify column cr_refunded_customer_sk set stats ('row_count'='14404374', 'ndv'='1977657', 'min_value'='1', 'max_value'='2000000', 'avg_size'='115234992', 'max_size'='115234992' )
"""

sql """
alter table item modify column i_product_name set stats ('row_count'='204000', 'ndv'='200390', 'min_value'='', 'max_value'='pripripripripriought', 'avg_size'='4546148', 'max_size'='4546148' )
"""

sql """
alter table store_returns modify column sr_addr_sk set stats ('row_count'='28795080', 'ndv'='1000237', 'min_value'='1', 'max_value'='1000000', 'avg_size'='230360640', 'max_size'='230360640' )
"""

sql """
alter table item modify column i_category_id set stats ('row_count'='204000', 'ndv'='10', 'min_value'='1', 'max_value'='10', 'avg_size'='816000', 'max_size'='816000' )
"""

sql """
alter table store_returns modify column sr_return_ship_cost set stats ('row_count'='28795080', 'ndv'='8186', 'min_value'='0.00', 'max_value'='9578.25', 'avg_size'='115180320', 'max_size'='115180320' )
"""

sql """
alter table catalog_sales modify column cs_sold_time_sk set stats ('row_count'='143997065', 'ndv'='87677', 'min_value'='0', 'max_value'='86399', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table date_dim modify column d_day_name set stats ('row_count'='73049', 'ndv'='7', 'min_value'='Friday', 'max_value'='Wednesday', 'avg_size'='521779', 'max_size'='521779' )
"""

sql """
alter table web_returns modify column wr_web_page_sk set stats ('row_count'='7197670', 'ndv'='2032', 'min_value'='1', 'max_value'='2040', 'avg_size'='57581360', 'max_size'='57581360' )
"""

sql """
alter table store modify column s_street_number set stats ('row_count'='402', 'ndv'='267', 'min_value'='', 'max_value'='986', 'avg_size'='1150', 'max_size'='1150' )
"""

sql """
alter table web_sales modify column ws_sold_time_sk set stats ('row_count'='72001237', 'ndv'='87677', 'min_value'='0', 'max_value'='86399', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table store_sales modify column ss_ext_tax set stats ('row_count'='287997024', 'ndv'='1722', 'min_value'='0.00', 'max_value'='1762.38', 'avg_size'='1151988096', 'max_size'='1151988096' )
"""

sql """
alter table date_dim modify column d_dow set stats ('row_count'='73049', 'ndv'='7', 'min_value'='0', 'max_value'='6', 'avg_size'='292196', 'max_size'='292196' )
"""

sql """
alter table store_returns modify column sr_refunded_cash set stats ('row_count'='28795080', 'ndv'='12626', 'min_value'='0.00', 'max_value'='17556.95', 'avg_size'='115180320', 'max_size'='115180320' )
"""

sql """
alter table call_center modify column cc_call_center_sk set stats ('row_count'='30', 'ndv'='30', 'min_value'='1', 'max_value'='30', 'avg_size'='240', 'max_size'='240' )
"""

sql """
alter table store_returns modify column sr_fee set stats ('row_count'='28795080', 'ndv'='101', 'min_value'='0.50', 'max_value'='100.00', 'avg_size'='115180320', 'max_size'='115180320' )
"""

sql """
alter table catalog_returns modify column cr_return_ship_cost set stats ('row_count'='14404374', 'ndv'='11144', 'min_value'='0.00', 'max_value'='14130.96', 'avg_size'='57617496', 'max_size'='57617496' )
"""

sql """
alter table catalog_sales modify column cs_bill_addr_sk set stats ('row_count'='143997065', 'ndv'='1000237', 'min_value'='1', 'max_value'='1000000', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table time_dim modify column t_time_id set stats ('row_count'='86400', 'ndv'='85663', 'min_value'='AAAAAAAAAAAABAAA', 'max_value'='AAAAAAAAPPPPAAAA', 'avg_size'='1382400', 'max_size'='1382400' )
"""

sql """
alter table catalog_sales modify column cs_net_paid set stats ('row_count'='143997065', 'ndv'='27448', 'min_value'='0.00', 'max_value'='29760.00', 'avg_size'='575988260', 'max_size'='575988260' )
"""

sql """
alter table catalog_sales modify column cs_bill_customer_sk set stats ('row_count'='143997065', 'ndv'='1993691', 'min_value'='1', 'max_value'='2000000', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table web_sales modify column ws_coupon_amt set stats ('row_count'='72001237', 'ndv'='20659', 'min_value'='0.00', 'max_value'='27591.16', 'avg_size'='288004948', 'max_size'='288004948' )
"""

sql """
alter table promotion modify column p_promo_sk set stats ('row_count'='1000', 'ndv'='986', 'min_value'='1', 'max_value'='1000', 'avg_size'='8000', 'max_size'='8000' )
"""

sql """
alter table web_page modify column wp_rec_end_date set stats ('row_count'='2040', 'ndv'='3', 'min_value'='1999-09-03', 'max_value'='2001-09-02', 'avg_size'='8160', 'max_size'='8160' )
"""

sql """
alter table web_returns modify column wr_refunded_addr_sk set stats ('row_count'='7197670', 'ndv'='999503', 'min_value'='1', 'max_value'='1000000', 'avg_size'='57581360', 'max_size'='57581360' )
"""

sql """
alter table web_page modify column wp_char_count set stats ('row_count'='2040', 'ndv'='1363', 'min_value'='303', 'max_value'='8523', 'avg_size'='8160', 'max_size'='8160' )
"""

sql """
alter table promotion modify column p_purpose set stats ('row_count'='1000', 'ndv'='2', 'min_value'='', 'max_value'='Unknown', 'avg_size'='6909', 'max_size'='6909' )
"""

sql """
alter table web_sales modify column ws_ship_date_sk set stats ('row_count'='72001237', 'ndv'='1952', 'min_value'='2450817', 'max_value'='2452762', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table date_dim modify column d_current_year set stats ('row_count'='73049', 'ndv'='2', 'min_value'='N', 'max_value'='Y', 'avg_size'='73049', 'max_size'='73049' )
"""

sql """
alter table store_sales modify column ss_net_paid set stats ('row_count'='287997024', 'ndv'='19028', 'min_value'='0.00', 'max_value'='19878.00', 'avg_size'='1151988096', 'max_size'='1151988096' )
"""

sql """
alter table web_returns modify column wr_returned_date_sk set stats ('row_count'='7197670', 'ndv'='2185', 'min_value'='2450820', 'max_value'='2453002', 'avg_size'='57581360', 'max_size'='57581360' )
"""

sql """
alter table store_returns modify column sr_cdemo_sk set stats ('row_count'='28795080', 'ndv'='1916366', 'min_value'='1', 'max_value'='1920800', 'avg_size'='230360640', 'max_size'='230360640' )
"""

sql """
alter table catalog_page modify column cp_description set stats ('row_count'='20400', 'ndv'='20501', 'min_value'='', 'max_value'='Youngsters should get very. Bad, necessary years must pick telecommunications. Co', 'avg_size'='1507423', 'max_size'='1507423' )
"""

sql """
alter table catalog_sales modify column cs_ext_tax set stats ('row_count'='143997065', 'ndv'='2488', 'min_value'='0.00', 'max_value'='2619.36', 'avg_size'='575988260', 'max_size'='575988260' )
"""

sql """
alter table date_dim modify column d_holiday set stats ('row_count'='73049', 'ndv'='2', 'min_value'='N', 'max_value'='Y', 'avg_size'='73049', 'max_size'='73049' )
"""

sql """
alter table catalog_sales modify column cs_ext_discount_amt set stats ('row_count'='143997065', 'ndv'='27722', 'min_value'='0.00', 'max_value'='29765.00', 'avg_size'='575988260', 'max_size'='575988260' )
"""

sql """
alter table warehouse modify column w_zip set stats ('row_count'='15', 'ndv'='15', 'min_value'='28721', 'max_value'='78721', 'avg_size'='75', 'max_size'='75' )
"""

sql """
alter table catalog_returns modify column cr_catalog_page_sk set stats ('row_count'='14404374', 'ndv'='11515', 'min_value'='1', 'max_value'='17108', 'avg_size'='115234992', 'max_size'='115234992' )
"""

sql """
alter table catalog_returns modify column cr_order_number set stats ('row_count'='14404374', 'ndv'='9425725', 'min_value'='2', 'max_value'='16000000', 'avg_size'='115234992', 'max_size'='115234992' )
"""

sql """
alter table catalog_sales modify column cs_ship_cdemo_sk set stats ('row_count'='143997065', 'ndv'='1916125', 'min_value'='1', 'max_value'='1920800', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table catalog_returns modify column cr_returned_time_sk set stats ('row_count'='14404374', 'ndv'='87677', 'min_value'='0', 'max_value'='86399', 'avg_size'='115234992', 'max_size'='115234992' )
"""

sql """
alter table web_sales modify column ws_ext_wholesale_cost set stats ('row_count'='72001237', 'ndv'='10009', 'min_value'='1.00', 'max_value'='10000.00', 'avg_size'='288004948', 'max_size'='288004948' )
"""

sql """
alter table web_page modify column wp_image_count set stats ('row_count'='2040', 'ndv'='7', 'min_value'='1', 'max_value'='7', 'avg_size'='8160', 'max_size'='8160' )
"""

sql """
alter table time_dim modify column t_shift set stats ('row_count'='86400', 'ndv'='3', 'min_value'='first', 'max_value'='third', 'avg_size'='460800', 'max_size'='460800' )
"""

sql """
alter table store_sales modify column ss_ext_discount_amt set stats ('row_count'='287997024', 'ndv'='16198', 'min_value'='0.00', 'max_value'='19225.00', 'avg_size'='1151988096', 'max_size'='1151988096' )
"""

sql """
alter table warehouse modify column w_warehouse_sk set stats ('row_count'='15', 'ndv'='15', 'min_value'='1', 'max_value'='15', 'avg_size'='120', 'max_size'='120' )
"""

sql """
alter table store_sales modify column ss_sold_time_sk set stats ('row_count'='287997024', 'ndv'='47252', 'min_value'='28800', 'max_value'='75599', 'avg_size'='2303976192', 'max_size'='2303976192' )
"""

sql """
alter table customer_address modify column ca_street_name set stats ('row_count'='1000000', 'ndv'='8155', 'min_value'='', 'max_value'='Woodland Woodland', 'avg_size'='8445649', 'max_size'='8445649' )
"""

sql """
alter table customer_address modify column ca_county set stats ('row_count'='1000000', 'ndv'='1825', 'min_value'='', 'max_value'='Ziebach County', 'avg_size'='13540273', 'max_size'='13540273' )
"""

sql """
alter table ship_mode modify column sm_contract set stats ('row_count'='20', 'ndv'='20', 'min_value'='2mM8l', 'max_value'='yVfotg7Tio3MVhBg6Bkn', 'avg_size'='252', 'max_size'='252' )
"""

sql """
alter table call_center modify column cc_closed_date_sk set stats ('row_count'='30', 'ndv'='0', 'min_value'='0', 'max_value'='0', 'avg_size'='120', 'max_size'='120' )
"""

sql """
alter table customer_address modify column ca_zip set stats ('row_count'='1000000', 'ndv'='7733', 'min_value'='', 'max_value'='99981', 'avg_size'='4848150', 'max_size'='4848150' )
"""

sql """
alter table store modify column s_county set stats ('row_count'='402', 'ndv'='10', 'min_value'='', 'max_value'='Ziebach County', 'avg_size'='5693', 'max_size'='5693' )
"""

sql """
alter table promotion modify column p_channel_tv set stats ('row_count'='1000', 'ndv'='2', 'min_value'='', 'max_value'='N', 'avg_size'='986', 'max_size'='986' )
"""

sql """
alter table time_dim modify column t_time_sk set stats ('row_count'='86400', 'ndv'='87677', 'min_value'='0', 'max_value'='86399', 'avg_size'='691200', 'max_size'='691200' )
"""

sql """
alter table date_dim modify column d_following_holiday set stats ('row_count'='73049', 'ndv'='2', 'min_value'='N', 'max_value'='Y', 'avg_size'='73049', 'max_size'='73049' )
"""

sql """
alter table store_returns modify column sr_return_time_sk set stats ('row_count'='28795080', 'ndv'='32660', 'min_value'='28799', 'max_value'='61199', 'avg_size'='230360640', 'max_size'='230360640' )
"""

sql """
alter table catalog_sales modify column cs_ext_ship_cost set stats ('row_count'='143997065', 'ndv'='14266', 'min_value'='0.00', 'max_value'='14896.00', 'avg_size'='575988260', 'max_size'='575988260' )
"""

sql """
alter table item modify column i_brand set stats ('row_count'='204000', 'ndv'='714', 'min_value'='', 'max_value'='univunivamalg #9', 'avg_size'='3287671', 'max_size'='3287671' )
"""

sql """
alter table customer modify column c_current_addr_sk set stats ('row_count'='2000000', 'ndv'='866672', 'min_value'='1', 'max_value'='1000000', 'avg_size'='16000000', 'max_size'='16000000' )
"""

sql """
alter table store modify column s_floor_space set stats ('row_count'='402', 'ndv'='300', 'min_value'='5004767', 'max_value'='9997773', 'avg_size'='1608', 'max_size'='1608' )
"""

sql """
alter table inventory modify column inv_warehouse_sk set stats ('row_count'='399330000', 'ndv'='15', 'min_value'='1', 'max_value'='15', 'avg_size'='3194640000', 'max_size'='3194640000' )
"""

sql """
alter table web_site modify column web_county set stats ('row_count'='24', 'ndv'='9', 'min_value'='Barrow County', 'max_value'='Ziebach County', 'avg_size'='331', 'max_size'='331' )
"""

sql """
alter table call_center modify column cc_rec_start_date set stats ('row_count'='30', 'ndv'='4', 'min_value'='1998-01-01', 'max_value'='2002-01-01', 'avg_size'='120', 'max_size'='120' )
"""

sql """
alter table date_dim modify column d_quarter_name set stats ('row_count'='73049', 'ndv'='799', 'min_value'='1900Q1', 'max_value'='2100Q1', 'avg_size'='438294', 'max_size'='438294' )
"""

sql """
alter table call_center modify column cc_company_name set stats ('row_count'='30', 'ndv'='6', 'min_value'='able', 'max_value'='pri', 'avg_size'='110', 'max_size'='110' )
"""

sql """
alter table customer_demographics modify column cd_credit_rating set stats ('row_count'='1920800', 'ndv'='4', 'min_value'='Good', 'max_value'='Unknown', 'avg_size'='13445600', 'max_size'='13445600' )
"""

sql """
alter table web_returns modify column wr_return_amt_inc_tax set stats ('row_count'='7197670', 'ndv'='19975', 'min_value'='0.00', 'max_value'='29493.38', 'avg_size'='28790680', 'max_size'='28790680' )
"""

sql """
alter table web_site modify column web_company_id set stats ('row_count'='24', 'ndv'='6', 'min_value'='1', 'max_value'='6', 'avg_size'='96', 'max_size'='96' )
"""

sql """
alter table date_dim modify column d_qoy set stats ('row_count'='73049', 'ndv'='4', 'min_value'='1', 'max_value'='4', 'avg_size'='292196', 'max_size'='292196' )
"""

sql """
alter table catalog_sales modify column cs_quantity set stats ('row_count'='143997065', 'ndv'='100', 'min_value'='1', 'max_value'='100', 'avg_size'='575988260', 'max_size'='575988260' )
"""

sql """
alter table web_sales modify column ws_ext_ship_cost set stats ('row_count'='72001237', 'ndv'='13977', 'min_value'='0.00', 'max_value'='14927.00', 'avg_size'='288004948', 'max_size'='288004948' )
"""

sql """
alter table catalog_sales modify column cs_list_price set stats ('row_count'='143997065', 'ndv'='301', 'min_value'='1.00', 'max_value'='300.00', 'avg_size'='575988260', 'max_size'='575988260' )
"""

sql """
alter table call_center modify column cc_zip set stats ('row_count'='30', 'ndv'='14', 'min_value'='20059', 'max_value'='75281', 'avg_size'='150', 'max_size'='150' )
"""

sql """
alter table call_center modify column cc_division_name set stats ('row_count'='30', 'ndv'='6', 'min_value'='able', 'max_value'='pri', 'avg_size'='123', 'max_size'='123' )
"""

sql """
alter table store_sales modify column ss_cdemo_sk set stats ('row_count'='287997024', 'ndv'='1916366', 'min_value'='1', 'max_value'='1920800', 'avg_size'='2303976192', 'max_size'='2303976192' )
"""

sql """
alter table catalog_sales modify column cs_ext_sales_price set stats ('row_count'='143997065', 'ndv'='27598', 'min_value'='0.00', 'max_value'='29808.00', 'avg_size'='575988260', 'max_size'='575988260' )
"""

sql """
alter table catalog_returns modify column cr_return_amt_inc_tax set stats ('row_count'='14404374', 'ndv'='21566', 'min_value'='0.00', 'max_value'='29353.87', 'avg_size'='57617496', 'max_size'='57617496' )
"""

sql """
alter table income_band modify column ib_upper_bound set stats ('row_count'='20', 'ndv'='20', 'min_value'='10000', 'max_value'='200000', 'avg_size'='80', 'max_size'='80' )
"""

sql """
alter table item modify column i_color set stats ('row_count'='204000', 'ndv'='93', 'min_value'='', 'max_value'='yellow', 'avg_size'='1094247', 'max_size'='1094247' )
"""

sql """
alter table catalog_sales modify column cs_ship_hdemo_sk set stats ('row_count'='143997065', 'ndv'='7251', 'min_value'='1', 'max_value'='7200', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table store modify column s_tax_precentage set stats ('row_count'='402', 'ndv'='1', 'min_value'='0.00', 'max_value'='0.11', 'avg_size'='1608', 'max_size'='1608' )
"""

sql """
alter table item modify column i_units set stats ('row_count'='204000', 'ndv'='22', 'min_value'='', 'max_value'='Unknown', 'avg_size'='852562', 'max_size'='852562' )
"""

sql """
alter table reason modify column r_reason_id set stats ('row_count'='55', 'ndv'='55', 'min_value'='AAAAAAAAABAAAAAA', 'max_value'='AAAAAAAAPCAAAAAA', 'avg_size'='880', 'max_size'='880' )
"""

sql """
alter table store_sales modify column ss_ext_list_price set stats ('row_count'='287997024', 'ndv'='19770', 'min_value'='1.00', 'max_value'='20000.00', 'avg_size'='1151988096', 'max_size'='1151988096' )
"""

sql """
alter table promotion modify column p_cost set stats ('row_count'='1000', 'ndv'='1', 'min_value'='1000.00', 'max_value'='1000.00', 'avg_size'='8000', 'max_size'='8000' )
"""

sql """
alter table web_site modify column web_state set stats ('row_count'='24', 'ndv'='9', 'min_value'='AL', 'max_value'='TN', 'avg_size'='48', 'max_size'='48' )
"""

sql """
alter table call_center modify column cc_country set stats ('row_count'='30', 'ndv'='1', 'min_value'='United States', 'max_value'='United States', 'avg_size'='390', 'max_size'='390' )
"""

sql """
alter table store modify column s_company_id set stats ('row_count'='402', 'ndv'='1', 'min_value'='1', 'max_value'='1', 'avg_size'='1608', 'max_size'='1608' )
"""

sql """
alter table time_dim modify column t_hour set stats ('row_count'='86400', 'ndv'='24', 'min_value'='0', 'max_value'='23', 'avg_size'='345600', 'max_size'='345600' )
"""

sql """
alter table date_dim modify column d_fy_quarter_seq set stats ('row_count'='73049', 'ndv'='801', 'min_value'='1', 'max_value'='801', 'avg_size'='292196', 'max_size'='292196' )
"""

sql """
alter table ship_mode modify column sm_code set stats ('row_count'='20', 'ndv'='4', 'min_value'='AIR', 'max_value'='SURFACE', 'avg_size'='87', 'max_size'='87' )
"""

sql """
alter table web_returns modify column wr_returning_hdemo_sk set stats ('row_count'='7197670', 'ndv'='7251', 'min_value'='1', 'max_value'='7200', 'avg_size'='57581360', 'max_size'='57581360' )
"""

sql """
alter table catalog_returns modify column cr_call_center_sk set stats ('row_count'='14404374', 'ndv'='30', 'min_value'='1', 'max_value'='30', 'avg_size'='115234992', 'max_size'='115234992' )
"""

sql """
alter table household_demographics modify column hd_demo_sk set stats ('row_count'='7200', 'ndv'='7251', 'min_value'='1', 'max_value'='7200', 'avg_size'='57600', 'max_size'='57600' )
"""

sql """
alter table catalog_returns modify column cr_net_loss set stats ('row_count'='14404374', 'ndv'='11753', 'min_value'='0.50', 'max_value'='15781.83', 'avg_size'='57617496', 'max_size'='57617496' )
"""

sql """
alter table catalog_returns modify column cr_item_sk set stats ('row_count'='14404374', 'ndv'='205012', 'min_value'='1', 'max_value'='204000', 'avg_size'='115234992', 'max_size'='115234992' )
"""

sql """
alter table store_returns modify column sr_item_sk set stats ('row_count'='28795080', 'ndv'='205012', 'min_value'='1', 'max_value'='204000', 'avg_size'='230360640', 'max_size'='230360640' )
"""

sql """
alter table call_center modify column cc_street_number set stats ('row_count'='30', 'ndv'='15', 'min_value'='406', 'max_value'='984', 'avg_size'='88', 'max_size'='88' )
"""

sql """
alter table promotion modify column p_channel_radio set stats ('row_count'='1000', 'ndv'='2', 'min_value'='', 'max_value'='N', 'avg_size'='987', 'max_size'='987' )
"""

sql """
alter table call_center modify column cc_name set stats ('row_count'='30', 'ndv'='15', 'min_value'='California', 'max_value'='Pacific Northwest_1', 'avg_size'='401', 'max_size'='401' )
"""

sql """
alter table call_center modify column cc_rec_end_date set stats ('row_count'='30', 'ndv'='3', 'min_value'='2000-01-01', 'max_value'='2001-12-31', 'avg_size'='120', 'max_size'='120' )
"""

sql """
alter table customer_demographics modify column cd_dep_count set stats ('row_count'='1920800', 'ndv'='7', 'min_value'='0', 'max_value'='6', 'avg_size'='7683200', 'max_size'='7683200' )
"""

sql """
alter table inventory modify column inv_date_sk set stats ('row_count'='399330000', 'ndv'='261', 'min_value'='2450815', 'max_value'='2452635', 'avg_size'='3194640000', 'max_size'='3194640000' )
"""

sql """
alter table customer_demographics modify column cd_demo_sk set stats ('row_count'='1920800', 'ndv'='1916366', 'min_value'='1', 'max_value'='1920800', 'avg_size'='15366400', 'max_size'='15366400' )
"""

sql """
alter table ship_mode modify column sm_ship_mode_sk set stats ('row_count'='20', 'ndv'='20', 'min_value'='1', 'max_value'='20', 'avg_size'='160', 'max_size'='160' )
"""

sql """
alter table store_sales modify column ss_list_price set stats ('row_count'='287997024', 'ndv'='201', 'min_value'='1.00', 'max_value'='200.00', 'avg_size'='1151988096', 'max_size'='1151988096' )
"""

sql """
alter table reason modify column r_reason_sk set stats ('row_count'='55', 'ndv'='55', 'min_value'='1', 'max_value'='55', 'avg_size'='440', 'max_size'='440' )
"""

sql """
alter table web_page modify column wp_autogen_flag set stats ('row_count'='2040', 'ndv'='3', 'min_value'='', 'max_value'='Y', 'avg_size'='2015', 'max_size'='2015' )
"""

sql """
alter table web_sales modify column ws_sold_date_sk set stats ('row_count'='72001237', 'ndv'='1820', 'min_value'='2450816', 'max_value'='2452642', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table catalog_returns modify column cr_returning_addr_sk set stats ('row_count'='14404374', 'ndv'='1000237', 'min_value'='1', 'max_value'='1000000', 'avg_size'='115234992', 'max_size'='115234992' )
"""

sql """
alter table web_site modify column web_street_type set stats ('row_count'='24', 'ndv'='15', 'min_value'='Avenue', 'max_value'='Wy', 'avg_size'='96', 'max_size'='96' )
"""

sql """
alter table store modify column s_rec_end_date set stats ('row_count'='402', 'ndv'='3', 'min_value'='1999-03-13', 'max_value'='2001-03-12', 'avg_size'='1608', 'max_size'='1608' )
"""

sql """
alter table item modify column i_formulation set stats ('row_count'='204000', 'ndv'='152702', 'min_value'='', 'max_value'='yellow98911509228741', 'avg_size'='4069400', 'max_size'='4069400' )
"""

sql """
alter table customer_demographics modify column cd_education_status set stats ('row_count'='1920800', 'ndv'='7', 'min_value'='2 yr Degree', 'max_value'='Unknown', 'avg_size'='18384800', 'max_size'='18384800' )
"""

sql """
alter table web_page modify column wp_link_count set stats ('row_count'='2040', 'ndv'='24', 'min_value'='2', 'max_value'='25', 'avg_size'='8160', 'max_size'='8160' )
"""

sql """
alter table warehouse modify column w_country set stats ('row_count'='15', 'ndv'='1', 'min_value'='United States', 'max_value'='United States', 'avg_size'='195', 'max_size'='195' )
"""

sql """
alter table catalog_returns modify column cr_store_credit set stats ('row_count'='14404374', 'ndv'='12156', 'min_value'='0.00', 'max_value'='22167.49', 'avg_size'='57617496', 'max_size'='57617496' )
"""

sql """
alter table store modify column s_rec_start_date set stats ('row_count'='402', 'ndv'='4', 'min_value'='1997-03-13', 'max_value'='2001-03-13', 'avg_size'='1608', 'max_size'='1608' )
"""

sql """
alter table web_site modify column web_site_id set stats ('row_count'='24', 'ndv'='12', 'min_value'='AAAAAAAAABAAAAAA', 'max_value'='AAAAAAAAOAAAAAAA', 'avg_size'='384', 'max_size'='384' )
"""

sql """
alter table call_center modify column cc_gmt_offset set stats ('row_count'='30', 'ndv'='2', 'min_value'='-6.00', 'max_value'='-5.00', 'avg_size'='120', 'max_size'='120' )
"""

sql """
alter table ship_mode modify column sm_ship_mode_id set stats ('row_count'='20', 'ndv'='20', 'min_value'='AAAAAAAAABAAAAAA', 'max_value'='AAAAAAAAPAAAAAAA', 'avg_size'='320', 'max_size'='320' )
"""

sql """
alter table catalog_returns modify column cr_return_amount set stats ('row_count'='14404374', 'ndv'='20656', 'min_value'='0.00', 'max_value'='28778.31', 'avg_size'='57617496', 'max_size'='57617496' )
"""

sql """
alter table store modify column s_hours set stats ('row_count'='402', 'ndv'='4', 'min_value'='', 'max_value'='8AM-8AM', 'avg_size'='2848', 'max_size'='2848' )
"""

sql """
alter table web_returns modify column wr_returning_cdemo_sk set stats ('row_count'='7197670', 'ndv'='1865149', 'min_value'='1', 'max_value'='1920800', 'avg_size'='57581360', 'max_size'='57581360' )
"""

sql """
alter table catalog_sales modify column cs_warehouse_sk set stats ('row_count'='143997065', 'ndv'='15', 'min_value'='1', 'max_value'='15', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table date_dim modify column d_date set stats ('row_count'='73049', 'ndv'='73250', 'min_value'='1900-01-02', 'max_value'='2100-01-01', 'avg_size'='292196', 'max_size'='292196' )
"""

sql """
alter table customer modify column c_first_name set stats ('row_count'='2000000', 'ndv'='5140', 'min_value'='', 'max_value'='Zulma', 'avg_size'='11267996', 'max_size'='11267996' )
"""

sql """
alter table catalog_sales modify column cs_net_profit set stats ('row_count'='143997065', 'ndv'='28450', 'min_value'='-10000.00', 'max_value'='19840.00', 'avg_size'='575988260', 'max_size'='575988260' )
"""

sql """
alter table web_site modify column web_suite_number set stats ('row_count'='24', 'ndv'='20', 'min_value'='Suite 130', 'max_value'='Suite U', 'avg_size'='196', 'max_size'='196' )
"""

sql """
alter table web_sales modify column ws_list_price set stats ('row_count'='72001237', 'ndv'='301', 'min_value'='1.00', 'max_value'='300.00', 'avg_size'='288004948', 'max_size'='288004948' )
"""

sql """
alter table web_returns modify column wr_returned_time_sk set stats ('row_count'='7197670', 'ndv'='87677', 'min_value'='0', 'max_value'='86399', 'avg_size'='57581360', 'max_size'='57581360' )
"""

sql """
alter table web_sales modify column ws_net_paid_inc_tax set stats ('row_count'='72001237', 'ndv'='28263', 'min_value'='0.00', 'max_value'='32492.90', 'avg_size'='288004948', 'max_size'='288004948' )
"""

sql """
alter table store_returns modify column sr_net_loss set stats ('row_count'='28795080', 'ndv'='8663', 'min_value'='0.50', 'max_value'='10447.72', 'avg_size'='115180320', 'max_size'='115180320' )
"""

sql """
alter table date_dim modify column d_same_day_lq set stats ('row_count'='73049', 'ndv'='72231', 'min_value'='2414930', 'max_value'='2487978', 'avg_size'='292196', 'max_size'='292196' )
"""

sql """
alter table store modify column s_suite_number set stats ('row_count'='402', 'ndv'='75', 'min_value'='', 'max_value'='Suite Y', 'avg_size'='3140', 'max_size'='3140' )
"""

sql """
alter table catalog_page modify column cp_start_date_sk set stats ('row_count'='20400', 'ndv'='91', 'min_value'='2450815', 'max_value'='2453005', 'avg_size'='81600', 'max_size'='81600' )
"""

sql """
alter table customer_address modify column ca_street_number set stats ('row_count'='1000000', 'ndv'='1002', 'min_value'='', 'max_value'='999', 'avg_size'='2805540', 'max_size'='2805540' )
"""

sql """
alter table item modify column i_current_price set stats ('row_count'='204000', 'ndv'='100', 'min_value'='0.09', 'max_value'='99.99', 'avg_size'='816000', 'max_size'='816000' )
"""

sql """
alter table store_returns modify column sr_ticket_number set stats ('row_count'='28795080', 'ndv'='16790866', 'min_value'='1', 'max_value'='23999996', 'avg_size'='230360640', 'max_size'='230360640' )
"""

sql """
alter table catalog_sales modify column cs_coupon_amt set stats ('row_count'='143997065', 'ndv'='22020', 'min_value'='0.00', 'max_value'='28422.94', 'avg_size'='575988260', 'max_size'='575988260' )
"""

sql """
alter table date_dim modify column d_current_month set stats ('row_count'='73049', 'ndv'='2', 'min_value'='N', 'max_value'='Y', 'avg_size'='73049', 'max_size'='73049' )
"""

sql """
alter table web_sales modify column ws_net_paid_inc_ship_tax set stats ('row_count'='72001237', 'ndv'='37541', 'min_value'='0.00', 'max_value'='44479.52', 'avg_size'='288004948', 'max_size'='288004948' )
"""

sql """
alter table web_sales modify column ws_promo_sk set stats ('row_count'='72001237', 'ndv'='986', 'min_value'='1', 'max_value'='1000', 'avg_size'='576009896', 'max_size'='576009896' )
"""

sql """
alter table customer modify column c_first_shipto_date_sk set stats ('row_count'='2000000', 'ndv'='3644', 'min_value'='2449028', 'max_value'='2452678', 'avg_size'='16000000', 'max_size'='16000000' )
"""

sql """
alter table catalog_page modify column cp_end_date_sk set stats ('row_count'='20400', 'ndv'='97', 'min_value'='2450844', 'max_value'='2453186', 'avg_size'='81600', 'max_size'='81600' )
"""

sql """
alter table store_sales modify column ss_promo_sk set stats ('row_count'='287997024', 'ndv'='986', 'min_value'='1', 'max_value'='1000', 'avg_size'='2303976192', 'max_size'='2303976192' )
"""

sql """
alter table catalog_page modify column cp_type set stats ('row_count'='20400', 'ndv'='4', 'min_value'='', 'max_value'='quarterly', 'avg_size'='155039', 'max_size'='155039' )
"""

sql """
alter table promotion modify column p_channel_demo set stats ('row_count'='1000', 'ndv'='2', 'min_value'='', 'max_value'='N', 'avg_size'='984', 'max_size'='984' )
"""

sql """
alter table store modify column s_market_manager set stats ('row_count'='402', 'ndv'='286', 'min_value'='', 'max_value'='Zane Perez', 'avg_size'='5129', 'max_size'='5129' )
"""

sql """
alter table item modify column i_item_desc set stats ('row_count'='204000', 'ndv'='148398', 'min_value'='', 'max_value'='Youngsters used to save quite colour', 'avg_size'='20471814', 'max_size'='20471814' )
"""

sql """
alter table call_center modify column cc_division set stats ('row_count'='30', 'ndv'='6', 'min_value'='1', 'max_value'='6', 'avg_size'='120', 'max_size'='120' )
"""

sql """
alter table web_site modify column web_class set stats ('row_count'='24', 'ndv'='1', 'min_value'='Unknown', 'max_value'='Unknown', 'avg_size'='168', 'max_size'='168' )
"""

sql """
alter table store modify column s_geography_class set stats ('row_count'='402', 'ndv'='2', 'min_value'='', 'max_value'='Unknown', 'avg_size'='2793', 'max_size'='2793' )
"""

sql """
alter table store_returns modify column sr_store_sk set stats ('row_count'='28795080', 'ndv'='200', 'min_value'='1', 'max_value'='400', 'avg_size'='230360640', 'max_size'='230360640' )
"""

sql """
alter table call_center modify column cc_street_name set stats ('row_count'='30', 'ndv'='15', 'min_value'='1st ', 'max_value'='View ', 'avg_size'='240', 'max_size'='240' )
"""

sql """
alter table date_dim modify column d_moy set stats ('row_count'='73049', 'ndv'='12', 'min_value'='1', 'max_value'='12', 'avg_size'='292196', 'max_size'='292196' )
"""

sql """
alter table customer modify column c_current_hdemo_sk set stats ('row_count'='2000000', 'ndv'='7251', 'min_value'='1', 'max_value'='7200', 'avg_size'='16000000', 'max_size'='16000000' )
"""

sql """
alter table customer modify column c_login set stats ('row_count'='2000000', 'ndv'='1', 'min_value'='', 'max_value'='', 'avg_size'='0', 'max_size'='0' )
"""

sql """
alter table web_sales modify column ws_ext_discount_amt set stats ('row_count'='72001237', 'ndv'='27052', 'min_value'='0.00', 'max_value'='29982.00', 'avg_size'='288004948', 'max_size'='288004948' )
"""

sql """
alter table call_center modify column cc_call_center_id set stats ('row_count'='30', 'ndv'='15', 'min_value'='AAAAAAAAABAAAAAA', 'max_value'='AAAAAAAAOAAAAAAA', 'avg_size'='480', 'max_size'='480' )
"""

sql """
alter table web_returns modify column wr_reversed_charge set stats ('row_count'='7197670', 'ndv'='10979', 'min_value'='0.00', 'max_value'='22972.36', 'avg_size'='28790680', 'max_size'='28790680' )
"""

sql """
alter table store modify column s_city set stats ('row_count'='402', 'ndv'='19', 'min_value'='', 'max_value'='Union', 'avg_size'='3669', 'max_size'='3669' )
"""

sql """
alter table promotion modify column p_channel_email set stats ('row_count'='1000', 'ndv'='2', 'min_value'='', 'max_value'='N', 'avg_size'='987', 'max_size'='987' )
"""

sql """
alter table catalog_page modify column cp_department set stats ('row_count'='20400', 'ndv'='2', 'min_value'='', 'max_value'='DEPARTMENT', 'avg_size'='201950', 'max_size'='201950' )
"""

sql """
alter table call_center modify column cc_hours set stats ('row_count'='30', 'ndv'='3', 'min_value'='8AM-12AM', 'max_value'='8AM-8AM', 'avg_size'='214', 'max_size'='214' )
"""

sql """
alter table promotion modify column p_channel_dmail set stats ('row_count'='1000', 'ndv'='3', 'min_value'='', 'max_value'='Y', 'avg_size'='987', 'max_size'='987' )
"""

sql """
alter table store modify column s_manager set stats ('row_count'='402', 'ndv'='301', 'min_value'='', 'max_value'='Zachary Price', 'avg_size'='5075', 'max_size'='5075' )
"""

sql """
alter table store_returns modify column sr_reversed_charge set stats ('row_count'='28795080', 'ndv'='9872', 'min_value'='0.00', 'max_value'='16099.52', 'avg_size'='115180320', 'max_size'='115180320' )
"""

sql """
alter table catalog_sales modify column cs_call_center_sk set stats ('row_count'='143997065', 'ndv'='30', 'min_value'='1', 'max_value'='30', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table household_demographics modify column hd_vehicle_count set stats ('row_count'='7200', 'ndv'='6', 'min_value'='-1', 'max_value'='4', 'avg_size'='28800', 'max_size'='28800' )
"""

sql """
alter table web_site modify column web_company_name set stats ('row_count'='24', 'ndv'='6', 'min_value'='able', 'max_value'='pri', 'avg_size'='97', 'max_size'='97' )
"""

sql """
alter table web_page modify column wp_web_page_id set stats ('row_count'='2040', 'ndv'='1019', 'min_value'='AAAAAAAAAABAAAAA', 'max_value'='AAAAAAAAPPEAAAAA', 'avg_size'='32640', 'max_size'='32640' )
"""

sql """
alter table store_sales modify column ss_sold_date_sk set stats ('row_count'='287997024', 'ndv'='1820', 'min_value'='2450816', 'max_value'='2452642', 'avg_size'='2303976192', 'max_size'='2303976192' )
"""

sql """
alter table customer_address modify column ca_street_type set stats ('row_count'='1000000', 'ndv'='21', 'min_value'='', 'max_value'='Wy', 'avg_size'='4073296', 'max_size'='4073296' )
"""

sql """
alter table web_sales modify column ws_ext_tax set stats ('row_count'='72001237', 'ndv'='2466', 'min_value'='0.00', 'max_value'='2682.90', 'avg_size'='288004948', 'max_size'='288004948' )
"""

sql """
alter table item modify column i_manufact_id set stats ('row_count'='204000', 'ndv'='1005', 'min_value'='1', 'max_value'='1000', 'avg_size'='816000', 'max_size'='816000' )
"""

sql """
alter table inventory modify column inv_quantity_on_hand set stats ('row_count'='399330000', 'ndv'='1006', 'min_value'='0', 'max_value'='1000', 'avg_size'='1597320000', 'max_size'='1597320000' )
"""

sql """
alter table call_center modify column cc_employees set stats ('row_count'='30', 'ndv'='22', 'min_value'='2935', 'max_value'='69020', 'avg_size'='120', 'max_size'='120' )
"""

sql """
alter table ship_mode modify column sm_carrier set stats ('row_count'='20', 'ndv'='20', 'min_value'='AIRBORNE', 'max_value'='ZOUROS', 'avg_size'='133', 'max_size'='133' )
"""

sql """
alter table store_returns modify column sr_reason_sk set stats ('row_count'='28795080', 'ndv'='55', 'min_value'='1', 'max_value'='55', 'avg_size'='230360640', 'max_size'='230360640' )
"""

sql """
alter table promotion modify column p_discount_active set stats ('row_count'='1000', 'ndv'='2', 'min_value'='', 'max_value'='N', 'avg_size'='981', 'max_size'='981' )
"""

sql """
alter table catalog_sales modify column cs_bill_hdemo_sk set stats ('row_count'='143997065', 'ndv'='7251', 'min_value'='1', 'max_value'='7200', 'avg_size'='1151976520', 'max_size'='1151976520' )
"""

sql """
alter table store_sales modify column ss_wholesale_cost set stats ('row_count'='287997024', 'ndv'='100', 'min_value'='1.00', 'max_value'='100.00', 'avg_size'='1151988096', 'max_size'='1151988096' )
"""


// sql """
// alter table date_dim modify column d_current_day set stats ('row_count'='366', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='366') partition (p2000);"""

// sql """
// alter table date_dim modify column d_current_day set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2003);"""

// sql """
// alter table date_dim modify column d_current_day set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1999);"""

// sql """
// alter table date_dim modify column d_current_day set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1998);"""

// sql """
// alter table date_dim modify column d_current_day set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2002);"""

// sql """
// alter table date_dim modify column d_current_day set stats ('row_count'='35065', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35065') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_current_day set stats ('row_count'='35793', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35793') partition (ppast);"""

// sql """
// alter table date_dim modify column d_current_day set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2001);"""

// sql """
// alter table date_dim modify column d_current_month set stats ('row_count'='35793', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35793') partition (ppast);"""

// sql """
// alter table date_dim modify column d_current_month set stats ('row_count'='35065', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35065') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_current_month set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1999);"""

// sql """
// alter table date_dim modify column d_current_month set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1998);"""

// sql """
// alter table date_dim modify column d_current_month set stats ('row_count'='366', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='366') partition (p2000);"""

// sql """
// alter table date_dim modify column d_current_month set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2003);"""

// sql """
// alter table date_dim modify column d_current_month set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2002);"""

// sql """
// alter table date_dim modify column d_current_month set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2001);"""

// sql """
// alter table date_dim modify column d_current_quarter set stats ('row_count'='366', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='366') partition (p2000);"""

// sql """
// alter table date_dim modify column d_current_quarter set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1999);"""

// sql """
// alter table date_dim modify column d_current_quarter set stats ('row_count'='35065', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35065') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_current_quarter set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2001);"""

// sql """
// alter table date_dim modify column d_current_quarter set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2002);"""

// sql """
// alter table date_dim modify column d_current_quarter set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2003);"""

// sql """
// alter table date_dim modify column d_current_quarter set stats ('row_count'='35793', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35793') partition (ppast);"""

// sql """
// alter table date_dim modify column d_current_quarter set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1998);"""

// sql """
// alter table date_dim modify column d_current_week set stats ('row_count'='35793', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35793') partition (ppast);"""

// sql """
// alter table date_dim modify column d_current_week set stats ('row_count'='35065', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35065') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_current_week set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2002);"""

// sql """
// alter table date_dim modify column d_current_week set stats ('row_count'='366', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='366') partition (p2000);"""

// sql """
// alter table date_dim modify column d_current_week set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2003);"""

// sql """
// alter table date_dim modify column d_current_week set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1999);"""

// sql """
// alter table date_dim modify column d_current_week set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1998);"""

// sql """
// alter table date_dim modify column d_current_week set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2001);"""

// sql """
// alter table date_dim modify column d_current_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1998);"""

// sql """
// alter table date_dim modify column d_current_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1999);"""

// sql """
// alter table date_dim modify column d_current_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='Y', 'max_value'='Y', 'data_size'='365') partition (p2003);"""

// sql """
// alter table date_dim modify column d_current_year set stats ('row_count'='366', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='366') partition (p2000);"""

// sql """
// alter table date_dim modify column d_current_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2001);"""

// sql """
// alter table date_dim modify column d_current_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2002);"""

// sql """
// alter table date_dim modify column d_current_year set stats ('row_count'='35793', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35793') partition (ppast);"""

// sql """
// alter table date_dim modify column d_current_year set stats ('row_count'='35065', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35065') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_date set stats ('row_count'='365', 'ndv'='366', 'num_nulls'='0', 'min_value'='2003-01-01', 'max_value'='2003-12-31', 'data_size'='1460') partition (p2003);"""

// sql """
// alter table date_dim modify column d_date set stats ('row_count'='365', 'ndv'='366', 'num_nulls'='0', 'min_value'='1999-01-01', 'max_value'='1999-12-31', 'data_size'='1460') partition (p1999);"""

// sql """
// alter table date_dim modify column d_date set stats ('row_count'='35793', 'ndv'='35630', 'num_nulls'='0', 'min_value'='1900-01-02', 'max_value'='1997-12-31', 'data_size'='143172') partition (ppast);"""

// sql """
// alter table date_dim modify column d_date set stats ('row_count'='365', 'ndv'='364', 'num_nulls'='0', 'min_value'='2002-01-01', 'max_value'='2002-12-31', 'data_size'='1460') partition (p2002);"""

// sql """
// alter table date_dim modify column d_date set stats ('row_count'='35065', 'ndv'='35118', 'num_nulls'='0', 'min_value'='2004-01-01', 'max_value'='2100-01-01', 'data_size'='140260') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_date set stats ('row_count'='365', 'ndv'='367', 'num_nulls'='0', 'min_value'='2001-01-01', 'max_value'='2001-12-31', 'data_size'='1460') partition (p2001);"""

// sql """
// alter table date_dim modify column d_date set stats ('row_count'='365', 'ndv'='366', 'num_nulls'='0', 'min_value'='1998-01-01', 'max_value'='1998-12-31', 'data_size'='1460') partition (p1998);"""

// sql """
// alter table date_dim modify column d_date set stats ('row_count'='366', 'ndv'='366', 'num_nulls'='0', 'min_value'='2000-01-01', 'max_value'='2000-12-31', 'data_size'='1464') partition (p2000);"""

// sql """
// alter table date_dim modify column d_date_id set stats ('row_count'='35793', 'ndv'='35428', 'num_nulls'='0', 'min_value'='AAAAAAAAAAAAFCAA', 'max_value'='AAAAAAAAPPPPECAA', 'data_size'='572688') partition (ppast);"""

// sql """
// alter table date_dim modify column d_date_id set stats ('row_count'='365', 'ndv'='368', 'num_nulls'='0', 'min_value'='AAAAAAAAAAGGFCAA', 'max_value'='AAAAAAAAPPFGFCAA', 'data_size'='5840') partition (p1998);"""

// sql """
// alter table date_dim modify column d_date_id set stats ('row_count'='365', 'ndv'='364', 'num_nulls'='0', 'min_value'='AAAAAAAAAAHGFCAA', 'max_value'='AAAAAAAAPPHGFCAA', 'data_size'='5840') partition (p1999);"""

// sql """
// alter table date_dim modify column d_date_id set stats ('row_count'='365', 'ndv'='367', 'num_nulls'='0', 'min_value'='AAAAAAAAAANGFCAA', 'max_value'='AAAAAAAAPPNGFCAA', 'data_size'='5840') partition (p2003);"""

// sql """
// alter table date_dim modify column d_date_id set stats ('row_count'='365', 'ndv'='366', 'num_nulls'='0', 'min_value'='AAAAAAAAAAKGFCAA', 'max_value'='AAAAAAAAPPKGFCAA', 'data_size'='5840') partition (p2001);"""

// sql """
// alter table date_dim modify column d_date_id set stats ('row_count'='35065', 'ndv'='35542', 'num_nulls'='0', 'min_value'='AAAAAAAAAAAHFCAA', 'max_value'='AAAAAAAAPPPOFCAA', 'data_size'='561040') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_date_id set stats ('row_count'='365', 'ndv'='364', 'num_nulls'='0', 'min_value'='AAAAAAAAAAMGFCAA', 'max_value'='AAAAAAAAPPLGFCAA', 'data_size'='5840') partition (p2002);"""

// sql """
// alter table date_dim modify column d_date_id set stats ('row_count'='366', 'ndv'='362', 'num_nulls'='0', 'min_value'='AAAAAAAAAAJGFCAA', 'max_value'='AAAAAAAAPPIGFCAA', 'data_size'='5856') partition (p2000);"""

// sql """
// alter table date_dim modify column d_date_sk set stats ('row_count'='365', 'ndv'='367', 'num_nulls'='0', 'min_value'='2450815', 'max_value'='2451179', 'data_size'='2920') partition (p1998);"""

// sql """
// alter table date_dim modify column d_date_sk set stats ('row_count'='365', 'ndv'='366', 'num_nulls'='0', 'min_value'='2451180', 'max_value'='2451544', 'data_size'='2920') partition (p1999);"""

// sql """
// alter table date_dim modify column d_date_sk set stats ('row_count'='365', 'ndv'='367', 'num_nulls'='0', 'min_value'='2451911', 'max_value'='2452275', 'data_size'='2920') partition (p2001);"""

// sql """
// alter table date_dim modify column d_date_sk set stats ('row_count'='366', 'ndv'='367', 'num_nulls'='0', 'min_value'='2451545', 'max_value'='2451910', 'data_size'='2928') partition (p2000);"""

// sql """
// alter table date_dim modify column d_date_sk set stats ('row_count'='35065', 'ndv'='35067', 'num_nulls'='0', 'min_value'='2453006', 'max_value'='2488070', 'data_size'='280520') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_date_sk set stats ('row_count'='365', 'ndv'='365', 'num_nulls'='0', 'min_value'='2452276', 'max_value'='2452640', 'data_size'='2920') partition (p2002);"""

// sql """
// alter table date_dim modify column d_date_sk set stats ('row_count'='35793', 'ndv'='36266', 'num_nulls'='0', 'min_value'='2415022', 'max_value'='2450814', 'data_size'='286344') partition (ppast);"""

// sql """
// alter table date_dim modify column d_date_sk set stats ('row_count'='365', 'ndv'='365', 'num_nulls'='0', 'min_value'='2452641', 'max_value'='2453005', 'data_size'='2920') partition (p2003);"""

// sql """
// alter table date_dim modify column d_day_name set stats ('row_count'='366', 'ndv'='7', 'num_nulls'='0', 'min_value'='Friday', 'max_value'='Wednesday', 'data_size'='2614') partition (p2000);"""

// sql """
// alter table date_dim modify column d_day_name set stats ('row_count'='35065', 'ndv'='7', 'num_nulls'='0', 'min_value'='Friday', 'max_value'='Wednesday', 'data_size'='250466') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_day_name set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='Friday', 'max_value'='Wednesday', 'data_size'='2608') partition (p1998);"""

// sql """
// alter table date_dim modify column d_day_name set stats ('row_count'='35793', 'ndv'='7', 'num_nulls'='0', 'min_value'='Friday', 'max_value'='Wednesday', 'data_size'='255663') partition (ppast);"""

// sql """
// alter table date_dim modify column d_day_name set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='Friday', 'max_value'='Wednesday', 'data_size'='2607') partition (p2002);"""

// sql """
// alter table date_dim modify column d_day_name set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='Friday', 'max_value'='Wednesday', 'data_size'='2606') partition (p1999);"""

// sql """
// alter table date_dim modify column d_day_name set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='Friday', 'max_value'='Wednesday', 'data_size'='2606') partition (p2001);"""

// sql """
// alter table date_dim modify column d_day_name set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='Friday', 'max_value'='Wednesday', 'data_size'='2609') partition (p2003);"""

// sql """
// alter table date_dim modify column d_dom set stats ('row_count'='365', 'ndv'='31', 'num_nulls'='0', 'min_value'='1', 'max_value'='31', 'data_size'='1460') partition (p2002);"""

// sql """
// alter table date_dim modify column d_dom set stats ('row_count'='365', 'ndv'='31', 'num_nulls'='0', 'min_value'='1', 'max_value'='31', 'data_size'='1460') partition (p2003);"""

// sql """
// alter table date_dim modify column d_dom set stats ('row_count'='35793', 'ndv'='31', 'num_nulls'='0', 'min_value'='1', 'max_value'='31', 'data_size'='143172') partition (ppast);"""

// sql """
// alter table date_dim modify column d_dom set stats ('row_count'='35065', 'ndv'='31', 'num_nulls'='0', 'min_value'='1', 'max_value'='31', 'data_size'='140260') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_dom set stats ('row_count'='365', 'ndv'='31', 'num_nulls'='0', 'min_value'='1', 'max_value'='31', 'data_size'='1460') partition (p1998);"""

// sql """
// alter table date_dim modify column d_dom set stats ('row_count'='365', 'ndv'='31', 'num_nulls'='0', 'min_value'='1', 'max_value'='31', 'data_size'='1460') partition (p1999);"""

// sql """
// alter table date_dim modify column d_dom set stats ('row_count'='366', 'ndv'='31', 'num_nulls'='0', 'min_value'='1', 'max_value'='31', 'data_size'='1464') partition (p2000);"""

// sql """
// alter table date_dim modify column d_dom set stats ('row_count'='365', 'ndv'='31', 'num_nulls'='0', 'min_value'='1', 'max_value'='31', 'data_size'='1460') partition (p2001);"""

// sql """
// alter table date_dim modify column d_dow set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='0', 'max_value'='6', 'data_size'='1460') partition (p1998);"""

// sql """
// alter table date_dim modify column d_dow set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='0', 'max_value'='6', 'data_size'='1460') partition (p2003);"""

// sql """
// alter table date_dim modify column d_dow set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='0', 'max_value'='6', 'data_size'='1460') partition (p2001);"""

// sql """
// alter table date_dim modify column d_dow set stats ('row_count'='35793', 'ndv'='7', 'num_nulls'='0', 'min_value'='0', 'max_value'='6', 'data_size'='143172') partition (ppast);"""

// sql """
// alter table date_dim modify column d_dow set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='0', 'max_value'='6', 'data_size'='1460') partition (p2002);"""

// sql """
// alter table date_dim modify column d_dow set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='0', 'max_value'='6', 'data_size'='1460') partition (p1999);"""

// sql """
// alter table date_dim modify column d_dow set stats ('row_count'='366', 'ndv'='7', 'num_nulls'='0', 'min_value'='0', 'max_value'='6', 'data_size'='1464') partition (p2000);"""

// sql """
// alter table date_dim modify column d_dow set stats ('row_count'='35065', 'ndv'='7', 'num_nulls'='0', 'min_value'='0', 'max_value'='6', 'data_size'='140260') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_first_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2451911', 'max_value'='2452245', 'data_size'='1460') partition (p2001);"""

// sql """
// alter table date_dim modify column d_first_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2451180', 'max_value'='2451514', 'data_size'='1460') partition (p1999);"""

// sql """
// alter table date_dim modify column d_first_dom set stats ('row_count'='35793', 'ndv'='1181', 'num_nulls'='0', 'min_value'='2415021', 'max_value'='2450784', 'data_size'='143172') partition (ppast);"""

// sql """
// alter table date_dim modify column d_first_dom set stats ('row_count'='366', 'ndv'='12', 'num_nulls'='0', 'min_value'='2451545', 'max_value'='2451880', 'data_size'='1464') partition (p2000);"""

// sql """
// alter table date_dim modify column d_first_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2452641', 'max_value'='2452975', 'data_size'='1460') partition (p2003);"""

// sql """
// alter table date_dim modify column d_first_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2452276', 'max_value'='2452610', 'data_size'='1460') partition (p2002);"""

// sql """
// alter table date_dim modify column d_first_dom set stats ('row_count'='35065', 'ndv'='1161', 'num_nulls'='0', 'min_value'='2453006', 'max_value'='2488070', 'data_size'='140260') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_first_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2450815', 'max_value'='2451149', 'data_size'='1460') partition (p1998);"""

// sql """
// alter table date_dim modify column d_following_holiday set stats ('row_count'='35065', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='35065') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_following_holiday set stats ('row_count'='35793', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='35793') partition (ppast);"""

// sql """
// alter table date_dim modify column d_following_holiday set stats ('row_count'='366', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='366') partition (p2000);"""

// sql """
// alter table date_dim modify column d_following_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p1998);"""

// sql """
// alter table date_dim modify column d_following_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2002);"""

// sql """
// alter table date_dim modify column d_following_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p1999);"""

// sql """
// alter table date_dim modify column d_following_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2003);"""

// sql """
// alter table date_dim modify column d_following_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2001);"""

// sql """
// alter table date_dim modify column d_fy_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='413', 'max_value'='417', 'data_size'='1460') partition (p2003);"""

// sql """
// alter table date_dim modify column d_fy_quarter_seq set stats ('row_count'='366', 'ndv'='5', 'num_nulls'='0', 'min_value'='401', 'max_value'='405', 'data_size'='1464') partition (p2000);"""

// sql """
// alter table date_dim modify column d_fy_quarter_seq set stats ('row_count'='35793', 'ndv'='394', 'num_nulls'='0', 'min_value'='1', 'max_value'='393', 'data_size'='143172') partition (ppast);"""

// sql """
// alter table date_dim modify column d_fy_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='393', 'max_value'='397', 'data_size'='1460') partition (p1998);"""

// sql """
// alter table date_dim modify column d_fy_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='405', 'max_value'='409', 'data_size'='1460') partition (p2001);"""

// sql """
// alter table date_dim modify column d_fy_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='397', 'max_value'='401', 'data_size'='1460') partition (p1999);"""

// sql """
// alter table date_dim modify column d_fy_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='409', 'max_value'='413', 'data_size'='1460') partition (p2002);"""

// sql """
// alter table date_dim modify column d_fy_quarter_seq set stats ('row_count'='35065', 'ndv'='387', 'num_nulls'='0', 'min_value'='417', 'max_value'='801', 'data_size'='140260') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_fy_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5375', 'max_value'='5427', 'data_size'='1460') partition (p2003);"""

// sql """
// alter table date_dim modify column d_fy_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5270', 'max_value'='5322', 'data_size'='1460') partition (p2001);"""

// sql """
// alter table date_dim modify column d_fy_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5323', 'max_value'='5375', 'data_size'='1460') partition (p2002);"""

// sql """
// alter table date_dim modify column d_fy_week_seq set stats ('row_count'='35793', 'ndv'='5136', 'num_nulls'='0', 'min_value'='1', 'max_value'='5114', 'data_size'='143172') partition (ppast);"""

// sql """
// alter table date_dim modify column d_fy_week_seq set stats ('row_count'='35065', 'ndv'='5008', 'num_nulls'='0', 'min_value'='5427', 'max_value'='10436', 'data_size'='140260') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_fy_week_seq set stats ('row_count'='366', 'ndv'='53', 'num_nulls'='0', 'min_value'='5218', 'max_value'='5270', 'data_size'='1464') partition (p2000);"""

// sql """
// alter table date_dim modify column d_fy_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5114', 'max_value'='5166', 'data_size'='1460') partition (p1998);"""

// sql """
// alter table date_dim modify column d_fy_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5166', 'max_value'='5218', 'data_size'='1460') partition (p1999);"""

// sql """
// alter table date_dim modify column d_fy_year set stats ('row_count'='366', 'ndv'='1', 'num_nulls'='0', 'min_value'='2000', 'max_value'='2000', 'data_size'='1464') partition (p2000);"""

// sql """
// alter table date_dim modify column d_fy_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='1998', 'max_value'='1998', 'data_size'='1460') partition (p1998);"""

// sql """
// alter table date_dim modify column d_fy_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='2002', 'max_value'='2002', 'data_size'='1460') partition (p2002);"""

// sql """
// alter table date_dim modify column d_fy_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='1999', 'max_value'='1999', 'data_size'='1460') partition (p1999);"""

// sql """
// alter table date_dim modify column d_fy_year set stats ('row_count'='35065', 'ndv'='97', 'num_nulls'='0', 'min_value'='2004', 'max_value'='2100', 'data_size'='140260') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_fy_year set stats ('row_count'='35793', 'ndv'='98', 'num_nulls'='0', 'min_value'='1900', 'max_value'='1997', 'data_size'='143172') partition (ppast);"""

// sql """
// alter table date_dim modify column d_fy_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='2001', 'max_value'='2001', 'data_size'='1460') partition (p2001);"""

// sql """
// alter table date_dim modify column d_fy_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='2003', 'max_value'='2003', 'data_size'='1460') partition (p2003);"""

// sql """
// alter table date_dim modify column d_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2002);"""

// sql """
// alter table date_dim modify column d_holiday set stats ('row_count'='35065', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='35065') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p1998);"""

// sql """
// alter table date_dim modify column d_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2001);"""

// sql """
// alter table date_dim modify column d_holiday set stats ('row_count'='366', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='366') partition (p2000);"""

// sql """
// alter table date_dim modify column d_holiday set stats ('row_count'='35793', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='35793') partition (ppast);"""

// sql """
// alter table date_dim modify column d_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2003);"""

// sql """
// alter table date_dim modify column d_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p1999);"""

// sql """
// alter table date_dim modify column d_last_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2452275', 'max_value'='2452943', 'data_size'='1460') partition (p2002);"""

// sql """
// alter table date_dim modify column d_last_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2451910', 'max_value'='2452578', 'data_size'='1460') partition (p2001);"""

// sql """
// alter table date_dim modify column d_last_dom set stats ('row_count'='35793', 'ndv'='1186', 'num_nulls'='0', 'min_value'='2415020', 'max_value'='2451117', 'data_size'='143172') partition (ppast);"""

// sql """
// alter table date_dim modify column d_last_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2452640', 'max_value'='2453308', 'data_size'='1460') partition (p2003);"""

// sql """
// alter table date_dim modify column d_last_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2451179', 'max_value'='2451847', 'data_size'='1460') partition (p1999);"""

// sql """
// alter table date_dim modify column d_last_dom set stats ('row_count'='35065', 'ndv'='1144', 'num_nulls'='0', 'min_value'='2453005', 'max_value'='2488372', 'data_size'='140260') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_last_dom set stats ('row_count'='366', 'ndv'='12', 'num_nulls'='0', 'min_value'='2451544', 'max_value'='2452214', 'data_size'='1464') partition (p2000);"""

// sql """
// alter table date_dim modify column d_last_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2450814', 'max_value'='2451482', 'data_size'='1460') partition (p1998);"""

// sql """
// alter table date_dim modify column d_month_seq set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1188', 'max_value'='1199', 'data_size'='1460') partition (p1999);"""

// sql """
// alter table date_dim modify column d_month_seq set stats ('row_count'='35793', 'ndv'='1176', 'num_nulls'='0', 'min_value'='0', 'max_value'='1175', 'data_size'='143172') partition (ppast);"""

// sql """
// alter table date_dim modify column d_month_seq set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1176', 'max_value'='1187', 'data_size'='1460') partition (p1998);"""

// sql """
// alter table date_dim modify column d_month_seq set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1236', 'max_value'='1247', 'data_size'='1460') partition (p2003);"""

// sql """
// alter table date_dim modify column d_month_seq set stats ('row_count'='35065', 'ndv'='1147', 'num_nulls'='0', 'min_value'='1248', 'max_value'='2400', 'data_size'='140260') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_month_seq set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1212', 'max_value'='1223', 'data_size'='1460') partition (p2001);"""

// sql """
// alter table date_dim modify column d_month_seq set stats ('row_count'='366', 'ndv'='12', 'num_nulls'='0', 'min_value'='1200', 'max_value'='1211', 'data_size'='1464') partition (p2000);"""

// sql """
// alter table date_dim modify column d_month_seq set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1224', 'max_value'='1235', 'data_size'='1460') partition (p2002);"""

// sql """
// alter table date_dim modify column d_moy set stats ('row_count'='35793', 'ndv'='12', 'num_nulls'='0', 'min_value'='1', 'max_value'='12', 'data_size'='143172') partition (ppast);"""

// sql """
// alter table date_dim modify column d_moy set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1', 'max_value'='12', 'data_size'='1460') partition (p2001);"""

// sql """
// alter table date_dim modify column d_moy set stats ('row_count'='35065', 'ndv'='12', 'num_nulls'='0', 'min_value'='1', 'max_value'='12', 'data_size'='140260') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_moy set stats ('row_count'='366', 'ndv'='12', 'num_nulls'='0', 'min_value'='1', 'max_value'='12', 'data_size'='1464') partition (p2000);"""

// sql """
// alter table date_dim modify column d_moy set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1', 'max_value'='12', 'data_size'='1460') partition (p1998);"""

// sql """
// alter table date_dim modify column d_moy set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1', 'max_value'='12', 'data_size'='1460') partition (p2002);"""

// sql """
// alter table date_dim modify column d_moy set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1', 'max_value'='12', 'data_size'='1460') partition (p1999);"""

// sql """
// alter table date_dim modify column d_moy set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1', 'max_value'='12', 'data_size'='1460') partition (p2003);"""

// sql """
// alter table date_dim modify column d_qoy set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='4', 'data_size'='1460') partition (p2001);"""

// sql """
// alter table date_dim modify column d_qoy set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='4', 'data_size'='1460') partition (p1998);"""

// sql """
// alter table date_dim modify column d_qoy set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='4', 'data_size'='1460') partition (p2002);"""

// sql """
// alter table date_dim modify column d_qoy set stats ('row_count'='366', 'ndv'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='4', 'data_size'='1464') partition (p2000);"""

// sql """
// alter table date_dim modify column d_qoy set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='4', 'data_size'='1460') partition (p1999);"""

// sql """
// alter table date_dim modify column d_qoy set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='4', 'data_size'='1460') partition (p2003);"""

// sql """
// alter table date_dim modify column d_qoy set stats ('row_count'='35793', 'ndv'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='4', 'data_size'='143172') partition (ppast);"""

// sql """
// alter table date_dim modify column d_qoy set stats ('row_count'='35065', 'ndv'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='4', 'data_size'='140260') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_quarter_name set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='2001Q1', 'max_value'='2001Q4', 'data_size'='2190') partition (p2001);"""

// sql """
// alter table date_dim modify column d_quarter_name set stats ('row_count'='35793', 'ndv'='393', 'num_nulls'='0', 'min_value'='1900Q1', 'max_value'='1997Q4', 'data_size'='214758') partition (ppast);"""

// sql """
// alter table date_dim modify column d_quarter_name set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='2002Q1', 'max_value'='2002Q4', 'data_size'='2190') partition (p2002);"""

// sql """
// alter table date_dim modify column d_quarter_name set stats ('row_count'='366', 'ndv'='4', 'num_nulls'='0', 'min_value'='2000Q1', 'max_value'='2000Q4', 'data_size'='2196') partition (p2000);"""

// sql """
// alter table date_dim modify column d_quarter_name set stats ('row_count'='35065', 'ndv'='387', 'num_nulls'='0', 'min_value'='2004Q1', 'max_value'='2100Q1', 'data_size'='210390') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_quarter_name set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='2003Q1', 'max_value'='2003Q4', 'data_size'='2190') partition (p2003);"""

// sql """
// alter table date_dim modify column d_quarter_name set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='1998Q1', 'max_value'='1998Q4', 'data_size'='2190') partition (p1998);"""

// sql """
// alter table date_dim modify column d_quarter_name set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='1999Q1', 'max_value'='1999Q4', 'data_size'='2190') partition (p1999);"""

// sql """
// alter table date_dim modify column d_quarter_seq set stats ('row_count'='35793', 'ndv'='394', 'num_nulls'='0', 'min_value'='1', 'max_value'='393', 'data_size'='143172') partition (ppast);"""

// sql """
// alter table date_dim modify column d_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='405', 'max_value'='409', 'data_size'='1460') partition (p2001);"""

// sql """
// alter table date_dim modify column d_quarter_seq set stats ('row_count'='366', 'ndv'='5', 'num_nulls'='0', 'min_value'='401', 'max_value'='405', 'data_size'='1464') partition (p2000);"""

// sql """
// alter table date_dim modify column d_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='397', 'max_value'='401', 'data_size'='1460') partition (p1999);"""

// sql """
// alter table date_dim modify column d_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='409', 'max_value'='413', 'data_size'='1460') partition (p2002);"""

// sql """
// alter table date_dim modify column d_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='393', 'max_value'='397', 'data_size'='1460') partition (p1998);"""

// sql """
// alter table date_dim modify column d_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='413', 'max_value'='417', 'data_size'='1460') partition (p2003);"""

// sql """
// alter table date_dim modify column d_quarter_seq set stats ('row_count'='35065', 'ndv'='387', 'num_nulls'='0', 'min_value'='417', 'max_value'='801', 'data_size'='140260') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_same_day_lq set stats ('row_count'='35793', 'ndv'='35806', 'num_nulls'='0', 'min_value'='2414930', 'max_value'='2450722', 'data_size'='143172') partition (ppast);"""

// sql """
// alter table date_dim modify column d_same_day_lq set stats ('row_count'='365', 'ndv'='362', 'num_nulls'='0', 'min_value'='2451088', 'max_value'='2451452', 'data_size'='1460') partition (p1999);"""

// sql """
// alter table date_dim modify column d_same_day_lq set stats ('row_count'='365', 'ndv'='362', 'num_nulls'='0', 'min_value'='2451819', 'max_value'='2452183', 'data_size'='1460') partition (p2001);"""

// sql """
// alter table date_dim modify column d_same_day_lq set stats ('row_count'='365', 'ndv'='361', 'num_nulls'='0', 'min_value'='2450723', 'max_value'='2451087', 'data_size'='1460') partition (p1998);"""

// sql """
// alter table date_dim modify column d_same_day_lq set stats ('row_count'='365', 'ndv'='364', 'num_nulls'='0', 'min_value'='2452184', 'max_value'='2452548', 'data_size'='1460') partition (p2002);"""

// sql """
// alter table date_dim modify column d_same_day_lq set stats ('row_count'='365', 'ndv'='362', 'num_nulls'='0', 'min_value'='2452549', 'max_value'='2452913', 'data_size'='1460') partition (p2003);"""

// sql """
// alter table date_dim modify column d_same_day_lq set stats ('row_count'='35065', 'ndv'='34991', 'num_nulls'='0', 'min_value'='2452914', 'max_value'='2487978', 'data_size'='140260') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_same_day_lq set stats ('row_count'='366', 'ndv'='365', 'num_nulls'='0', 'min_value'='2451453', 'max_value'='2451818', 'data_size'='1464') partition (p2000);"""

// sql """
// alter table date_dim modify column d_same_day_ly set stats ('row_count'='365', 'ndv'='364', 'num_nulls'='0', 'min_value'='2452276', 'max_value'='2452640', 'data_size'='1460') partition (p2003);"""

// sql """
// alter table date_dim modify column d_same_day_ly set stats ('row_count'='35793', 'ndv'='35878', 'num_nulls'='0', 'min_value'='2414657', 'max_value'='2450449', 'data_size'='143172') partition (ppast);"""

// sql """
// alter table date_dim modify column d_same_day_ly set stats ('row_count'='366', 'ndv'='363', 'num_nulls'='0', 'min_value'='2451180', 'max_value'='2451544', 'data_size'='1464') partition (p2000);"""

// sql """
// alter table date_dim modify column d_same_day_ly set stats ('row_count'='365', 'ndv'='363', 'num_nulls'='0', 'min_value'='2451545', 'max_value'='2451910', 'data_size'='1460') partition (p2001);"""

// sql """
// alter table date_dim modify column d_same_day_ly set stats ('row_count'='365', 'ndv'='364', 'num_nulls'='0', 'min_value'='2450450', 'max_value'='2450814', 'data_size'='1460') partition (p1998);"""

// sql """
// alter table date_dim modify column d_same_day_ly set stats ('row_count'='35065', 'ndv'='35076', 'num_nulls'='0', 'min_value'='2452641', 'max_value'='2487705', 'data_size'='140260') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_same_day_ly set stats ('row_count'='365', 'ndv'='363', 'num_nulls'='0', 'min_value'='2450815', 'max_value'='2451179', 'data_size'='1460') partition (p1999);"""

// sql """
// alter table date_dim modify column d_same_day_ly set stats ('row_count'='365', 'ndv'='367', 'num_nulls'='0', 'min_value'='2451911', 'max_value'='2452275', 'data_size'='1460') partition (p2002);"""

// sql """
// alter table date_dim modify column d_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5270', 'max_value'='5322', 'data_size'='1460') partition (p2001);"""

// sql """
// alter table date_dim modify column d_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5114', 'max_value'='5166', 'data_size'='1460') partition (p1998);"""

// sql """
// alter table date_dim modify column d_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5166', 'max_value'='5218', 'data_size'='1460') partition (p1999);"""

// sql """
// alter table date_dim modify column d_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5375', 'max_value'='5427', 'data_size'='1460') partition (p2003);"""

// sql """
// alter table date_dim modify column d_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5323', 'max_value'='5375', 'data_size'='1460') partition (p2002);"""

// sql """
// alter table date_dim modify column d_week_seq set stats ('row_count'='35065', 'ndv'='5008', 'num_nulls'='0', 'min_value'='5427', 'max_value'='10436', 'data_size'='140260') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_week_seq set stats ('row_count'='35793', 'ndv'='5136', 'num_nulls'='0', 'min_value'='1', 'max_value'='5114', 'data_size'='143172') partition (ppast);"""

// sql """
// alter table date_dim modify column d_week_seq set stats ('row_count'='366', 'ndv'='53', 'num_nulls'='0', 'min_value'='5218', 'max_value'='5270', 'data_size'='1464') partition (p2000);"""

// sql """
// alter table date_dim modify column d_weekend set stats ('row_count'='366', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='366') partition (p2000);"""

// sql """
// alter table date_dim modify column d_weekend set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2002);"""

// sql """
// alter table date_dim modify column d_weekend set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2001);"""

// sql """
// alter table date_dim modify column d_weekend set stats ('row_count'='35793', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='35793') partition (ppast);"""

// sql """
// alter table date_dim modify column d_weekend set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2003);"""

// sql """
// alter table date_dim modify column d_weekend set stats ('row_count'='35065', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='35065') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_weekend set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p1998);"""

// sql """
// alter table date_dim modify column d_weekend set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p1999);"""

// sql """
// alter table date_dim modify column d_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='1999', 'max_value'='1999', 'data_size'='1460') partition (p1999);"""

// sql """
// alter table date_dim modify column d_year set stats ('row_count'='35793', 'ndv'='98', 'num_nulls'='0', 'min_value'='1900', 'max_value'='1997', 'data_size'='143172') partition (ppast);"""

// sql """
// alter table date_dim modify column d_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='1998', 'max_value'='1998', 'data_size'='1460') partition (p1998);"""

// sql """
// alter table date_dim modify column d_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='2001', 'max_value'='2001', 'data_size'='1460') partition (p2001);"""

// sql """
// alter table date_dim modify column d_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='2002', 'max_value'='2002', 'data_size'='1460') partition (p2002);"""

// sql """
// alter table date_dim modify column d_year set stats ('row_count'='366', 'ndv'='1', 'num_nulls'='0', 'min_value'='2000', 'max_value'='2000', 'data_size'='1464') partition (p2000);"""

// sql """
// alter table date_dim modify column d_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='2003', 'max_value'='2003', 'data_size'='1460') partition (p2003);"""

// sql """
// alter table date_dim modify column d_year set stats ('row_count'='35065', 'ndv'='97', 'num_nulls'='0', 'min_value'='2004', 'max_value'='2100', 'data_size'='140260') partition (pfuture);"""

// sql """
// alter table date_dim modify column d_current_day set stats ('row_count'='366', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='366') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_current_day set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_current_day set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_current_day set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_current_day set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_current_day set stats ('row_count'='35065', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35065') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_current_day set stats ('row_count'='35793', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35793') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_current_day set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_current_month set stats ('row_count'='35793', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35793') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_current_month set stats ('row_count'='35065', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35065') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_current_month set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_current_month set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_current_month set stats ('row_count'='366', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='366') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_current_month set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_current_month set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_current_month set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_current_quarter set stats ('row_count'='366', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='366') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_current_quarter set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_current_quarter set stats ('row_count'='35065', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35065') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_current_quarter set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_current_quarter set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_current_quarter set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_current_quarter set stats ('row_count'='35793', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35793') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_current_quarter set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_current_week set stats ('row_count'='35793', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35793') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_current_week set stats ('row_count'='35065', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35065') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_current_week set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_current_week set stats ('row_count'='366', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='366') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_current_week set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_current_week set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_current_week set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_current_week set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_current_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_current_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_current_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='Y', 'max_value'='Y', 'data_size'='365') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_current_year set stats ('row_count'='366', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='366') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_current_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_current_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='365') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_current_year set stats ('row_count'='35793', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35793') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_current_year set stats ('row_count'='35065', 'ndv'='1', 'num_nulls'='0', 'min_value'='N', 'max_value'='N', 'data_size'='35065') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_date set stats ('row_count'='365', 'ndv'='366', 'num_nulls'='0', 'min_value'='2003-01-01', 'max_value'='2003-12-31', 'data_size'='1460') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_date set stats ('row_count'='365', 'ndv'='366', 'num_nulls'='0', 'min_value'='1999-01-01', 'max_value'='1999-12-31', 'data_size'='1460') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_date set stats ('row_count'='35793', 'ndv'='35630', 'num_nulls'='0', 'min_value'='1900-01-02', 'max_value'='1997-12-31', 'data_size'='143172') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_date set stats ('row_count'='365', 'ndv'='364', 'num_nulls'='0', 'min_value'='2002-01-01', 'max_value'='2002-12-31', 'data_size'='1460') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_date set stats ('row_count'='35065', 'ndv'='35118', 'num_nulls'='0', 'min_value'='2004-01-01', 'max_value'='2100-01-01', 'data_size'='140260') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_date set stats ('row_count'='365', 'ndv'='367', 'num_nulls'='0', 'min_value'='2001-01-01', 'max_value'='2001-12-31', 'data_size'='1460') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_date set stats ('row_count'='365', 'ndv'='366', 'num_nulls'='0', 'min_value'='1998-01-01', 'max_value'='1998-12-31', 'data_size'='1460') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_date set stats ('row_count'='366', 'ndv'='366', 'num_nulls'='0', 'min_value'='2000-01-01', 'max_value'='2000-12-31', 'data_size'='1464') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_date_id set stats ('row_count'='35793', 'ndv'='35428', 'num_nulls'='0', 'min_value'='AAAAAAAAAAAAFCAA', 'max_value'='AAAAAAAAPPPPECAA', 'data_size'='572688') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_date_id set stats ('row_count'='365', 'ndv'='368', 'num_nulls'='0', 'min_value'='AAAAAAAAAAGGFCAA', 'max_value'='AAAAAAAAPPFGFCAA', 'data_size'='5840') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_date_id set stats ('row_count'='365', 'ndv'='364', 'num_nulls'='0', 'min_value'='AAAAAAAAAAHGFCAA', 'max_value'='AAAAAAAAPPHGFCAA', 'data_size'='5840') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_date_id set stats ('row_count'='365', 'ndv'='367', 'num_nulls'='0', 'min_value'='AAAAAAAAAANGFCAA', 'max_value'='AAAAAAAAPPNGFCAA', 'data_size'='5840') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_date_id set stats ('row_count'='365', 'ndv'='366', 'num_nulls'='0', 'min_value'='AAAAAAAAAAKGFCAA', 'max_value'='AAAAAAAAPPKGFCAA', 'data_size'='5840') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_date_id set stats ('row_count'='35065', 'ndv'='35542', 'num_nulls'='0', 'min_value'='AAAAAAAAAAAHFCAA', 'max_value'='AAAAAAAAPPPOFCAA', 'data_size'='561040') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_date_id set stats ('row_count'='365', 'ndv'='364', 'num_nulls'='0', 'min_value'='AAAAAAAAAAMGFCAA', 'max_value'='AAAAAAAAPPLGFCAA', 'data_size'='5840') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_date_id set stats ('row_count'='366', 'ndv'='362', 'num_nulls'='0', 'min_value'='AAAAAAAAAAJGFCAA', 'max_value'='AAAAAAAAPPIGFCAA', 'data_size'='5856') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_date_sk set stats ('row_count'='365', 'ndv'='367', 'num_nulls'='0', 'min_value'='2450815', 'max_value'='2451179', 'data_size'='2920') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_date_sk set stats ('row_count'='365', 'ndv'='366', 'num_nulls'='0', 'min_value'='2451180', 'max_value'='2451544', 'data_size'='2920') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_date_sk set stats ('row_count'='365', 'ndv'='367', 'num_nulls'='0', 'min_value'='2451911', 'max_value'='2452275', 'data_size'='2920') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_date_sk set stats ('row_count'='366', 'ndv'='367', 'num_nulls'='0', 'min_value'='2451545', 'max_value'='2451910', 'data_size'='2928') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_date_sk set stats ('row_count'='35065', 'ndv'='35067', 'num_nulls'='0', 'min_value'='2453006', 'max_value'='2488070', 'data_size'='280520') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_date_sk set stats ('row_count'='365', 'ndv'='365', 'num_nulls'='0', 'min_value'='2452276', 'max_value'='2452640', 'data_size'='2920') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_date_sk set stats ('row_count'='35793', 'ndv'='36266', 'num_nulls'='0', 'min_value'='2415022', 'max_value'='2450814', 'data_size'='286344') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_date_sk set stats ('row_count'='365', 'ndv'='365', 'num_nulls'='0', 'min_value'='2452641', 'max_value'='2453005', 'data_size'='2920') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_day_name set stats ('row_count'='366', 'ndv'='7', 'num_nulls'='0', 'min_value'='Friday', 'max_value'='Wednesday', 'data_size'='2614') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_day_name set stats ('row_count'='35065', 'ndv'='7', 'num_nulls'='0', 'min_value'='Friday', 'max_value'='Wednesday', 'data_size'='250466') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_day_name set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='Friday', 'max_value'='Wednesday', 'data_size'='2608') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_day_name set stats ('row_count'='35793', 'ndv'='7', 'num_nulls'='0', 'min_value'='Friday', 'max_value'='Wednesday', 'data_size'='255663') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_day_name set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='Friday', 'max_value'='Wednesday', 'data_size'='2607') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_day_name set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='Friday', 'max_value'='Wednesday', 'data_size'='2606') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_day_name set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='Friday', 'max_value'='Wednesday', 'data_size'='2606') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_day_name set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='Friday', 'max_value'='Wednesday', 'data_size'='2609') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_dom set stats ('row_count'='365', 'ndv'='31', 'num_nulls'='0', 'min_value'='1', 'max_value'='31', 'data_size'='1460') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_dom set stats ('row_count'='365', 'ndv'='31', 'num_nulls'='0', 'min_value'='1', 'max_value'='31', 'data_size'='1460') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_dom set stats ('row_count'='35793', 'ndv'='31', 'num_nulls'='0', 'min_value'='1', 'max_value'='31', 'data_size'='143172') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_dom set stats ('row_count'='35065', 'ndv'='31', 'num_nulls'='0', 'min_value'='1', 'max_value'='31', 'data_size'='140260') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_dom set stats ('row_count'='365', 'ndv'='31', 'num_nulls'='0', 'min_value'='1', 'max_value'='31', 'data_size'='1460') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_dom set stats ('row_count'='365', 'ndv'='31', 'num_nulls'='0', 'min_value'='1', 'max_value'='31', 'data_size'='1460') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_dom set stats ('row_count'='366', 'ndv'='31', 'num_nulls'='0', 'min_value'='1', 'max_value'='31', 'data_size'='1464') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_dom set stats ('row_count'='365', 'ndv'='31', 'num_nulls'='0', 'min_value'='1', 'max_value'='31', 'data_size'='1460') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_dow set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='0', 'max_value'='6', 'data_size'='1460') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_dow set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='0', 'max_value'='6', 'data_size'='1460') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_dow set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='0', 'max_value'='6', 'data_size'='1460') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_dow set stats ('row_count'='35793', 'ndv'='7', 'num_nulls'='0', 'min_value'='0', 'max_value'='6', 'data_size'='143172') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_dow set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='0', 'max_value'='6', 'data_size'='1460') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_dow set stats ('row_count'='365', 'ndv'='7', 'num_nulls'='0', 'min_value'='0', 'max_value'='6', 'data_size'='1460') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_dow set stats ('row_count'='366', 'ndv'='7', 'num_nulls'='0', 'min_value'='0', 'max_value'='6', 'data_size'='1464') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_dow set stats ('row_count'='35065', 'ndv'='7', 'num_nulls'='0', 'min_value'='0', 'max_value'='6', 'data_size'='140260') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_first_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2451911', 'max_value'='2452245', 'data_size'='1460') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_first_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2451180', 'max_value'='2451514', 'data_size'='1460') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_first_dom set stats ('row_count'='35793', 'ndv'='1181', 'num_nulls'='0', 'min_value'='2415021', 'max_value'='2450784', 'data_size'='143172') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_first_dom set stats ('row_count'='366', 'ndv'='12', 'num_nulls'='0', 'min_value'='2451545', 'max_value'='2451880', 'data_size'='1464') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_first_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2452641', 'max_value'='2452975', 'data_size'='1460') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_first_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2452276', 'max_value'='2452610', 'data_size'='1460') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_first_dom set stats ('row_count'='35065', 'ndv'='1161', 'num_nulls'='0', 'min_value'='2453006', 'max_value'='2488070', 'data_size'='140260') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_first_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2450815', 'max_value'='2451149', 'data_size'='1460') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_following_holiday set stats ('row_count'='35065', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='35065') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_following_holiday set stats ('row_count'='35793', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='35793') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_following_holiday set stats ('row_count'='366', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='366') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_following_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_following_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_following_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_following_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_following_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_fy_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='413', 'max_value'='417', 'data_size'='1460') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_fy_quarter_seq set stats ('row_count'='366', 'ndv'='5', 'num_nulls'='0', 'min_value'='401', 'max_value'='405', 'data_size'='1464') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_fy_quarter_seq set stats ('row_count'='35793', 'ndv'='394', 'num_nulls'='0', 'min_value'='1', 'max_value'='393', 'data_size'='143172') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_fy_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='393', 'max_value'='397', 'data_size'='1460') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_fy_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='405', 'max_value'='409', 'data_size'='1460') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_fy_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='397', 'max_value'='401', 'data_size'='1460') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_fy_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='409', 'max_value'='413', 'data_size'='1460') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_fy_quarter_seq set stats ('row_count'='35065', 'ndv'='387', 'num_nulls'='0', 'min_value'='417', 'max_value'='801', 'data_size'='140260') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_fy_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5375', 'max_value'='5427', 'data_size'='1460') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_fy_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5270', 'max_value'='5322', 'data_size'='1460') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_fy_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5323', 'max_value'='5375', 'data_size'='1460') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_fy_week_seq set stats ('row_count'='35793', 'ndv'='5136', 'num_nulls'='0', 'min_value'='1', 'max_value'='5114', 'data_size'='143172') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_fy_week_seq set stats ('row_count'='35065', 'ndv'='5008', 'num_nulls'='0', 'min_value'='5427', 'max_value'='10436', 'data_size'='140260') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_fy_week_seq set stats ('row_count'='366', 'ndv'='53', 'num_nulls'='0', 'min_value'='5218', 'max_value'='5270', 'data_size'='1464') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_fy_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5114', 'max_value'='5166', 'data_size'='1460') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_fy_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5166', 'max_value'='5218', 'data_size'='1460') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_fy_year set stats ('row_count'='366', 'ndv'='1', 'num_nulls'='0', 'min_value'='2000', 'max_value'='2000', 'data_size'='1464') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_fy_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='1998', 'max_value'='1998', 'data_size'='1460') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_fy_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='2002', 'max_value'='2002', 'data_size'='1460') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_fy_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='1999', 'max_value'='1999', 'data_size'='1460') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_fy_year set stats ('row_count'='35065', 'ndv'='97', 'num_nulls'='0', 'min_value'='2004', 'max_value'='2100', 'data_size'='140260') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_fy_year set stats ('row_count'='35793', 'ndv'='98', 'num_nulls'='0', 'min_value'='1900', 'max_value'='1997', 'data_size'='143172') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_fy_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='2001', 'max_value'='2001', 'data_size'='1460') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_fy_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='2003', 'max_value'='2003', 'data_size'='1460') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_holiday set stats ('row_count'='35065', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='35065') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_holiday set stats ('row_count'='366', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='366') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_holiday set stats ('row_count'='35793', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='35793') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_holiday set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_last_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2452275', 'max_value'='2452943', 'data_size'='1460') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_last_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2451910', 'max_value'='2452578', 'data_size'='1460') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_last_dom set stats ('row_count'='35793', 'ndv'='1186', 'num_nulls'='0', 'min_value'='2415020', 'max_value'='2451117', 'data_size'='143172') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_last_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2452640', 'max_value'='2453308', 'data_size'='1460') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_last_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2451179', 'max_value'='2451847', 'data_size'='1460') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_last_dom set stats ('row_count'='35065', 'ndv'='1144', 'num_nulls'='0', 'min_value'='2453005', 'max_value'='2488372', 'data_size'='140260') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_last_dom set stats ('row_count'='366', 'ndv'='12', 'num_nulls'='0', 'min_value'='2451544', 'max_value'='2452214', 'data_size'='1464') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_last_dom set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='2450814', 'max_value'='2451482', 'data_size'='1460') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_month_seq set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1188', 'max_value'='1199', 'data_size'='1460') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_month_seq set stats ('row_count'='35793', 'ndv'='1176', 'num_nulls'='0', 'min_value'='0', 'max_value'='1175', 'data_size'='143172') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_month_seq set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1176', 'max_value'='1187', 'data_size'='1460') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_month_seq set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1236', 'max_value'='1247', 'data_size'='1460') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_month_seq set stats ('row_count'='35065', 'ndv'='1147', 'num_nulls'='0', 'min_value'='1248', 'max_value'='2400', 'data_size'='140260') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_month_seq set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1212', 'max_value'='1223', 'data_size'='1460') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_month_seq set stats ('row_count'='366', 'ndv'='12', 'num_nulls'='0', 'min_value'='1200', 'max_value'='1211', 'data_size'='1464') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_month_seq set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1224', 'max_value'='1235', 'data_size'='1460') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_moy set stats ('row_count'='35793', 'ndv'='12', 'num_nulls'='0', 'min_value'='1', 'max_value'='12', 'data_size'='143172') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_moy set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1', 'max_value'='12', 'data_size'='1460') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_moy set stats ('row_count'='35065', 'ndv'='12', 'num_nulls'='0', 'min_value'='1', 'max_value'='12', 'data_size'='140260') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_moy set stats ('row_count'='366', 'ndv'='12', 'num_nulls'='0', 'min_value'='1', 'max_value'='12', 'data_size'='1464') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_moy set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1', 'max_value'='12', 'data_size'='1460') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_moy set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1', 'max_value'='12', 'data_size'='1460') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_moy set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1', 'max_value'='12', 'data_size'='1460') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_moy set stats ('row_count'='365', 'ndv'='12', 'num_nulls'='0', 'min_value'='1', 'max_value'='12', 'data_size'='1460') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_qoy set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='4', 'data_size'='1460') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_qoy set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='4', 'data_size'='1460') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_qoy set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='4', 'data_size'='1460') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_qoy set stats ('row_count'='366', 'ndv'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='4', 'data_size'='1464') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_qoy set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='4', 'data_size'='1460') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_qoy set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='4', 'data_size'='1460') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_qoy set stats ('row_count'='35793', 'ndv'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='4', 'data_size'='143172') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_qoy set stats ('row_count'='35065', 'ndv'='4', 'num_nulls'='0', 'min_value'='1', 'max_value'='4', 'data_size'='140260') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_quarter_name set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='2001Q1', 'max_value'='2001Q4', 'data_size'='2190') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_quarter_name set stats ('row_count'='35793', 'ndv'='393', 'num_nulls'='0', 'min_value'='1900Q1', 'max_value'='1997Q4', 'data_size'='214758') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_quarter_name set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='2002Q1', 'max_value'='2002Q4', 'data_size'='2190') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_quarter_name set stats ('row_count'='366', 'ndv'='4', 'num_nulls'='0', 'min_value'='2000Q1', 'max_value'='2000Q4', 'data_size'='2196') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_quarter_name set stats ('row_count'='35065', 'ndv'='387', 'num_nulls'='0', 'min_value'='2004Q1', 'max_value'='2100Q1', 'data_size'='210390') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_quarter_name set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='2003Q1', 'max_value'='2003Q4', 'data_size'='2190') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_quarter_name set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='1998Q1', 'max_value'='1998Q4', 'data_size'='2190') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_quarter_name set stats ('row_count'='365', 'ndv'='4', 'num_nulls'='0', 'min_value'='1999Q1', 'max_value'='1999Q4', 'data_size'='2190') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_quarter_seq set stats ('row_count'='35793', 'ndv'='394', 'num_nulls'='0', 'min_value'='1', 'max_value'='393', 'data_size'='143172') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='405', 'max_value'='409', 'data_size'='1460') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_quarter_seq set stats ('row_count'='366', 'ndv'='5', 'num_nulls'='0', 'min_value'='401', 'max_value'='405', 'data_size'='1464') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='397', 'max_value'='401', 'data_size'='1460') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='409', 'max_value'='413', 'data_size'='1460') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='393', 'max_value'='397', 'data_size'='1460') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_quarter_seq set stats ('row_count'='365', 'ndv'='5', 'num_nulls'='0', 'min_value'='413', 'max_value'='417', 'data_size'='1460') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_quarter_seq set stats ('row_count'='35065', 'ndv'='387', 'num_nulls'='0', 'min_value'='417', 'max_value'='801', 'data_size'='140260') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_same_day_lq set stats ('row_count'='35793', 'ndv'='35806', 'num_nulls'='0', 'min_value'='2414930', 'max_value'='2450722', 'data_size'='143172') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_same_day_lq set stats ('row_count'='365', 'ndv'='362', 'num_nulls'='0', 'min_value'='2451088', 'max_value'='2451452', 'data_size'='1460') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_same_day_lq set stats ('row_count'='365', 'ndv'='362', 'num_nulls'='0', 'min_value'='2451819', 'max_value'='2452183', 'data_size'='1460') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_same_day_lq set stats ('row_count'='365', 'ndv'='361', 'num_nulls'='0', 'min_value'='2450723', 'max_value'='2451087', 'data_size'='1460') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_same_day_lq set stats ('row_count'='365', 'ndv'='364', 'num_nulls'='0', 'min_value'='2452184', 'max_value'='2452548', 'data_size'='1460') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_same_day_lq set stats ('row_count'='365', 'ndv'='362', 'num_nulls'='0', 'min_value'='2452549', 'max_value'='2452913', 'data_size'='1460') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_same_day_lq set stats ('row_count'='35065', 'ndv'='34991', 'num_nulls'='0', 'min_value'='2452914', 'max_value'='2487978', 'data_size'='140260') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_same_day_lq set stats ('row_count'='366', 'ndv'='365', 'num_nulls'='0', 'min_value'='2451453', 'max_value'='2451818', 'data_size'='1464') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_same_day_ly set stats ('row_count'='365', 'ndv'='364', 'num_nulls'='0', 'min_value'='2452276', 'max_value'='2452640', 'data_size'='1460') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_same_day_ly set stats ('row_count'='35793', 'ndv'='35878', 'num_nulls'='0', 'min_value'='2414657', 'max_value'='2450449', 'data_size'='143172') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_same_day_ly set stats ('row_count'='366', 'ndv'='363', 'num_nulls'='0', 'min_value'='2451180', 'max_value'='2451544', 'data_size'='1464') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_same_day_ly set stats ('row_count'='365', 'ndv'='363', 'num_nulls'='0', 'min_value'='2451545', 'max_value'='2451910', 'data_size'='1460') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_same_day_ly set stats ('row_count'='365', 'ndv'='364', 'num_nulls'='0', 'min_value'='2450450', 'max_value'='2450814', 'data_size'='1460') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_same_day_ly set stats ('row_count'='35065', 'ndv'='35076', 'num_nulls'='0', 'min_value'='2452641', 'max_value'='2487705', 'data_size'='140260') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_same_day_ly set stats ('row_count'='365', 'ndv'='363', 'num_nulls'='0', 'min_value'='2450815', 'max_value'='2451179', 'data_size'='1460') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_same_day_ly set stats ('row_count'='365', 'ndv'='367', 'num_nulls'='0', 'min_value'='2451911', 'max_value'='2452275', 'data_size'='1460') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5270', 'max_value'='5322', 'data_size'='1460') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5114', 'max_value'='5166', 'data_size'='1460') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5166', 'max_value'='5218', 'data_size'='1460') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5375', 'max_value'='5427', 'data_size'='1460') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_week_seq set stats ('row_count'='365', 'ndv'='53', 'num_nulls'='0', 'min_value'='5323', 'max_value'='5375', 'data_size'='1460') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_week_seq set stats ('row_count'='35065', 'ndv'='5008', 'num_nulls'='0', 'min_value'='5427', 'max_value'='10436', 'data_size'='140260') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_week_seq set stats ('row_count'='35793', 'ndv'='5136', 'num_nulls'='0', 'min_value'='1', 'max_value'='5114', 'data_size'='143172') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_week_seq set stats ('row_count'='366', 'ndv'='53', 'num_nulls'='0', 'min_value'='5218', 'max_value'='5270', 'data_size'='1464') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_weekend set stats ('row_count'='366', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='366') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_weekend set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_weekend set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_weekend set stats ('row_count'='35793', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='35793') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_weekend set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_weekend set stats ('row_count'='35065', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='35065') partition (pfuture);
// """

// sql """
// alter table date_dim modify column d_weekend set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_weekend set stats ('row_count'='365', 'ndv'='2', 'num_nulls'='0', 'min_value'='N', 'max_value'='Y', 'data_size'='365') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='1999', 'max_value'='1999', 'data_size'='1460') partition (p1999);
// """

// sql """
// alter table date_dim modify column d_year set stats ('row_count'='35793', 'ndv'='98', 'num_nulls'='0', 'min_value'='1900', 'max_value'='1997', 'data_size'='143172') partition (ppast);
// """

// sql """
// alter table date_dim modify column d_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='1998', 'max_value'='1998', 'data_size'='1460') partition (p1998);
// """

// sql """
// alter table date_dim modify column d_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='2001', 'max_value'='2001', 'data_size'='1460') partition (p2001);
// """

// sql """
// alter table date_dim modify column d_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='2002', 'max_value'='2002', 'data_size'='1460') partition (p2002);
// """

// sql """
// alter table date_dim modify column d_year set stats ('row_count'='366', 'ndv'='1', 'num_nulls'='0', 'min_value'='2000', 'max_value'='2000', 'data_size'='1464') partition (p2000);
// """

// sql """
// alter table date_dim modify column d_year set stats ('row_count'='365', 'ndv'='1', 'num_nulls'='0', 'min_value'='2003', 'max_value'='2003', 'data_size'='1460') partition (p2003);
// """

// sql """
// alter table date_dim modify column d_year set stats ('row_count'='35065', 'ndv'='97', 'num_nulls'='0', 'min_value'='2004', 'max_value'='2100', 'data_size'='140260') partition (pfuture);
// """



}
