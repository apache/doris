
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
  sql """
  drop table if exists store_sales;
  drop table if exists date_dim;
  drop table if exists web_sales;
  
  CREATE TABLE `store_sales` (
  `ss_sold_date_sk` bigint NULL,
  `ss_sold_time_sk` bigint NULL,
  `ss_item_sk` bigint NULL,
  `ss_customer_sk` bigint NULL,
  `ss_cdemo_sk` bigint NULL,
  `ss_hdemo_sk` bigint NULL,
  `ss_addr_sk` bigint NULL,
  `ss_store_sk` bigint NULL,
  `ss_promo_sk` bigint NULL,
  `ss_ticket_number` bigint NULL,
  `ss_quantity` int NULL,
  `ss_wholesale_cost` decimal(7,2) NULL,
  `ss_list_price` decimal(7,2) NULL,
  `ss_sales_price` decimal(7,2) NULL,
  `ss_ext_discount_amt` decimal(7,2) NULL,
  `ss_ext_sales_price` decimal(7,2) NULL,
  `ss_ext_wholesale_cost` decimal(7,2) NULL,
  `ss_ext_list_price` decimal(7,2) NULL,
  `ss_ext_tax` decimal(7,2) NULL,
  `ss_coupon_amt` decimal(7,2) NULL,
  `ss_net_paid` decimal(7,2) NULL,
  `ss_net_paid_inc_tax` decimal(7,2) NULL,
  `ss_net_profit` decimal(7,2) NULL
) ENGINE=OLAP
DUPLICATE KEY(`ss_sold_date_sk`, `ss_sold_time_sk`, `ss_item_sk`, `ss_customer_sk`)
DISTRIBUTED BY HASH(`ss_customer_sk`) BUCKETS 3
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V3",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);

CREATE TABLE `date_dim` (
  `d_date_sk` bigint NULL,
  `d_date_id` char(16) NULL,
  `d_date` date NULL,
  `d_month_seq` int NULL,
  `d_week_seq` int NULL,
  `d_quarter_seq` int NULL,
  `d_year` int NULL,
  `d_dow` int NULL,
  `d_moy` int NULL,
  `d_dom` int NULL,
  `d_qoy` int NULL,
  `d_fy_year` int NULL,
  `d_fy_quarter_seq` int NULL,
  `d_fy_week_seq` int NULL,
  `d_day_name` char(9) NULL,
  `d_quarter_name` char(6) NULL,
  `d_holiday` char(1) NULL,
  `d_weekend` char(1) NULL,
  `d_following_holiday` char(1) NULL,
  `d_first_dom` int NULL,
  `d_last_dom` int NULL,
  `d_same_day_ly` int NULL,
  `d_same_day_lq` int NULL,
  `d_current_day` char(1) NULL,
  `d_current_week` char(1) NULL,
  `d_current_month` char(1) NULL,
  `d_current_quarter` char(1) NULL,
  `d_current_year` char(1) NULL
) ENGINE=OLAP
DUPLICATE KEY(`d_date_sk`, `d_date_id`)
DISTRIBUTED BY HASH(`d_date_id`) BUCKETS 3
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V3",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);

CREATE TABLE `web_sales` (
  `ws_sold_date_sk` bigint NULL,
  `ws_sold_time_sk` bigint NULL,
  `ws_ship_date_sk` bigint NULL,
  `ws_item_sk` bigint NULL,
  `ws_bill_customer_sk` bigint NULL,
  `ws_bill_cdemo_sk` bigint NULL,
  `ws_bill_hdemo_sk` bigint NULL,
  `ws_bill_addr_sk` bigint NULL,
  `ws_ship_customer_sk` bigint NULL,
  `ws_ship_cdemo_sk` bigint NULL,
  `ws_ship_hdemo_sk` bigint NULL,
  `ws_ship_addr_sk` bigint NULL,
  `ws_web_page_sk` bigint NULL,
  `ws_web_site_sk` bigint NULL,
  `ws_ship_mode_sk` bigint NULL,
  `ws_warehouse_sk` bigint NULL,
  `ws_promo_sk` bigint NULL,
  `ws_order_number` bigint NULL,
  `ws_quantity` int NULL,
  `ws_wholesale_cost` decimal(7,2) NULL,
  `ws_list_price` decimal(7,2) NULL,
  `ws_sales_price` decimal(7,2) NULL,
  `ws_ext_discount_amt` decimal(7,2) NULL,
  `ws_ext_sales_price` decimal(7,2) NULL,
  `ws_ext_wholesale_cost` decimal(7,2) NULL,
  `ws_ext_list_price` decimal(7,2) NULL,
  `ws_ext_tax` decimal(7,2) NULL,
  `ws_coupon_amt` decimal(7,2) NULL,
  `ws_ext_ship_cost` decimal(7,2) NULL,
  `ws_net_paid` decimal(7,2) NULL,
  `ws_net_paid_inc_tax` decimal(7,2) NULL,
  `ws_net_paid_inc_ship` decimal(7,2) NULL,
  `ws_net_paid_inc_ship_tax` decimal(7,2) NULL,
  `ws_net_profit` decimal(7,2) NULL
) ENGINE=OLAP
DUPLICATE KEY(`ws_sold_date_sk`, `ws_sold_time_sk`, `ws_ship_date_sk`, `ws_item_sk`)
DISTRIBUTED BY HASH(`ws_item_sk`) BUCKETS 3
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V3",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
); 

INSERT INTO store_sales (
  ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk,
  ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity,
  ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt,
  ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax,
  ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit
) VALUES (
  20240101, 36000, 1001, 501, 601, 701,
  801, 901, 10001, 55500001, 2,
  10.00, 12.00, 11.00, 2.00,
  22.00, 20.00, 24.00, 1.54,
  0.00, 22.00, 23.54, 3.54
);

INSERT INTO date_dim (
  d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year,
  d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq,
  d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday,
  d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq,
  d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year
) VALUES (
  20240101, '2024-01-01', '2024-01-01', 1, 1, 1, 2024,
  1, 1, 1, 1, 2024, 1, 1,
  'MON', 'Q1', 'N', 'N', 'N',
  1, 31, 20230101, 20231001,
  'Y', 'Y', 'Y', 'Y', 'Y'
);

INSERT INTO web_sales (
  ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk,
  ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk,
  ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk,
  ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk,
  ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price,
  ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price,
  ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax,
  ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit
) VALUES (
  20240101, 43200, 20240103, 2001,
  601, 701, 801, 901,
  602, 702, 802, 902,
  3001, 4001, 5001, 6001, 7001,
  8800001, 3, 15.00, 18.00, 16.50,
  4.50, 49.50, 45.00, 54.00,
  3.47, 0.00, 5.00, 49.50, 52.97,
  54.50, 58.00, 7.97
);
"""
}

