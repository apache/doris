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

drop table if exists dim_locations;
CREATE TABLE `dim_locations` (
  `record_id` varchar(4) NOT NULL,
  `location_id` varchar(8) NOT NULL,
  `city` varchar(11) NOT NULL,
  `state` varchar(2) NOT NULL,
  `country` varchar(3) NOT NULL,
  `region` varchar(9) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`record_id`)
DISTRIBUTED BY HASH(`record_id`) BUCKETS 12
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);

drop table if exists dim_products;
CREATE TABLE `dim_products` (
  `record_id` varchar(2) NOT NULL,
  `product_id` varchar(2) NOT NULL,
  `name` varchar(22) NOT NULL,
  `category` varchar(5) NOT NULL,
  `subcategory` varchar(8) NOT NULL,
  `standard_cost` double NOT NULL,
  `standard_price` double NOT NULL,
  `from_date` date NOT NULL,
  `to_date` date NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`record_id`)
DISTRIBUTED BY HASH(`record_id`) BUCKETS 12
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);


drop table if exists fact_sales;
CREATE TABLE `fact_sales` (
  `order_id` varchar(255) NOT NULL,
  `order_line_id` varchar(255) NOT NULL,
  `order_date` date NOT NULL,
  `time_of_day` varchar(50) NOT NULL,
  `season` varchar(50) NOT NULL,
  `month` int NOT NULL,
  `location_id` varchar(8) NOT NULL,
  `region` varchar(100) NOT NULL,
  `product_name` varchar(255) NOT NULL,
  `quantity` int NOT NULL,
  `sales_amount` double NOT NULL,
  `discount_percentage` int NOT NULL,
  `product_id` varchar(255) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`order_id`, `order_line_id`)
DISTRIBUTED BY HASH(`order_id`) BUCKETS 256
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);

drop table if exists temp1;
drop table if exists temp2;
drop table if exists temp3;


CREATE TABLE `temp1` (
t1 date not null,
t2 varchar(5) not null,
t3 double not null
) ENGINE=OLAP
DUPLICATE KEY(`t1`)
DISTRIBUTED BY RANDOM BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);

CREATE TABLE `temp2` (
  `t1` date NOT NULL,
  `t2` string NOT NULL,
  `t3` bigint NOT NULL,
  `t4` bigint NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`t1`)
DISTRIBUTED BY RANDOM BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);

CREATE TABLE `temp3` (
  `t1` varchar(11) NOT NULL,
  `t2` string NOT NULL,
  `t3` date NULL,
  `t4` double  NULL,
 `t5` double  NULL,
 `t6` double NULL
) ENGINE=OLAP
DUPLICATE KEY(`t1`)
DISTRIBUTED BY RANDOM BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);
