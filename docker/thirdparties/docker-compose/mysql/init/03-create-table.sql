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

create table doris_test.test1 (
    k1 boolean,
    k2 char(100),
    k3 varchar(128),
    k4 date,
    k5 float,
    k6 tinyint,
    k7 smallint,
    k8 int,
    k9 bigint,
    k10 double,
    k11 datetime,
    k12 decimal(10, 3)
);

CREATE TABLE doris_test.ex_tb0 (
  `id` int PRIMARY KEY,
  `name` varchar(128)
);

CREATE TABLE doris_test.ex_tb1 (
  id varchar(128)
);

CREATE TABLE doris_test.ex_tb2 (
  id int,
  count_value varchar(20)
);

CREATE TABLE `doris_test.ex_tb3` (
  `game_code` varchar(20) NOT NULL,
  `plat_code` varchar(20) NOT NULL,
  `account` varchar(100) NOT NULL,
  `login_time` bigint(20) NOT NULL,
  `register_time` bigint(20) DEFAULT NULL,
  `pid` varchar(20) DEFAULT NULL,
  `gid` varchar(20) DEFAULT NULL,
  `region` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`game_code`,`plat_code`,`account`,`login_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `doris_test.ex_tb4` (
  `products_id` int(11) NOT NULL AUTO_INCREMENT,
  `orders_id` int(11) NOT NULL,
  `sales_add_time` datetime NOT NULL COMMENT '领款时间',
  `sales_update_time` datetime NOT NULL COMMENT '录入更新时间',
  `finance_admin` int(11) NOT NULL COMMENT '1代表系统自动录入',
  PRIMARY KEY (`products_id`),
  UNIQUE KEY `idx_orders_id` (`orders_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1985724 DEFAULT CHARSET=utf8;

CREATE TABLE `doris_test.ex_tb5` (
  `id` int(10) unsigned not null AUTO_INCREMENT comment "主建",
  `apply_id` varchar(32) Default null,
  `begin_value` mediumtext,
  `operator` varchar(32) Default null,
  `operator_name` varchar(32) Default null,
  `state` varchar(8) Default null,
  `sub_state` varchar(8) Default null,
  `state_count` smallint(5) unsigned Default null,
  `create_time` datetime Default CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_apply_id` (`apply_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3732465 DEFAULT CHARSET=utf8mb4;

