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

CREATE TABLE doris_test.ex_tb3 (
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

CREATE TABLE doris_test.ex_tb4 (
  `products_id` int(11) NOT NULL AUTO_INCREMENT,
  `orders_id` int(11) NOT NULL,
  `sales_add_time` datetime NOT NULL COMMENT '领款时间',
  `sales_update_time` datetime NOT NULL COMMENT '录入更新时间',
  `finance_admin` int(11) NOT NULL COMMENT '1代表系统自动录入',
  PRIMARY KEY (`products_id`),
  UNIQUE KEY `idx_orders_id` (`orders_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1985724 DEFAULT CHARSET=utf8;

CREATE TABLE doris_test.ex_tb5 (
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

CREATE TABLE doris_test.ex_tb6 (
  `id` bigint(20) PRIMARY KEY,
  `t_id` bigint(20) NULL,
  `name` text NULL
);

CREATE TABLE doris_test.ex_tb7 (
  `id` varchar(32) NULL DEFAULT "",
  `user_name` varchar(32) NULL DEFAULT "",
  `member_list` DECIMAL(10,3)
) engine=innodb charset=utf8;

CREATE TABLE doris_test.ex_tb8 (
   `date` date NOT NULL COMMENT "",
   `uid` varchar(64) NOT NULL,
   `stat_type` int(11) NOT NULL COMMENT "",
   `price` varchar(255) NULL COMMENT "price"
) engine=innodb charset=utf8;

CREATE TABLE doris_test.ex_tb9 (
   c_date date null
) engine=innodb charset=utf8;

CREATE TABLE doris_test.ex_tb10 (
    `aa` varchar(200) NULL,
    `bb` int NULL,
    `cc` bigint NULL
) engine=innodb charset=utf8;

CREATE TABLE doris_test.ex_tb11 (
 `aa` varchar(200) PRIMARY KEY,
 `bb` int NULL
) engine=innodb charset=utf8;

CREATE TABLE doris_test.ex_tb12 (
 `cc` varchar(200) PRIMARY KEY,
 `dd` int NULL
) engine=innodb charset=utf8;

CREATE TABLE doris_test.ex_tb13 (
     name varchar(128),
     age INT,
     idCode  varchar(128),
     cardNo varchar(128),
     number varchar(128),
     birthday DATETIME,
     country varchar(128),
     gender varchar(128),
     covid BOOLEAN
) engine=innodb charset=utf8;

CREATE TABLE doris_test.ex_tb14 (
     tid varchar(128),
     log_time date,
     dt  date,
     cmd varchar(128),
     dp_from varchar(128)
) engine=innodb charset=utf8;

CREATE TABLE doris_test.ex_tb15 (
    col1 varchar(10) NULL ,
    col2 varchar(10) NULL ,
    col3 varchar(10) NULL ,
    col4 int(11) NULL ,
    col5 double NULL ,
    col6 double NULL ,
    col7 int(11) NULL ,
    col8 int(11) NULL ,
    col9 int(11) NULL ,
    col10 varchar(10) NULL ,
    col11 varchar(10) NULL ,
    col12 datetime NULL
) engine=innodb charset=utf8;

CREATE TABLE doris_test.ex_tb16 (
   `id` bigint(20) NOT NULL COMMENT '',
   `name` varchar(192) NOT NULL COMMENT '',
   `is_delete` tinyint(4) NULL,
   `create_uid` bigint(20) NULL,
   `modify_uid` bigint(20) NULL,
   `ctime` bigint(20) NULL,
   `mtime` bigint(20) NULL
) engine=innodb charset=utf8;

CREATE TABLE doris_test.ex_tb17 (
  `id` bigint(20) NULL,
  `media_order_id` int(11) NULL,
  `supplier_id` int(11) NULL,
  `agent_policy_type` tinyint(4) NULL,
  `agent_policy` decimal(6, 2) NULL,
  `capital_type` bigint(20) NULL,
  `petty_cash_type` tinyint(4) NULL,
  `recharge_amount` decimal(10, 2) NULL,
  `need_actual_amount` decimal(10, 2) NULL,
  `voucher_url` varchar(765) NULL,
  `ctime` bigint(20) NULL,
  `mtime` bigint(20) NULL,
  `is_delete` tinyint(4) NULL,
  `media_remark` text NULL,
  `account_number` varchar(765) NULL,
  `currency_type` tinyint(4) NULL,
  `order_source` tinyint(4) NULL
) engine=innodb charset=utf8;

create table doris_test.ex_tb18 (
    num_tinyint tinyint,
    num_tinyint2 tinyint unsigned,
    num_smallint SMALLINT,
    num_smallint2 SMALLINT unsigned,
    num_mediumint MEDIUMINT,
    num_mediumint2 MEDIUMINT unsigned,
    num_bigint BIGINT,
    num_int int(5),
    num_int2 int(5) unsigned,
    num_int3 int(5) unsigned zerofill,
    num_float float(5, 2),
    num_double double(10, 3),
    num_decimal decimal(20, 2),
    char_value1 char(5),
    char_value2 char(100),
    varchar_value1 varchar(5),
    varchar_value2 varchar(10),
    varchar_value3 varchar(100),
    text_value TEXT(123)
) engine=innodb charset=utf8;

create table doris_test.ex_tb19 (
    date_value date,
    time_value time,
    year_value year,
    datetime_value datetime,
    timestamp_value timestamp
) engine=innodb charset=utf8;


