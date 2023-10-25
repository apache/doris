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

create table doris_test.ex_tb20 (
    decimal_normal decimal(38, 5),
    decimal_unsigned decimal(37, 5) unsigned,
    decimal_out1 decimal(39, 5),
    decimal_unsigned_out1 decimal(38, 5) unsigned,
    decimal_long decimal(65, 5),
    decimal_unsigned_long decimal(65, 5) unsigned
) engine=innodb charset=utf8;

create table doris_test.test_insert (
    `id` varchar(128) NULL,
    `name` varchar(128) NULL,
    `age` int NULL
) engine=innodb charset=utf8;


create table doris_test.test_insert2 (
    id varchar(128) NULL,
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
    k11 decimal(10, 3),
    k12 time,
    k13 year,
    k14 datetime,
    k15 timestamp
) engine=innodb charset=utf8;

create table doris_test.all_types (
  `tinyint_u` tinyint unsigned,
  `smallint_u` smallint unsigned,
  `mediumint_u` mediumint unsigned,
  `int_u` int unsigned,
  `bigint_u` bigint unsigned,
  `decimal_u` decimal(18, 5) unsigned,
  `double_u` double unsigned,
  `float_u` float unsigned,
  `boolean` boolean,
  `tinyint` tinyint,
  `smallint` smallint,
  `year` year,
  `mediumint` mediumint,
  `int` int,
  `bigint` bigint,
  `date` date,
  `timestamp` timestamp(4) null,
  `datetime` datetime,
  `float` float,
  `double` double,
  `decimal` decimal(12, 4),
  `char` char(5),
  `varchar` varchar(10),
  `time` time(4),
  `text` text,
  `blob` blob,
  `json` json,
  `set` set('Option1', 'Option2', 'Option3'),
  `bit` bit(6),
  `binary` binary(12),
  `varbinary` varbinary(12),
  `enum` enum('Value1', 'Value2', 'Value3')
) engine=innodb charset=utf8;

CREATE TABLE `doris_test`.`auto_default_t` (
    `id` bigint NOT NULL AUTO_INCREMENT,
    `name` varchar(64) DEFAULT NULL,
    `dt` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
) engine=innodb charset=utf8;

CREATE TABLE doris_test.dt (
  `timestamp0` timestamp(0) DEFAULT CURRENT_TIMESTAMP(0),
  `timestamp1` timestamp(1) DEFAULT CURRENT_TIMESTAMP(1),
  `timestamp2` timestamp(2) DEFAULT CURRENT_TIMESTAMP(2),
  `timestamp3` timestamp(3) DEFAULT CURRENT_TIMESTAMP(3),
  `timestamp4` timestamp(4) DEFAULT CURRENT_TIMESTAMP(4),
  `timestamp5` timestamp(5) DEFAULT CURRENT_TIMESTAMP(5),
  `timestamp6` timestamp(6) DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=INNODB CHARSET=utf8;

CREATE TABLE doris_test.dt_null (
  `dt` datetime NOT NULL
) ENGINE=INNODB CHARSET=utf8;

CREATE VIEW doris_test.mysql_view as
select 10086 as col_1, 4294967295 as col_2, tinyint_u as col_3  from doris_test.all_types where tinyint_u=201;

CREATE TABLE show_test_do_not_modify.ex_tb0 (
  `id` int PRIMARY KEY,
  `name` varchar(128)
);

CREATE TABLE show_test_do_not_modify.ex_tb1 (
  id varchar(128)
);

CREATE TABLE show_test_do_not_modify.ex_tb2 (
  id int,
  count_value varchar(20)
);

create table doris_test.test_bit_type(
 bit_01 bit,
 bit_02 bit,
 bit_03 bit
);

create table doris_test.test_boolean_type(
 boolean_01 boolean,
 boolean_02 boolean,
 boolean_03 boolean,
 boolean_04 boolean,
 boolean_05 boolean
);

create table doris_test.test_tinyint_type(
 t_01 tinyint,
 t_02 tinyint,
 t_03 tinyint,
 t_04 tinyint
);


create table doris_test.test_smallint_type(
s_01 smallint,
s_02 smallint,
s_03 smallint,
s_04 smallint
);

create table doris_test.test_integer_type(
 i_01 integer,
 i_02 integer,
 i_03 integer,
 i_04 integer,
 i_05 bigint,
 i_06 bigint,
 i_07 bigint,
 i_08 bigint
);

create table doris_test.test_varchar2_type(
  v_01 tinytext,
  v_02 text,
  v_03 mediumtext,
  v_04 longtext,
  v_05 varchar(32),
  v_06 varchar(15000)
);

create table doris_test.test_varchar3_type(
  v_01 tinytext CHARACTER SET utf8,
  v_02 text CHARACTER SET utf8,
  v_03 mediumtext CHARACTER SET utf8,
  v_04 longtext CHARACTER SET utf8,
  v_05 varchar(30) CHARACTER SET utf8,
  v_06 varchar(32) CHARACTER SET utf8,
  v_07 varchar(20000) CHARACTER SET utf8
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

create table doris_test.test_char_type(
  v_01 char,
  v_02 char,
  v_03 char(1),
  v_04 char(1),
  v_05 char(8),
  v_06 char(8),
  v_07 char(255),
  v_08 char,
  v_09 char(255)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

create table doris_test.test_char1_type(
    v_01 char(5),
    v_02 char(20),
    v_03 char(20)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

create table doris_test.test_decimal_type(
 d_01 decimal(3, 0),
 d_02 decimal(3, 1),
 d_03 decimal(4, 2),
 d_04 decimal(24, 2),
 d_05 decimal(24, 2),
 d_06 decimal(30, 5),
 d_07 decimal(38, 0),
 d_08 decimal(3, 0)
);

create table doris_test.test_binary_type(
b_01 binary(18),
b_02 binary(18),
b_03 binary(18),
b_04 binary(18),
b_05 binary(18),
b_06 binary(18),
b_07 binary(18)
);


create table doris_test.test_date_type(
  d_01 date,
  d_02 date,
  d_03 date,
  d_04 time,
  d_05 time,
  d_06 time(1),
  d_07 time(2),
  d_08 time(3),
  d_09 datetime(3),
  d_10 datetime(6),
  d_11 time,
  d_12 time
);

create table doris_test.test_date1_type(
d_13 timestamp DEFAULT CURRENT_TIMESTAMP,
d_14 timestamp DEFAULT CURRENT_TIMESTAMP,
d_15 timestamp DEFAULT CURRENT_TIMESTAMP,
d_16 timestamp DEFAULT CURRENT_TIMESTAMP
);


create table doris_test.test_date2_type(
d_01 date,
d_02 date,
d_03 date,
d_04 time
);

create table doris_test.test_json_type(
  j_01 json,
  j_02 json,
  j_03 json,
  j_04 json,
  j_05 json,
  j_06 json,
  j_07 json,
  j_08 json,
  j_09 json,
  j_10 json
);


create table doris_test.test_json1_type(
j_01 json,
j_02 json,
j_03 json,
j_04 json,
j_05 json,
j_06 json,
j_07 json,
j_08 json,
j_09 json,
j_10 json
);


create table doris_test.test_real_type(
  r_01 real,
  r_02 real,
  r_03 real,
  f_01 float,
  f_02 float,
  f_03 float,
  d_01 double,
  d_02 double,
  d_03 double
);

create table doris_test.test_Unsigned_type(
  i_01 TINYINT UNSIGNED,
  i_02 SMALLINT UNSIGNED,
  i_03 INT UNSIGNED,
  i_04 INTEGER UNSIGNED,
  i_05 BIGINT UNSIGNED
);

CREATE TABLE doris_test.test_enum(id int, enum_column ENUM ('b','a','C'));


create table doris_test.testImplementCount(
  v_bigint BIGINT,
  v_double double
);

create table  doris_test.test_nation(
regionkey bigint,
nationkey bigint,
name varchar(255)
);

create table doris_test.binary_test(
x int,
y varbinary(50)
);

create table doris_test.in_test(
x bigint,
y bigint
);

create table doris_test.test_insert_unicode(test varchar(50) CHARACTER SET utf8mb4)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

create table doris_test.test_insert_unicode1(t_varchar varchar(50),t_char char(50))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

create table doris_test.test_load(t_varchar varchar(50),t_char char(50))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE doris_test.nation_load (
    n_nationkey int NOT NULL,
    n_name varchar(250) NOT NULL,
    n_regionkey int NOT NULL,
    n_comment varchar(152) NOT NULL
);

CREATE  VIEW doris_test.test_view AS SELECT * FROM doris_test.nation_load;





























