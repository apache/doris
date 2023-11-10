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

CREATE TABLE doris_test.ex_tb0
(
    `id`   int PRIMARY KEY,
    `name` varchar(128)
);

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
  `timestamp0` timestamp(0) DEFAULT CURRENT_TIMESTAMP(),
  `timestamp1` timestamp(0) DEFAULT CURRENT_TIMESTAMP(0),
  `timestamp2` timestamp(1) DEFAULT CURRENT_TIMESTAMP(1),
  `timestamp3` timestamp(2) DEFAULT CURRENT_TIMESTAMP(2),
  `timestamp4` timestamp(3) DEFAULT CURRENT_TIMESTAMP(3),
  `timestamp5` timestamp(4) DEFAULT CURRENT_TIMESTAMP(4),
  `timestamp6` timestamp(5) DEFAULT CURRENT_TIMESTAMP(5),
  `timestamp7` timestamp(6) DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=INNODB CHARSET=utf8;

