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

import org.junit.jupiter.api.Assertions;

suite("docs/table-design/data-partitioning/basic-concepts.md") {
    sql "drop table if exists example_range_tbl"
    sql """
    -- Range Partition
    CREATE TABLE IF NOT EXISTS example_range_tbl
    (
        `user_id` LARGEINT NOT NULL COMMENT "用户id",
        `date` DATE NOT NULL COMMENT "数据灌入日期时间",
        `timestamp` DATETIME NOT NULL COMMENT "数据灌入的时间戳",
        `city` VARCHAR(20) COMMENT "用户所在城市",
        `age` SMALLINT COMMENT "用户年龄",
        `sex` TINYINT COMMENT "用户性别",
        `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
        `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
        `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
        `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
    )
    ENGINE=OLAP
    AGGREGATE KEY(`user_id`, `date`, `timestamp`, `city`, `age`, `sex`)
    PARTITION BY RANGE(`date`)
    (
        PARTITION `p201701` VALUES LESS THAN ("2017-02-01"),
        PARTITION `p201702` VALUES LESS THAN ("2017-03-01"),
        PARTITION `p201703` VALUES LESS THAN ("2017-04-01"),
        PARTITION `p2018` VALUES [("2018-01-01"), ("2019-01-01"))
    )
    DISTRIBUTED BY HASH(`user_id`) BUCKETS 16
    PROPERTIES
    (
        "replication_num" = "1"
    );
    """

    sql "show create table example_range_tbl"
    sql "show partitions from example_range_tbl"
    sql """ALTER TABLE example_range_tbl ADD PARTITION p201704 VALUES LESS THAN("2020-05-01") DISTRIBUTED BY HASH(`user_id`) BUCKETS 5"""



    // table items
    sql "drop table if exists example_range_tbl"
    sql """
    CREATE TABLE IF NOT EXISTS example_range_tbl
    (
        `user_id` LARGEINT NOT NULL COMMENT "用户id",
        `date` DATE NOT NULL COMMENT "数据灌入日期时间",
        `timestamp` DATETIME NOT NULL COMMENT "数据灌入的时间戳",
        `city` VARCHAR(20) COMMENT "用户所在城市",
        `age` SMALLINT COMMENT "用户年龄",
        `sex` TINYINT COMMENT "用户性别",
        `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
        `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
        `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
        `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
    )
    ENGINE=OLAP
    AGGREGATE KEY(`user_id`, `date`, `timestamp`, `city`, `age`, `sex`)
    AUTO PARTITION BY RANGE(date_trunc(`date`, 'month')) --- 使用月作为分区粒度
    ()
    DISTRIBUTED BY HASH(`user_id`) BUCKETS 16
    PROPERTIES
    (
        "replication_num" = "1"
    );
    """

    sql "drop table if exists example_range_tbl"
    sql """
    CREATE TABLE IF NOT EXISTS example_range_tbl
    (
        `user_id` LARGEINT NOT NULL COMMENT "用户id",
        `date` DATE NOT NULL COMMENT "数据灌入日期时间",
        `timestamp` DATETIME NOT NULL COMMENT "数据灌入的时间戳",
        `city` VARCHAR(20) COMMENT "用户所在城市",
        `age` SMALLINT COMMENT "用户年龄",
        `sex` TINYINT COMMENT "用户性别",
        `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
        `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
        `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
        `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
    )
    ENGINE=OLAP
    AGGREGATE KEY(`user_id`, `date`, `timestamp`, `city`, `age`, `sex`)
    PARTITION BY RANGE(`date`)
    ()
    DISTRIBUTED BY HASH(`user_id`) BUCKETS 16
    PROPERTIES
    (
        "replication_num" = "1",
        "dynamic_partition.enable" = "true",
        "dynamic_partition.time_unit" = "WEEK", --- 分区粒度为周
        "dynamic_partition.start" = "-2", --- 向前保留两周
        "dynamic_partition.end" = "2", --- 提前创建后两周
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.buckets" = "8"
    );
    """

    sql "drop table if exists example_range_tbl"
    sql """
    CREATE TABLE IF NOT EXISTS example_range_tbl
    (
        `user_id` LARGEINT NOT NULL COMMENT "用户id",
        `date` DATE NOT NULL COMMENT "数据灌入日期时间",
        `timestamp` DATETIME NOT NULL COMMENT "数据灌入的时间戳",
        `city` VARCHAR(20) COMMENT "用户所在城市",
        `age` SMALLINT COMMENT "用户年龄",
        `sex` TINYINT COMMENT "用户性别",
        `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
        `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
        `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
        `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
    )
    ENGINE=OLAP
    AGGREGATE KEY(`user_id`, `date`, `timestamp`, `city`, `age`, `sex`)
    AUTO PARTITION BY RANGE(date_trunc(`date`, 'month')) --- 使用月作为分区粒度
    ()
    DISTRIBUTED BY HASH(`user_id`) BUCKETS 16
    PROPERTIES
    (
        "replication_num" = "1",
        "dynamic_partition.enable" = "true",
        "dynamic_partition.time_unit" = "month", --- 二者粒度必须相同
        "dynamic_partition.start" = "-2", --- 动态分区自动清理超过两周的历史分区
        "dynamic_partition.end" = "0", --- 动态分区不创建未来分区，完全交给自动分区
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.buckets" = "8"
    );
    """

    sql "drop table if exists example_range_tbl"
    sql """
    CREATE TABLE IF NOT EXISTS example_range_tbl
    (
        `user_id` LARGEINT NOT NULL COMMENT "用户id",
        `date` DATE NOT NULL COMMENT "数据灌入日期时间",
        `timestamp` DATETIME NOT NULL COMMENT "数据灌入的时间戳",
        `city` VARCHAR(20) COMMENT "用户所在城市",
        `age` SMALLINT COMMENT "用户年龄",
        `sex` TINYINT COMMENT "用户性别",
        `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
        `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
        `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
        `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
    )
    ENGINE=OLAP
    AGGREGATE KEY(`user_id`, `date`, `timestamp`, `city`, `age`, `sex`)
    PARTITION BY RANGE(`date`)
    (
        PARTITION `p201701` VALUES LESS THAN ("2017-02-01"),
        PARTITION `p201702` VALUES LESS THAN ("2017-03-01"),
        PARTITION `p201703` VALUES LESS THAN ("2017-04-01"),
        PARTITION `p2018` VALUES [("2018-01-01"), ("2019-01-01"))
    )
    DISTRIBUTED BY HASH(`user_id`) BUCKETS AUTO
    PROPERTIES
    (
        "replication_num" = "1",
        "estimate_partition_size" = "2G" --- 用户估计一个分区将有的数据量，不提供则默认为 10G
    );
    """



    // Partition Retrieval
    sql "create database if not exists partition_basic_concepts"
    sql "use partition_basic_concepts"
    sql "drop table if exists DAILY_TRADE_VALUE"
    sql """
    CREATE TABLE `DAILY_TRADE_VALUE`
    (
        `TRADE_DATE`              datev2 NOT NULL COMMENT '交易日期',
        `TRADE_ID`                varchar(40) NOT NULL COMMENT '交易编号',
    )
    UNIQUE KEY(`TRADE_DATE`, `TRADE_ID`)
    AUTO PARTITION BY RANGE (date_trunc(`TRADE_DATE`, 'year'))
    (
    )
    DISTRIBUTED BY HASH(`TRADE_DATE`) BUCKETS 10
    PROPERTIES (
        "replication_num" = "1"
    );
    """
    
    sql "drop table if exists list_table1"
    sql """
    CREATE TABLE `list_table1` (
        `str` varchar
    ) ENGINE=OLAP
    DUPLICATE KEY(`str`)
    COMMENT 'OLAP'
    AUTO PARTITION BY LIST (`str`)
    (
    )
    DISTRIBUTED BY HASH(`str`) BUCKETS 10
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """ insert into list_table1 values ("Beijing"), ("XXX"), ("xxx"), ("Beijing"), ("Abc"), (null) """
    sql """ insert into list_table1 values (null), ("") """ // not same partition

    sql """ select * from partitions("catalog"="internal", "database"="partition_basic_concepts", "table"="DAILY_TRADE_VALUE") where PartitionName = auto_partition_name('range', 'year', '2008-02-03'); """
    sql """ select * from information_schema.partitions where TABLE_SCHEMA='partition_basic_concepts' and TABLE_NAME='list_table1' and PARTITION_NAME=auto_partition_name('list', null); """
    sql """ select * from information_schema.partitions where TABLE_NAME='DAILY_TRADE_VALUE' and PARTITION_DESCRIPTION like "[('2012-01-01'),%"; """
}
