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

suite("docs/table-design/data-partition.md") {
    try {
        sql "drop table if exists example_range_tbl"
        multi_sql """
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
        sql """ALTER TABLE example_range_tbl ADD  PARTITION p201704 VALUES LESS THAN("2020-05-01") DISTRIBUTED BY HASH(`user_id`) BUCKETS 5"""

        sql "drop table if exists null_list"
        multi_sql """
        create table null_list(
        k0 varchar null
        )
        partition by list (k0)
        (
        PARTITION pX values in ((NULL))
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 1
        properties("replication_num" = "1");
        insert into null_list values (null);
        select * from null_list;
        """

        sql "drop table if exists null_range"
        multi_sql """
        create table null_range(
        k0 int null
        )
        partition by range (k0)
        (
        PARTITION p10 values less than (10),
        PARTITION p100 values less than (100),
        PARTITION pMAX values less than (maxvalue)
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 1
        properties("replication_num" = "1");
        insert into null_range values (null);
        select * from null_range partition(p10);
        """

        sql "drop table if exists null_range2"
        sql """
        create table null_range2(
        k0 int null
        )
        partition by range (k0)
        (
        PARTITION p200 values [("100"), ("200"))
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 1
        properties("replication_num" = "1")
        """
        try {
            sql " insert into null_range2 values (null) "
            Assertions.fail("The SQL above should throw an exception as follows:\n\t\terrCode = 2, detailMessage = Insert has filtered data in strict mode. url: http://127.0.0.1:8040/api/_load_error_log?file=__shard_0/error_log_insert_stmt_b3a6d1f1fac74750-b3bb5d6e92a66da4_b3a6d1f1fac74750_b3bb5d6e92a66da4")
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("errCode = 2, detailMessage = Insert has filtered data in strict mode. url:"))
        }

        sql "drop table if exists tbl1"
        sql """
        CREATE TABLE tbl1
        (
            k1 DATE
        )
        PARTITION BY RANGE(k1) ()
        DISTRIBUTED BY HASH(k1)
        PROPERTIES
        (
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.start" = "-7",
            "dynamic_partition.end" = "3",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "32",
            "replication_num" = "1"
        )
        """

        sql "drop table if exists tbl1"
        sql """
        CREATE TABLE tbl1
        (
            k1 DATETIME,
        )
        PARTITION BY RANGE(k1) ()
        DISTRIBUTED BY HASH(k1)
        PROPERTIES
        (
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "WEEK",
            "dynamic_partition.start" = "-2",
            "dynamic_partition.end" = "2",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "8",
            "replication_num" = "1"
        )
        """

        sql "drop table if exists tbl1"
        sql """
        CREATE TABLE tbl1
        (
            k1 DATE
        )
        PARTITION BY RANGE(k1) ()
        DISTRIBUTED BY HASH(k1)
        PROPERTIES
        (
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "MONTH",
            "dynamic_partition.end" = "2",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "8",
            "dynamic_partition.start_day_of_month" = "3",
            "replication_num" = "1"
        )
        """

        sql "SHOW DYNAMIC PARTITION TABLES"
        sql """ ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true") """
        cmd """ curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -XGET http://${context.config.feHttpAddress}/api/_set_config?dynamic_partition_enable=true """

        sql """ ADMIN SET FRONTEND CONFIG ("dynamic_partition_check_interval_seconds" = "7200") """
        cmd """ curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -XGET http://${context.config.feHttpAddress}/api/_set_config?dynamic_partition_check_interval_seconds=432000 """

        sql "drop table if exists `DAILY_TRADE_VALUE`"
        sql """
        CREATE TABLE `DAILY_TRADE_VALUE`
        (
            `TRADE_DATE`              datev2 NOT NULL COMMENT '交易日期',
            `TRADE_ID`                varchar(40) NOT NULL COMMENT '交易编号',
        )
        UNIQUE KEY(`TRADE_DATE`, `TRADE_ID`)
        PARTITION BY RANGE(`TRADE_DATE`)
        (
            PARTITION p_2000 VALUES [('2000-01-01'), ('2001-01-01')),
            PARTITION p_2001 VALUES [('2001-01-01'), ('2002-01-01')),
            PARTITION p_2002 VALUES [('2002-01-01'), ('2003-01-01')),
            PARTITION p_2003 VALUES [('2003-01-01'), ('2004-01-01')),
            PARTITION p_2004 VALUES [('2004-01-01'), ('2005-01-01')),
            PARTITION p_2005 VALUES [('2005-01-01'), ('2006-01-01')),
            PARTITION p_2006 VALUES [('2006-01-01'), ('2007-01-01')),
            PARTITION p_2007 VALUES [('2007-01-01'), ('2008-01-01')),
            PARTITION p_2008 VALUES [('2008-01-01'), ('2009-01-01')),
            PARTITION p_2009 VALUES [('2009-01-01'), ('2010-01-01')),
            PARTITION p_2010 VALUES [('2010-01-01'), ('2011-01-01')),
            PARTITION p_2011 VALUES [('2011-01-01'), ('2012-01-01')),
            PARTITION p_2012 VALUES [('2012-01-01'), ('2013-01-01')),
            PARTITION p_2013 VALUES [('2013-01-01'), ('2014-01-01')),
            PARTITION p_2014 VALUES [('2014-01-01'), ('2015-01-01')),
            PARTITION p_2015 VALUES [('2015-01-01'), ('2016-01-01')),
            PARTITION p_2016 VALUES [('2016-01-01'), ('2017-01-01')),
            PARTITION p_2017 VALUES [('2017-01-01'), ('2018-01-01')),
            PARTITION p_2018 VALUES [('2018-01-01'), ('2019-01-01')),
            PARTITION p_2019 VALUES [('2019-01-01'), ('2020-01-01')),
            PARTITION p_2020 VALUES [('2020-01-01'), ('2021-01-01')),
            PARTITION p_2021 VALUES [('2021-01-01'), ('2022-01-01'))
        )
        DISTRIBUTED BY HASH(`TRADE_DATE`) BUCKETS 10
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        sql "drop table if exists `date_table`"
        sql """
        CREATE TABLE `date_table` (
            `TIME_STAMP` datev2 NOT NULL COMMENT '采集日期'
        ) ENGINE=OLAP
        DUPLICATE KEY(`TIME_STAMP`)
        AUTO PARTITION BY RANGE (date_trunc(`TIME_STAMP`, 'month'))
        (
        )
        DISTRIBUTED BY HASH(`TIME_STAMP`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        """

        sql "drop table if exists `str_table`"
        sql """
        CREATE TABLE `str_table` (
            `str` varchar not null
        ) ENGINE=OLAP
        DUPLICATE KEY(`str`)
        AUTO PARTITION BY LIST (`str`)
        (
        )
        DISTRIBUTED BY HASH(`str`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        """

        sql "drop table if exists auto_null_list"
        multi_sql """
        create table auto_null_list(
        k0 varchar null
        )
        auto partition by list (k0)
        (
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 1
        properties("replication_num" = "1");

        insert into auto_null_list values (null);
        select * from auto_null_list;
        select * from auto_null_list partition(pX);
        """

        try {
            sql "drop table if exists `range_table_nullable`"
            sql """
            CREATE TABLE `range_table_nullable` (
                `k1` INT,
                `k2` DATETIMEV2(3),
                `k3` DATETIMEV2(6)
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            AUTO PARTITION BY RANGE (date_trunc(`k2`, 'day'))
            (
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 16
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            )
            """
            Assertions.fail("The SQL above should throw an exception as follows:\n\t\terrCode = 2, detailMessage = AUTO RANGE PARTITION doesn't support NULL column")
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("errCode = 2, detailMessage = AUTO RANGE PARTITION doesn't support NULL column"))
        }

        sql "drop table if exists `DAILY_TRADE_VALUE`"
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
        )
        """
        def res1 = sql "show partitions from `DAILY_TRADE_VALUE`"
        assertTrue(res1.isEmpty())

        def res2 = multi_sql """
            insert into `DAILY_TRADE_VALUE` values ('2012-12-13', 1), ('2008-02-03', 2), ('2014-11-11', 3);
            show partitions from `DAILY_TRADE_VALUE`;
        """
        assertTrue(res2[1].size() == 3)

    } catch (Throwable t) {
        Assertions.fail("examples in docs/table-design/data-partition.md failed to exec, please fix it", t)
    }
}
