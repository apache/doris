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

suite("docs/table-design/data-partitioning/auto-partitioning.md") {
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

    sql "create database if not exists auto_partition_doc_test"
    sql "use auto_partition_doc_test"
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

    sql """
        select * from partitions("catalog"="internal","database"="auto_partition_doc_test","table"="DAILY_TRADE_VALUE") where PartitionName = auto_partition_name('range', 'year', '2008-02-03');
    """



    sql "drop table if exists auto_dynamic"
    sql """
    create table auto_dynamic(
        k0 datetime(6) NOT NULL
    )
    auto partition by range (date_trunc(k0, 'year'))
    (
    )
    DISTRIBUTED BY HASH(`k0`) BUCKETS 2
    properties(
        "dynamic_partition.enable" = "true",
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.start" = "-50",
        "dynamic_partition.end" = "0", --- Dynamic Partition 不创建分区
        "dynamic_partition.time_unit" = "year",
        "replication_num" = "1"
    );
    """
}
