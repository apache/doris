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

suite("test_delete") {
    def tableName = "delete_regression_test"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (c1 varchar(190) NOT NULL COMMENT "",c2 bigint(20) NOT NULL COMMENT "", c3 varchar(160) NULL COMMENT "" ) ENGINE=OLAP DUPLICATE KEY(c1, c2) COMMENT "OLAP" DISTRIBUTED BY HASH(c3) BUCKETS 3 
    PROPERTIES ( "replication_num" = "1" );"""

    sql """INSERT INTO ${tableName} VALUES ('abcdef',1,'fjdsajfldjafljdslajfdl'),('abcdef',2,'fjdsajfldjafljdslajfdl'),('abcdef',4,'fjdsajfldjafljdslajfdl'),('abcdef',5,'fjdsajfldjafljdslajfdl')"""
    sql """delete from ${tableName} where c1 = 'fjdsajfldjafljdslajfdl';"""
    sql """INSERT INTO ${tableName} VALUES ('abcdef',1,'fjdsajfldjafljdslajfdl'),('abcdef',2,'fjdsajfldjafljdslajfdl'),('abcdef',4,'fjdsajfldjafljdslajfdl'),('abcdef',5,'fjdsajfldjafljdslajfdl');"""
    
    qt_sql """select count(*) from ${tableName};"""
    qt_sql """select count(c2) from ${tableName};"""
    qt_sql """select count(c2) from ${tableName} where c1 = 'abcdef';"""
    qt_sql """select count(c1) from ${tableName};"""
    qt_sql """select count(c1) from ${tableName} where c1 = 'abcdef';"""

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """ CREATE TABLE IF NOT EXISTS delete_regression_test (k1 varchar(190) NOT NULL COMMENT "", k2 DATEV2 NOT NULL COMMENT "", k3 DATETIMEV2 NOT NULL COMMENT "", k4 DATETIMEV2(3) NOT NULL COMMENT "", v1 DATEV2 NOT NULL COMMENT "", v2 DATETIMEV2 NOT NULL COMMENT "", v3 DATETIMEV2(3) NOT NULL COMMENT "" ) ENGINE=OLAP DUPLICATE KEY(k1, k2, k3, k4) COMMENT "OLAP" DISTRIBUTED BY HASH(k1, k2, k3, k4) BUCKETS 3
    PROPERTIES ( "replication_num" = "1" );"""

    sql """ INSERT INTO delete_regression_test VALUES ('abcdef','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111'),('abcdef','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111'); """
    sql """ delete from ${tableName} where k2 = '2022-08-16' """
    qt_sql1 """select * from ${tableName} ORDER BY k2;"""
    sql """ delete from ${tableName} where k1 = 'abcdef' """

    sql """ INSERT INTO ${tableName} VALUES ('abcdef','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111'),('abcdef','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111'); """
    sql """ delete from ${tableName} where k3 = '2022-08-16 11:11:11' """
    qt_sql2 """select * from ${tableName} ORDER BY k2;"""
    sql """ delete from ${tableName} where k1 = 'abcdef' """

    sql """ INSERT INTO ${tableName} VALUES ('abcdef','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111'),('abcdef','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111'); """
    sql """ delete from ${tableName} where k4 = '2022-08-16 11:11:11' """
    qt_sql3 """select * from ${tableName} ORDER BY k2;"""
    sql """ delete from ${tableName} where k4 = '2022-08-16 11:11:11.111' """
    qt_sql4 """select * from ${tableName} ORDER BY k2;"""
    sql """ delete from delete_regression_test where k1 = 'abcdef' """

    sql """ INSERT INTO ${tableName} VALUES ('abcdef','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111'),('abcdef','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111'); """
    sql """ delete from ${tableName} where v1 = '2022-08-16' """
    qt_sql5 """select * from ${tableName} ORDER BY k2;"""
    sql """ delete from delete_regression_test where k1 = 'abcdef' """

    sql """ INSERT INTO ${tableName} VALUES ('abcdef','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111'),('abcdef','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111'); """
    sql """ delete from ${tableName} where v2 = '2022-08-16 11:11:11' """
    qt_sql6 """select * from ${tableName} ORDER BY k2;"""
    sql """ delete from delete_regression_test where k1 = 'abcdef' """

    sql """ INSERT INTO ${tableName} VALUES ('abcdef','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111'),('abcdef','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111'); """
    sql """ delete from ${tableName} where v3 = '2022-08-16 11:11:11' """
    qt_sql7 """select * from ${tableName} ORDER BY k2;"""
    sql """ delete from ${tableName} where v3 = '2022-08-16 11:11:11.111' """
    qt_sql8 """select * from ${tableName} ORDER BY k2;"""
    sql """ delete from delete_regression_test where k1 = 'abcdef' """

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """ DROP TABLE IF EXISTS tb_test1 """
    sql """  CREATE TABLE `tb_test1` (
  	    `dt` date NULL,
  	    `code` int(11) NULL
	) ENGINE=OLAP
    DUPLICATE KEY(`dt`, `code`)
    COMMENT 'OLAP'
    PARTITION BY RANGE(`dt`)
    (PARTITION m202206 VALUES [('2022-06-01'), ('2022-07-01')),
    PARTITION m202207 VALUES [('2022-07-01'), ('2022-08-01')),
    PARTITION m202208 VALUES [('2022-08-01'), ('2022-09-01')),
    PARTITION m202209 VALUES [('2022-09-01'), ('2022-10-01')),
    PARTITION m202210 VALUES [('2022-10-01'), ('2022-11-01')),
    PARTITION m202211 VALUES [('2022-11-01'), ('2022-12-01')),
    PARTITION m202212 VALUES [('2022-12-01'), ('2023-01-01')),
    PARTITION m202301 VALUES [('2023-01-01'), ('2023-02-01')),
    PARTITION m202302 VALUES [('2023-02-01'), ('2023-03-01')),
    PARTITION m202303 VALUES [('2023-03-01'), ('2023-04-01')),
    PARTITION m202304 VALUES [('2023-04-01'), ('2023-05-01')))
    DISTRIBUTED BY HASH(`dt`, `code`) BUCKETS 10
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "dynamic_partition.enable" = "true",
        "dynamic_partition.time_unit" = "MONTH",
        "dynamic_partition.time_zone" = "Asia/Shanghai",
        "dynamic_partition.start" = "-2147483648",
        "dynamic_partition.end" = "1",
        "dynamic_partition.prefix" = "m",
        "dynamic_partition.replication_allocation" = "tag.location.default: 1",
        "dynamic_partition.buckets" = "10",
        "dynamic_partition.create_history_partition" = "false",
        "dynamic_partition.history_partition_num" = "-1",
        "dynamic_partition.hot_partition_num" = "0",
        "dynamic_partition.reserved_history_periods" = "NULL",
        "dynamic_partition.storage_policy" = "",
        "dynamic_partition.start_day_of_month" = "1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
    );
	"""
    sql """ insert into tb_test1 values ('2022-10-01', 123); """
    qt_sql9 """select * from tb_test1;"""
    sql """ delete from tb_test1 where DT = '20221001'; """
    qt_sql10 """select * from tb_test1;"""

    sql """ DROP TABLE IF EXISTS delete_test_tb """
    sql """
        CREATE TABLE `delete_test_tb` (
          `k1` varchar(30)  NULL,
          `v1` varchar(30) NULL
        )
        UNIQUE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 4
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    sql """
        insert into delete_test_tb values
            (' ', '1'), ('  ', '2'), ('   ', '3'), ('    ', '4'),
            ('abc', '5'), ("'d", '6'), ("'e'", '7'), ("f'", '8');
    """

    qt_check_data """
        select k1, v1 from delete_test_tb order by v1;
    """

    sql """
        delete from delete_test_tb where k1 = '  ';
    """
    qt_check_data2 """
        select k1, v1 from delete_test_tb order by v1;
    """

     sql """
        delete from delete_test_tb where k1 = '    ';
    """
    qt_check_data3 """
        select k1, v1 from delete_test_tb order by v1;
    """

    sql """
        delete from delete_test_tb where k1 = "'d";
    """
    qt_check_data4 """
        select k1, v1 from delete_test_tb order by v1;
    """

    sql """
        delete from delete_test_tb where k1 = "'e'";
    """
    qt_check_data5 """
        select k1, v1 from delete_test_tb order by v1;
    """

    sql """
        delete from delete_test_tb where k1 = "f'";
    """
    qt_check_data6 """
        select k1, v1 from delete_test_tb order by v1;
    """

    sql """ DROP TABLE IF EXISTS delete_test_tb2 """
    sql """
        CREATE TABLE `delete_test_tb2` (
          `k1` int  NULL,
          `k2` decimal(9, 2) NULL,
          `v1` double NULL
        )
        UNIQUE KEY(`k1`, `k2`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 4
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    sql """
        insert into delete_test_tb2 values
            (1, 1.12, 1.1), (2, 2.23, 2.2), (3, 3.34, 3.3), (null, 4.45, 4.4),
            (5, null, 5.5), (null, null, 6.6);
    """

    qt_check_numeric """ select k1, k2, v1 from delete_test_tb2 order by k1, k2; """;

    sql """ delete from  delete_test_tb2 where k1 = 1; """
    qt_check_numeric """ select k1, k2, v1 from delete_test_tb2 order by k1, k2; """;

    sql """ delete from  delete_test_tb2 where k2 = 2.23; """
    qt_check_numeric2 """ select k1, k2, v1 from delete_test_tb2 order by k1, k2; """;

    sql """ delete from  delete_test_tb2 where k1 = 3 and k2 = 3.3; """
    qt_check_numeric3 """ select k1, k2, v1 from delete_test_tb2 order by k1, k2; """;

    sql """ delete from  delete_test_tb2 where k1 = 3 and k2 = 3.34; """
    qt_check_numeric4 """ select k1, k2, v1 from delete_test_tb2 order by k1, k2; """;

    sql """ delete from  delete_test_tb2 where k1 is not null and k2 = 4.45; """
    qt_check_numeric4 """ select k1, k2, v1 from delete_test_tb2 order by k1, k2; """;

    sql """ delete from  delete_test_tb2 where k1 is null and k2 = 4.45; """
    qt_check_numeric4 """ select k1, k2, v1 from delete_test_tb2 order by k1, k2; """;
    
    sql """ DROP TABLE IF EXISTS test1 """

    sql '''
        CREATE TABLE test1 (
            x varchar(10) NOT NULL,
            id varchar(10) NOT NULL
        )
        ENGINE=OLAP
        UNIQUE KEY(`x`)COMMENT "OLAP"
        DISTRIBUTED BY HASH(`x`) 
        BUCKETS 96
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    '''
    
    sql 'insert into test1 values("a", "a"), ("bb", "bb"), ("ccc", "ccc")'
    sql 'delete from test1 where length(x)=2'
    
    qt_delete_fn 'select * from test1 order by x'
    
    sql 'truncate table test1'

    sql 'insert into test1 values("a", "a"), ("bb", "bb"), ("ccc", "ccc")'
    sql 'delete from test1 where length(id) >= 2'

    test {
        sql 'select * from test1 order by x'
        result([['a', 'a']])
    }

    sql "drop table if exists dwd_pay"
    sql """
    CREATE TABLE `dwd_pay` (
  `tenant_id` int(11) DEFAULT NULL COMMENT '租户ID',
  `pay_time` datetime DEFAULT NULL COMMENT '付款时间',
)  ENGINE=OLAP
DUPLICATE KEY(`tenant_id`)
COMMENT "付款明细"
PARTITION BY RANGE(`pay_time` ) (
PARTITION p202012 VALUES LESS THAN ('2021-01-01 00:00:00')
)
DISTRIBUTED BY HASH(`tenant_id`) BUCKETS auto
PROPERTIES
(
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.end" = "2",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.start_day_of_month" = "1",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "120",
    "dynamic_partition.buckets"="1",
    "estimate_partition_size" = "1G",
    "storage_type" = "COLUMN",
    "replication_num" = "1"
);
    """

    sql "delete from dwd_pay partitions(p202310) where pay_time = '20231002';"

    // add every type of delete
    sql "drop table if exists every_type_table"
    sql """
    CREATE TABLE `every_type_table` 
  (
            col_1 tinyint,
            col_2 smallint,
            col_3 int,
            col_4 bigint,
            col_5 decimal(10,3),
            col_6 char,
            col_7 varchar(20),
            col_9 date,
            col_10 datetime,
            col_11 boolean,
            col_8 string,
        ) ENGINE=OLAP
        duplicate KEY(`col_1`, col_2, col_3, col_4, col_5, col_6, col_7,  col_9, col_10, col_11)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`col_1`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
         INSERT INTO every_type_table VALUES 
        (1, 2, 3, 4, 11.22, 'a', 'b', '2023-01-01', '2023-01-01 00:01:02', true, 'aaa'),
        (2, 3, 4, 5, 22.33, 'b', 'c', '2023-01-02', '2023-01-02 00:01:02', false, 'bbb'),
        (3, 4, 5, 6, 33.44, 'c', 'd', '2023-01-03', '2023-01-03 00:01:02', true, 'ccc'),
        (4, 5, 6, 7, 44.55, 'd', 'e', '2023-01-04', '2023-01-04 00:01:02', false, 'ddd'),
        (5, 6, 7, 8, 55.66, 'e', 'f', '2023-01-05', '2023-01-05 00:01:02', true, 'eee'),
        (6, 7, 8, 9, 66.77, 'f', 'g', '2023-01-06', '2023-01-06 00:01:02', false, 'fff'),
        (7, 8, 9, 10, 77.88, 'g', 'h', '2023-01-07', '2023-01-07 00:01:02', true, 'ggg'),
        (8, 9, 10, 11, 88.99, 'h', 'i', '2023-01-08', '2023-01-08 00:01:02', false, 'hhh'),
        (9, 10, 11, 12, 99.1, 'i', 'j', '2023-01-09', '2023-01-09 00:01:02', true, 'iii'),
        (10, 11, 12, 13, 101.2, 'j', 'k', '2023-01-10', '2023-01-10 00:01:02', false, 'jjj'),
        (11, 12, 13, 14, 102.2, 'l', 'k', '2023-01-11', '2023-01-11 00:01:02', true, 'kkk'),
        (12, 13, 14, 15, 103.2, 'm', 'l', '2023-01-12', '2023-01-12 00:01:02', false, 'lll'),
        (13, 14, 15, 16, 104.2, 'n', 'm', '2023-01-13', '2023-01-13 00:01:02', true, 'mmm'),
        (14, 15, 16, 17, 105.2, 'o', 'n', '2023-01-14', '2023-01-14 00:01:02', false, 'nnn'),
        (15, 16, 17, 18, 106.2, 'p', 'o', '2023-01-15', '2023-01-15 00:01:02', true, 'ooo'),
        (15, 16, 17, 18, 106.2, 'q', 'p', '2023-01-16', '2023-01-16 00:01:02', false, 'ppp'),
        (16, 17, 18, 19, 107.2, 'r', 'q', '2023-01-17', '2023-01-17 00:01:02', true, 'qqq'),
        (17, 18, 19, 20, 108.2, 's', 'r', '2023-01-18', '2023-01-18 00:01:02', false, 'rrr'),
        (18, 19, 20, 21, 109.2, 't', 's', '2023-01-19', '2023-01-19 00:01:02', true, 'sss'),
        (19, 20, 21, 22, 110.2, 'v', 't', '2023-01-20', '2023-01-20 00:01:02', false, 'ttt'),
        (20, 21, 22, 23, 111.2, 'u', 'u', '2023-01-21', '2023-01-21 00:01:02', true, 'uuu'),
        (21, 22, 23, 24, 112.2, 'w', 'v', '2023-01-22', '2023-01-22 00:01:02', false, 'vvv'),
        (22, 23, 24, 25, 113.2, 'x', 'w', '2023-01-23', '2023-01-23 00:01:02', true, 'www'),
        (23, 24, 25, 26, 114.2, 'y', 'x', '2023-01-24', '2023-01-24 00:01:02', false, 'xxx'),
        (24, 25, 26, 27, 115.2, 'z', 'y', '2023-01-25', '2023-01-25 00:01:02', true, 'yyy'),
        (25, 26, 27, 28, 116.2, 'a', 'z', '2023-01-26', '2023-01-26 00:01:02', false, 'zzz'),
        (26, 27, 28, 29, 117.2, 'b', 'a', '2023-01-27', '2023-01-27 00:01:02', true, 'aaa'),
        (27, 28, 29, 30, 118.2, 'c', 'b', '2023-01-28', '2023-01-28 00:01:02', false, 'bbb'),
        (28, 29, 30, 31, 119.2, 'd', 'c', '2023-01-29', '2023-01-29 00:01:02', true, 'ccc');
    """

    sql "DELETE FROM every_type_table WHERE col_1 = 1"
    sql "DELETE FROM every_type_table WHERE col_2 = 3"
    sql "DELETE FROM every_type_table WHERE col_3 = 5"
    sql "DELETE FROM every_type_table WHERE col_4 = 7"
    sql "DELETE FROM every_type_table WHERE col_5 = 55.66"
    sql "DELETE FROM every_type_table WHERE col_6 = 'f'"
    sql "DELETE FROM every_type_table WHERE col_7 = 'h'"
    sql "DELETE FROM every_type_table WHERE col_8 = 'hhh'"
    sql "DELETE FROM every_type_table WHERE col_9 = '2023-01-09'"
    sql "DELETE FROM every_type_table WHERE col_10 = '2023-01-10 00:01:02'"
    sql "DELETE FROM every_type_table WHERE col_11 = true"
    sql "DELETE FROM every_type_table WHERE col_1 >= 27"
    sql "DELETE FROM every_type_table WHERE col_1 != 26 and col_1 != 25 and col_1 != 24 and col_1 != 23"

    qt_check_data7 """ select * from  every_type_table order by col_1; """
}
