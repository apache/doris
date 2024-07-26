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

    // test delete after light schema change
    sql """ALTER TABLE ${tableName} ADD COLUMN c4 int;"""
    sql """delete from ${tableName} where `c4` = 1;"""
    qt_sql """select count(*) from ${tableName};"""

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
    `pay_time` datetime DEFAULT NULL COMMENT '付款时间'
    )  ENGINE=OLAP
    DUPLICATE KEY(`tenant_id`)
    COMMENT "付款明细"
    PARTITION BY RANGE(`pay_time` ) (
    PARTITION p202012 VALUES LESS THAN ('2021-01-01 00:00:00')
    )
    DISTRIBUTED BY HASH(`tenant_id`) BUCKETS auto
    PROPERTIES
    (
        "replication_num" = "1"
    );
    """

    sql "delete from dwd_pay partitions(p202012) where pay_time = '20231002';"

    sql """
    ADMIN SET FRONTEND CONFIG ('disable_decimalv2' = 'false');
    """

    sql "drop table if exists test"
    sql """
    CREATE TABLE `test`  
    (
            col_1 int,
            col_2 decimalv2(10,3)
    )ENGINE=OLAP
    duplicate KEY(`col_1`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`col_1`) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql "DELETE FROM test  WHERE col_2 = cast(123.45 as decimalv2(10,3));"

    // add every type of delete
    sql "drop table if exists every_type_table"
    sql  "ADMIN SET FRONTEND CONFIG ('disable_decimalv2' = 'false')"
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
            col_12 decimalv2(10,3),
            col_8 string
        ) ENGINE=OLAP
        duplicate KEY(`col_1`, col_2, col_3, col_4, col_5, col_6, col_7,  col_9, col_10, col_11, col_12)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`col_1`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
         INSERT INTO every_type_table VALUES 
        (1, 2, 3, 4, 11.22, 'a', 'b', '2023-01-01', '2023-01-01 00:01:02', true, 11.22, 'aaa'),
        (2, 3, 4, 5, 22.33, 'b', 'c', '2023-01-02', '2023-01-02 00:01:02', false, 22.33,'bbb'),
        (3, 4, 5, 6, 33.44, 'c', 'd', '2023-01-03', '2023-01-03 00:01:02', true, 33.44, 'ccc'),
        (4, 5, 6, 7, 44.55, 'd', 'e', '2023-01-04', '2023-01-04 00:01:02', false, 44.55, 'ddd'),
        (5, 6, 7, 8, 55.66, 'e', 'f', '2023-01-05', '2023-01-05 00:01:02', true, 55.66, 'eee'),
        (6, 7, 8, 9, 66.77, 'f', 'g', '2023-01-06', '2023-01-06 00:01:02', false, 66.77, 'fff'),
        (7, 8, 9, 10, 77.88, 'g', 'h', '2023-01-07', '2023-01-07 00:01:02', true, 77.88, 'ggg'),
        (8, 9, 10, 11, 88.99, 'h', 'i', '2023-01-08', '2023-01-08 00:01:02', false, 88.99, 'hhh'),
        (9, 10, 11, 12, 99.1, 'i', 'j', '2023-01-09', '2023-01-09 00:01:02', true, 99.100, 'iii'),
        (10, 11, 12, 13, 101.2, 'j', 'k', '2023-01-10', '2023-01-10 00:01:02', false, 100.101, 'jjj'),
        (11, 12, 13, 14, 102.2, 'l', 'k', '2023-01-11', '2023-01-11 00:01:02', true, 101.102, 'kkk'),
        (12, 13, 14, 15, 103.2, 'm', 'l', '2023-01-12', '2023-01-12 00:01:02', false, 102.103, 'lll'),
        (13, 14, 15, 16, 104.2, 'n', 'm', '2023-01-13', '2023-01-13 00:01:02', true, 103.104, 'mmm'),
        (14, 15, 16, 17, 105.2, 'o', 'n', '2023-01-14', '2023-01-14 00:01:02', false, 11111.105, 'nnn'),
        (15, 16, 17, 18, 106.2, 'p', 'o', '2023-01-15', '2023-01-15 00:01:02', true, 22222.106, 'ooo'),
        (15, 16, 17, 18, 106.2, 'q', 'p', '2023-01-16', '2023-01-16 00:01:02', false, 22223.106, 'ppp'),
        (16, 17, 18, 19, 107.2, 'r', 'q', '2023-01-17', '2023-01-17 00:01:02', true, 22224.106, 'qqq'),
        (17, 18, 19, 20, 108.2, 's', 'r', '2023-01-18', '2023-01-18 00:01:02', false, 22225.106, 'rrr'),
        (18, 19, 20, 21, 109.2, 't', 's', '2023-01-19', '2023-01-19 00:01:02', true, 22226.106, 'sss'),
        (19, 20, 21, 22, 110.2, 'v', 't', '2023-01-20', '2023-01-20 00:01:02', false, 22227.106, 'ttt'),
        (20, 21, 22, 23, 111.2, 'u', 'u', '2023-01-21', '2023-01-21 00:01:02', true, 22228.106, 'uuu'),
        (21, 22, 23, 24, 112.2, 'w', 'v', '2023-01-22', '2023-01-22 00:01:02', false, 22229.106, 'vvv'),
        (22, 23, 24, 25, 113.2, 'x', 'w', '2023-01-23', '2023-01-23 00:01:02', true, 22210.106, 'www'),
        (23, 24, 25, 26, 114.2, 'y', 'x', '2023-01-24', '2023-01-24 00:01:02', false, 22211.106, 'xxx'),
        (24, 25, 26, 27, 115.2, 'z', 'y', '2023-01-25', '2023-01-25 00:01:02', true, 22212.106, 'yyy'),
        (25, 26, 27, 28, 116.2, 'a', 'z', '2023-01-26', '2023-01-26 00:01:02', false, 22213.106, 'zzz'),
        (26, 27, 28, 29, 117.2, 'b', 'a', '2023-01-27', '2023-01-27 00:01:02', true, 22214.106, 'aaa'),
        (27, 28, 29, 30, 118.2, 'c', 'b', '2023-01-28', '2023-01-28 00:01:02', false, 22215.106, 'bbb'),
        (28, 29, 30, 31, 119.2, 'd', 'c', '2023-01-29', '2023-01-29 00:01:02', true, 22216.106, 'ccc'),
        (28, 29, 31, 32, 120.2, 'q', 'd', '2023-01-30', '2023-01-30 00:01:02', true, 22217.106, 'ccc'),
        (29, 30, 32, 33, 121.2, 'w', 'e', '2023-01-31', '2023-01-31 00:01:02', false, 22218.106, 'dd'),
        (30, 31, 33, 34, 122.2, 'e', 'f', '2023-02-01', '2023-02-01 00:01:02', true, 22219.106, 'ee'),
        (31, 32, 34, 35, 123.2, 'r', 'g', '2023-02-02', '2023-02-02 00:01:02', false, 22220.106, 'ff'),
        (32, 33, 35, 36, 124.2, 't', 'h', '2023-02-03', '2023-02-03 00:01:02', true, 22221.106, 'gg'),
        (33, 34, 36, 37, 125.2, 'y', 'i', '2023-02-04', '2023-02-04 00:01:02', false, 22230.106, 'hh'),
        (34, 35, 37, 38, 126.2, 'u', 'j', '2023-02-05', '2023-02-05 00:01:02', true, 22231.106, 'ii'),
        (35, 36, 38, 39, 127.2, 'i', 'k', '2023-02-06', '2023-02-06 00:01:02', false, 22232.106, 'jj'),
        (36, 37, 39, 40, 128.2, 'o', 'l', '2023-02-07', '2023-02-07 00:01:02', true, 22233.106, 'kk'),
        (37, 38, 40, 41, 129.2, 'p', 'm', '2023-02-08', '2023-02-08 00:01:02', false, 22234.106, 'll'),
        (38, 39, 41, 42, 130.2, 'a', 'n', '2023-02-09', '2023-02-09 00:01:02', true, 22235.106, 'mm'),
        (39, 40, 42, 43, 131.2, 's', 'o', '2023-02-10', '2023-02-10 00:01:02', false, 22236.106, 'nn'),
        (40, 41, 43, 44, 132.2, 'd', 'p', '2023-02-11', '2023-02-11 00:01:02', true, 22237.106, 'oo'),
        (41, 42, 44, 45, 133.2, 'f', 'r', '2023-02-12', '2023-02-12 00:01:02', false, 22238.106, 'pp'),
        (42, 43, 45, 46, 134.2, 'g', 's', '2023-02-13', '2023-02-13 00:01:02', true, 22239.106, 'qq'),
        (43, 44, 46, 47, 135.2, 'g', 't', '2023-02-14', '2023-02-14 00:01:02', false, 22240.106, 'rr'),
        (44, 45, 47, 48, 136.2, 'h', 'u', '2023-02-15', '2023-02-15 00:01:02', true, 2222222.106, 'ss'),
        (45, 46, 48, 49, 137.2, 'j', 'v', '2023-02-16', '2023-02-16 00:01:02', false, 2222223.106, 'tt'),
        (46, 47, 49, 50, 138.2, 'k', 'w', '2023-02-17', '2023-02-17 00:01:02', true, 2222224.106, 'uu'),
        (47, 48, 50, 51, 139.2, 'l', 'x', '2023-02-18', '2023-02-18 00:01:02', false, 2222225.106, 'vv'),
        (48, 49, 51, 52, 140.2, 'z', 'y', '2023-02-19', '2023-02-19 00:01:02', true, 2222226.106, 'ww');
        
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
    // todo: add the following delete check, now it's not supported
    // sql "DELETE FROM every_type_table WHERE col_12 = 2222226.106"
    sql "DELETE FROM every_type_table WHERE col_12 in (2222226.106, 2222225.106, 2222224.106, 2222223.106)"
    sql "DELETE FROM every_type_table WHERE col_1 in (9, 48, 47, 46)"
    sql "DELETE FROM every_type_table WHERE col_2 in (10, 11)"
    sql "DELETE FROM every_type_table WHERE col_3 in (12, 13)"
    sql "DELETE FROM every_type_table WHERE col_4 in (14, 15)"
    sql "DELETE FROM every_type_table WHERE col_5 in (66.77, 88.99)"
    sql "DELETE FROM every_type_table WHERE col_6 in ('u', 'v')"
    sql "DELETE FROM every_type_table WHERE col_7 in ('i', 'j')"
    sql "DELETE FROM every_type_table WHERE col_8 in ('xxx', 'yyy')"
    sql "DELETE FROM every_type_table WHERE col_9 in ('2023-01-09', '2023-02-01')"
    sql "DELETE FROM every_type_table WHERE col_10 in ('2023-01-11 00:01:02', '2023-01-12 00:01:02')"
    sql "DELETE FROM every_type_table WHERE col_12 in (2222226.106, 2222225.106, 2222224.106, 2222223.106)"
    sql "DELETE FROM every_type_table WHERE col_11 = true"
    sql "DELETE FROM every_type_table WHERE col_1 <= 30 and col_1 != 21 and col_1 >= 25"
    sql "DELETE FROM every_type_table WHERE col_5 >= 137.2 and col_5 != 138.2 and col_5 <= 140.2"
    sql "DELETE FROM every_type_table WHERE col_6 >= 'x' and col_6 != 'y' and col_6 <= 'z'"
    sql "DELETE FROM every_type_table WHERE col_7 >= 'i' and col_7 != 'j' and col_7 <= 'k'"
    sql "DELETE FROM every_type_table WHERE col_8 >= 'xxx' and col_8 != 'yyy' and col_8 <= 'zzz'"
    sql "DELETE FROM every_type_table WHERE col_9 >= '2023-02-17' and col_9 in ('2023-02-19')"
    sql "DELETE FROM every_type_table WHERE col_10 >= '2023-02-17 00:01:02' and col_10 in ('2023-02-17 00:01:02')"
    qt_check_data7 """ select * from  every_type_table order by col_1; """
    sql "drop table every_type_table"


    sql "drop table if exists test2"
    sql """
    CREATE TABLE `test2`  
    (
            col_1 int,
            col_2 decimalv2(10,3)
    )ENGINE=OLAP
    duplicate KEY(`col_1`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`col_1`) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql "set enable_fold_constant_by_be = true;"
    sql "DELETE FROM test2  WHERE col_2 = cast(123.45 as decimalv2(10,3));"

    sql "drop table if exists test3"
    sql """
            CREATE TABLE `test3` (
            `statistic_date` datev2 NULL,
            `project_name` varchar(20) NULL ,
            `brand` varchar(30) NULL ,
            `vehicle_status` varchar(30) NULL ,
            `abnormal_note` varchar(30) NULL ,
            `inv_qty` bigint(20) NULL ,
            `age_120_qty` bigint(20) NULL ,
            `create_date` datetime NULL ,
            `zparvin` varchar(50) NULL,
            `tonnage` varchar(50) NULL 
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`statistic_date`, `project_name`) BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            ); 
    """
    sql "set experimental_enable_nereids_planner = false;"
    sql "delete from test3 where statistic_date >= date_sub('2024-01-16',INTERVAL 1 day);"

    sql "drop table if exists bi_acti_per_period_plan"
    sql """
            CREATE TABLE `bi_acti_per_period_plan` (
            `proj_id` bigint(20) NULL,
            `proj_name` varchar(400) NULL,
            `proj_start_date` datetime NULL,
            `proj_end_date` datetime NULL,
            `last_data_date` datetime NULL,
            `data_date` datetime NULL,
            `data_batch_num` datetime NULL,
            `la_sum_base_proj_id` varchar(200) NULL,
            `sum_base_proj_id` varchar(200) NULL,
            `today_date` datetime NULL,
            `count` bigint(20) NULL,
            `count_type` varchar(50) NULL,
            `bl_count` bigint(20) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`proj_id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`proj_id`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            ); 
    """
    sql """
            INSERT INTO bi_acti_per_period_plan (proj_id,proj_name,proj_start_date,proj_end_date,last_data_date,data_date,data_batch_num,la_sum_base_proj_id,sum_base_proj_id,today_date,count,count_type,bl_count) VALUES
            (4508,'建筑工程项目A','2023-05-30 00:00:00','2024-03-07 00:00:00','2023-06-01 00:00:00','2023-08-15 00:00:00','2024-01-31 00:00:00','4509','4509','2023-08-27 00:00:00',5,'plan',4);
    """
    sql "set experimental_enable_nereids_planner = false;"
    sql "set @data_batch_num='2024-01-31 00:00:00';"
    sql "delete  from bi_acti_per_period_plan where data_batch_num =@data_batch_num; "

    // delete bitmap
    sql "drop table if exists table_bitmap"
    sql """
        CREATE TABLE if not exists `table_bitmap` (
          `dt` DATE NULL,
          `page_id` INT NULL,
          `page_level` INT NULL,
          `user_id` BITMAP NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`dt`, `page_id`, `page_level`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`dt`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into table_bitmap values
        ('2021-12-09',     101 ,          1 , BITMAP_FROM_STRING('100001,100002,100003,100004,100005')),
        ('2021-12-09',     102 ,          2 , BITMAP_FROM_STRING('100001,100003,100004')),
        ('2021-12-09',     103 ,          3 , BITMAP_FROM_STRING('100003'));
    """

    test {
        sql "delete from table_bitmap where user_id is null"
        exception "Can not apply delete condition to column type: bitmap"
    }

    // delete decimal
    sql "drop table if exists table_decimal"
    sql """
        CREATE TABLE table_decimal (
          `k1` BOOLEAN NOT NULL,
          `k2` DECIMAL(17, 1) NOT NULL,
          `k3` INT NOT NULL,
          `k4` DECIMAL(7, 7)
        ) ENGINE=OLAP 
        DUPLICATE KEY(`k1`,`k2`,`k3`) 
        DISTRIBUTED BY HASH(`k1`,`k2`,`k3`) BUCKETS 4 
        PROPERTIES (
        "replication_num" = "1",
        "disable_auto_compaction" = "false"
        ); 
    """
    sql """
        insert into table_decimal values
        (false, '-9999782574499444.2', -20, 0.1234567),
        (true, '-1', 10, 0.7654321);
    """

    sql """
        delete from table_decimal where k1 = false and k2 = '-9999782574499444.2' and k3 = '-20';
    """

    sql """
        delete from table_decimal where k4 = '0.1234567';
    """

    sql """
        delete from table_decimal where k4 = '-0.123';
    """

    qt_check_decimal """
        select * from table_decimal;
    """
}
