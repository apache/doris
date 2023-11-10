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
}
