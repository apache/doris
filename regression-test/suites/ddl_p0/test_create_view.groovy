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

suite("test_create_view") {

    sql """DROP TABLE IF EXISTS count_distinct"""
    sql """
        CREATE TABLE IF NOT EXISTS count_distinct
        (
            RQ DATE NOT NULL  COMMENT "日期",
            v1 VARCHAR(100) NOT NULL  COMMENT "字段1",
            v2 VARCHAR(100) NOT NULL  COMMENT "字段2",
            v3 VARCHAR(100) REPLACE_IF_NOT_NULL  COMMENT "字段3"
        )
        AGGREGATE KEY(RQ,v1,v2)
        PARTITION BY RANGE(RQ)
        (
            PARTITION p20220908 VALUES LESS THAN ('2022-09-09')
        )
        DISTRIBUTED BY HASH(v1,v2) BUCKETS 3
        PROPERTIES(
        "replication_num" = "1",
        "dynamic_partition.enable" = "true",
        "dynamic_partition.time_unit" = "DAY",
        "dynamic_partition.start" = "-3",
        "dynamic_partition.end" = "3",
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.buckets" = "3"
        );
    """
    sql """
    CREATE VIEW IF NOT EXISTS test_count_distinct
    (
        RQ comment "日期",
        v1 comment "v1",
        v2 comment "v2",
        v3 comment "v3"
    )
    AS
    select aa.RQ as RQ, aa.v1 as v1,aa.v2 as v2 , bb.v3 as v3  from
    (
        select RQ, count(distinct v1) as v1 , count(distinct  v2 ) as v2
        from count_distinct
        group by RQ
    ) aa
    LEFT JOIN
    (
        select RQ, max(v3) as v3
        from count_distinct
        group by RQ
    ) bb
    on aa.RQ = bb.RQ;
    """

    sql """select * from test_count_distinct"""
    sql """DROP VIEW IF EXISTS test_count_distinct"""
    sql """DROP TABLE IF EXISTS count_distinct"""

    sql """DROP TABLE IF EXISTS t1"""
    sql """
    CREATE TABLE `t1` (
        k1 int,
        k2 date,
        v1 int
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`,`k2`)
        COMMENT '测试'
        PARTITION BY RANGE(k2) (
        PARTITION p1 VALUES [('2023-07-01'), ('2023-07-10')),
        PARTITION p2 VALUES [('2023-07-11'), ('2023-07-20'))
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );"""
    sql """DROP TABLE IF EXISTS t2"""
    sql """
    CREATE TABLE `t2` (
        k1 int,
        k2 date,
        v1 int
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`,`k2`)
        COMMENT '测试'
        PARTITION BY RANGE(k2) (
        PARTITION p1 VALUES [('2023-07-01'), ('2023-07-05')),
        PARTITION p2 VALUES [('2023-07-05'), ('2023-07-15'))
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        ); """
    sql """
        CREATE VIEW IF NOT EXISTS my_view AS
        SELECT t1.* FROM t1 PARTITION(p1) JOIN t2 PARTITION(p2) ON t1.k1 = t2.k1; """
    sql """SELECT * FROM my_view"""
    sql """DROP VIEW IF EXISTS my_view"""
    sql """DROP TABLE IF EXISTS t1"""
    sql """DROP TABLE IF EXISTS t2"""


    sql """DROP TABLE IF EXISTS view_baseall"""
    sql """DROP VIEW IF EXISTS test_view7"""
    sql """DROP VIEW IF EXISTS test_view8"""
    sql """
        CREATE TABLE `view_baseall` (
            `k1` int(11) NULL,
            `k3` array<int> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
    """
    sql """insert into view_baseall values(1,[1,2,3]);"""
    sql """insert into view_baseall values(2,[10,-2,8]);"""
    sql """insert into view_baseall values(3,[-1,20,0]);"""

    qt_test_view_1 """ select * from view_baseall order by k1; """
    qt_test_view_2 """ select *, array_map(x->x>0,k3) from view_baseall order by k1; """
    qt_test_view_3 """ select *, array_filter(x->x>0,k3),array_filter(`k3`, array_map(x -> x > 0, `k3`)) from view_baseall order by k1; """


    sql """
    create view IF NOT EXISTS test_view7 (k1,k2,k3,k4) as
            select *, array_filter(x->x>0,k3),array_filter(`k3`, array_map(x -> x > 0, `k3`)) from view_baseall order by k1;
    """
    qt_test_view_4 """ select * from test_view7 order by k1; """

    sql """
    create view IF NOT EXISTS test_view8 (k1,k2,k3) as
            select *, array_map(x->x>0,k3) from view_baseall order by k1;
    """
    qt_test_view_5 """ select * from test_view8 order by k1; """

    sql """DROP TABLE IF EXISTS view_column_name_test"""
    sql """
     CREATE TABLE IF NOT EXISTS view_column_name_test
    (
        `timestamp` DATE NOT NULL COMMENT "['0000-01-01', '9999-12-31']",
        `type` TINYINT NOT NULL COMMENT "[-128, 127]",
        `error_code` INT COMMENT "[-2147483648, 2147483647]",
        `error_msg` VARCHAR(300) COMMENT "[1-65533]",
        `op_id` BIGINT COMMENT "[-9223372036854775808, 9223372036854775807]",
        `op_time` DATETIME COMMENT "['0000-01-01 00:00:00', '9999-12-31 23:59:59']",
        `target` float COMMENT "4 字节",
        `source` double COMMENT "8 字节",
        `lost_cost` decimal(12,2) COMMENT "",
        `remark` string COMMENT "1m size",
        `op_userid` LARGEINT COMMENT "[-2^127 + 1 ~ 2^127 - 1]",
        `plate` SMALLINT COMMENT "[-32768, 32767]",
        `iscompleted` boolean COMMENT "true 或者 false"
    )
    DISTRIBUTED BY HASH(`type`) BUCKETS 1
    PROPERTIES ('replication_num' = '1');
    """

    sql """
    DROP VIEW IF EXISTS v1
    """
    sql """
    CREATE VIEW v1 AS 
    SELECT
      error_code, 
      1, 
      'string', 
      now(), 
      dayofyear(op_time), 
      cast (source AS BIGINT), 
      min(`timestamp`) OVER (
        ORDER BY 
          op_time DESC ROWS BETWEEN UNBOUNDED PRECEDING
          AND 1 FOLLOWING
      ), 
      1 > 2,
      2 + 3,
      1 IN (1, 2, 3, 4), 
      remark LIKE '%like', 
      CASE WHEN remark = 's' THEN 1 ELSE 2 END,
      TRUE | FALSE 
    FROM 
      view_column_name_test
    """
    qt_test_view_6 """ SHOW VIEW FROM view_column_name_test;"""
}
