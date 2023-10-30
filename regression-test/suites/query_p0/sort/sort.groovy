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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("sort") {
    qt_sort_string_single_column """ select * from ( select '汇总' as a union all select '2022-01-01' as a ) a order by 1 """
    qt_sort_string_multiple_columns """ select * from ( select '汇总' as a,1 as b union all select '2022-01-01' as a,1 as b ) a order by 1,2 """
    qt_sort_string_on_fe """ select '汇总' > '2022-01-01' """

    sql """CREATE TABLE IF NOT EXISTS Test2PhaseSortWhenAggTable
    (`l1` VARCHAR(20) NOT NULL, `l2` VARCHAR(20) NOT NULL, `id` INT REPLACE NOT NULL, `maximum` INT MAX DEFAULT "0" )
    ENGINE=olap AGGREGATE KEY(`l1`, `l2`) PARTITION BY LIST(`l1`, `l2`) ( PARTITION `p1` VALUES IN (("a", "a"), ("b", "b"), ("c", "c")),
    PARTITION `p2` VALUES IN (("d", "d"), ("e", "e"), ("f", "f")), PARTITION `p3` VALUES IN (("g", "g"), ("h", "h"), ("i", "i")) ) DISTRIBUTED BY HASH(`l1`) BUCKETS 2 PROPERTIES ( "replication_num" = "1" )"""

    sql """insert into Test2PhaseSortWhenAggTable values ("a", "a", 1, 1), ("b", "b", 3, 2), ("c", "c", 3, 3), ("d", "d", 4, 4), ("e", "e", 5, 5), ("f", "f", 6, 6), ("g", "g", 7, 7), ("h", "h", 8, 8), ("i", "i", 9, 9)"""

    qt_sql """
    SELECT /*+ SET_VAR(query_timeout = 600) */ ref_0.`l1` AS c0,
                                           bitmap_empty() AS c1,
                                           ref_0.`l1` AS c2
    FROM Test2PhaseSortWhenAggTable AS ref_0
    WHERE ref_0.`l2` IS NOT NULL
    ORDER BY ref_0.`l1` DESC
    LIMIT 110
    OFFSET 130
    """

    sql """drop table if exists tbl1"""
    sql """create table tbl1 (k1 varchar(100), k2 string) distributed by hash(k1) buckets 1 properties("replication_num" = "1");"""
    sql """insert into tbl1 values(1, "alice");"""
    sql """insert into tbl1 values(2, "bob");"""
    sql """insert into tbl1 values(3, "mark");"""
    sql """insert into tbl1 values(4, "thor");"""
    qt_sql """select cast(k1 as INT) as id from tbl1 order by id;"""
    qt_sql """select cast(k1 as INT) % 2 as id from tbl1 order by id;"""
    qt_sql """select cast(k1 as BIGINT) as id from tbl1 order by id;"""
    qt_sql """select cast(k1 as STRING) as id from tbl1 order by id;"""
    qt_sql """select cast(k1 as INT) as id from tbl1 order by id limit 2"""
    qt_sql """select cast(k1 as STRING) as id from tbl1 order by id limit 2"""

	
    sql """drop table if exists test_convert"""
    sql """CREATE TABLE `test_convert` (
                 `a` varchar(100) NULL
             ) ENGINE=OLAP
               DUPLICATE KEY(`a`)
               DISTRIBUTED BY HASH(`a`) BUCKETS 3
               PROPERTIES (
               "replication_allocation" = "tag.location.default: 1"
               );"""
    sql """insert into test_convert values("b"),("z"),("a"), ("c"), ("睿"), ("多"), ("丝");"""
    qt_sql """select * from test_convert order by convert(a using gbk);"""

    sql """ DROP TABLE if exists `sort_non_overlap`; """
    sql """ CREATE TABLE `sort_non_overlap` (
      `time_period` datetime NOT NULL,
      `area_name` varchar(255) NOT NULL,
      `province` varchar(255) NOT NULL,
      `res_name` varchar(255) NOT NULL,
      `dev` varchar(255) NOT NULL,
      `dec0` decimal(10, 3) REPLACE_IF_NOT_NULL NULL,
      `dec1` decimal(10, 3) REPLACE_IF_NOT_NULL NULL,
      `dec2` decimal(10, 3) REPLACE_IF_NOT_NULL NULL,
      `dec3` decimal(10, 3) REPLACE_IF_NOT_NULL NULL,
      `update_time` datetime REPLACE NULL
    ) ENGINE=OLAP
    AGGREGATE KEY(`time_period`, `area_name`, `province`, `res_name`, `dev`)
    DISTRIBUTED BY HASH(`area_name`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "disable_auto_compaction" = "true"
    );
    """

    sql """ insert into sort_non_overlap values
        ('2023-03-21 06:00:00', 'area1', 'p0', 'aaaaa', 'ddddd1', 100, 100, 100, 100, '2023-03-21 17:00:00'),
        ('2023-03-21 07:00:00', 'area1', 'p0', 'aaaaa', 'ddddd2', 100, 100, 100, 100, '2023-03-21 17:00:00');
    """

    sql """ insert into sort_non_overlap values
                ('2023-03-21 08:00:00', 'area1', 'p0', 'aaaaa', 'ddddd5', 100, 100, 100, 100, '2023-03-21 17:00:00'),
                ('2023-03-21 09:00:00', 'area1', 'p0', 'aaaaa', 'ddddd6', 100, 100, 100, 100, '2023-03-21 17:00:00');
    """

    qt_sql_orderby_non_overlap_desc """ select * from sort_non_overlap order by time_period desc limit 4; """


    // test topn 2phase opt with light schema change
    sql """set topn_opt_limit_threshold = 1024"""
    sql """set enable_two_phase_read_opt= true"""
    sql """ DROP TABLE if exists `sort_default_value`; """
    sql """ CREATE TABLE `sort_default_value` (
      `k1` int NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "disable_auto_compaction" = "true"
    );
    """
    sql "insert into sort_default_value values (1)"
    sql "insert into sort_default_value values (2)"
    sql """ alter table sort_default_value add column k4 INT default "1024" """
    sql "insert into sort_default_value values (3, 0)"
    sql "insert into sort_default_value values (4, null)"
    qt_sql "select * from sort_default_value order by k1 limit 10"
    explain {
        sql("select * from sort_default_value order by k1 limit 10")
        contains "OPT TWO PHASE"
    }

    def tblName = "sort_one_float_column"
    sql """ DROP TABLE IF EXISTS ${tblName} """
    sql """ CREATE TABLE `${tblName}` (
      `time_period` datetime NOT NULL,
      `dc` double NULL,
      `ic` int NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`time_period`)
    DISTRIBUTED BY HASH(`time_period`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "disable_auto_compaction" = "true"
    );


    """
    StringBuilder sb = new StringBuilder("""INSERT INTO ${tblName} values """)
    for (int i = 1; i <= 1024; i++) {
        sb.append("""
            ('${i+2000}-03-21 08:00:00', ${i}.${i},${i}),
        """)
    }
    sb.append("""('2023-03-21 08:00:00', 1.1,1)""")
    sql """ ${sb.toString()} """

    qt_order_by_float """ select /*SET_VAR(parallel_pipeline_task_num=1,parallel_fragment_exec_instance_num=1)*/ * from ${tblName} order by dc; """
    qt_order_by_int """ select /*SET_VAR(parallel_pipeline_task_num=1,parallel_fragment_exec_instance_num=1)*/ * from ${tblName} order by ic; """
    qt_order_by_uint """ select /*SET_VAR(parallel_pipeline_task_num=1,parallel_fragment_exec_instance_num=1)*/ * from ${tblName} order by time_period; """
}
