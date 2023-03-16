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
}
