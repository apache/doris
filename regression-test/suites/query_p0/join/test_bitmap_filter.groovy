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

suite("test_bitmap_filter", "query_p0") {
    def tbl1 = "bigtable"
    def tbl2 = "bitmap_table"
    def tbl3 = "baseall"

    sql "set runtime_filter_type = 16"
    sql "set enable_vectorized_engine = true"
    sql "use test_query_db"
    sql "DROP TABLE IF EXISTS ${tbl2}"
    sql """
    CREATE TABLE ${tbl2} (
      `k1` int(11) NULL,
      `k2` bitmap BITMAP_UNION NULL,
      `k3` bitmap BITMAP_UNION NULL
    ) ENGINE=OLAP
    AGGREGATE KEY(`k1`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`k1`) BUCKETS 2
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """insert into bitmap_table values (1, bitmap_from_string('1, 3, 5, 7, 9, 11, 13, 99'),
    bitmap_from_string('32767, 1985, 255, 789, 1991')),(2, bitmap_from_string('10, 11, 12, 13, 14'), bitmap_empty());"""

    qt_sql1 "select k1, k2 from ${tbl1} where k1 in (select k2 from ${tbl2}) order by k1;"

    qt_sql2 "select k1, k2 from ${tbl1} where k1 + 1 in (select k2 from ${tbl2}) order by k1;"

    qt_sql3 "select k1, k2 from ${tbl1} where k1 not in (select k2 from ${tbl2} where k1 = 1) order by k1;"

    qt_sql4 "select t1.k1, t1.k2 from ${tbl1} t1 join ${tbl3} t3 on t1.k1 = t3.k1 where t1.k1 in (select k2 from ${tbl2} where k1 = 1) order by t1.k1;"

    qt_sql5 "select k1, k2 from ${tbl1} where k1 in (select k2 from ${tbl2}) and k2 not in (select k3 from ${tbl2}) order by k1;"

    qt_sql6 "select k2, count(k2) from ${tbl1} where k1 in (select k2 from ${tbl2}) group by k2 order by k2;"
}
