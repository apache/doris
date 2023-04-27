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

suite("test_insert") {
    // todo: test insert, such as insert values, insert select, insert txn
    sql "show load"
    def test_baseall = "test_query_db.baseall";
    def test_bigtable = "test_query_db.bigtable";
    def insert_tbl = "test_insert_tbl";

    sql """ DROP TABLE IF EXISTS ${insert_tbl}"""
    sql """
     CREATE TABLE ${insert_tbl} (
       `k1` char(5) NULL,
       `k2` int(11) NULL,
       `k3` tinyint(4) NULL,
       `k4` int(11) NULL
     ) ENGINE=OLAP
     DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`)
     COMMENT 'OLAP'
     DISTRIBUTED BY HASH(`k1`) BUCKETS 5
     PROPERTIES (
       "replication_num"="1"
     );
    """

    sql """ 
    INSERT INTO ${insert_tbl}
    SELECT a.k6, a.k3, b.k1
    	, sum(b.k1) OVER (PARTITION BY a.k6 ORDER BY a.k3) AS w_sum
    FROM ${test_baseall} a
    	JOIN ${test_bigtable} b ON a.k1 = b.k1 + 5
    ORDER BY a.k6, a.k3, a.k1, w_sum
    """

    qt_sql1 "select * from ${insert_tbl} order by 1, 2, 3, 4"
}
