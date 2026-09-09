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

suite("test_group_join_no_agg") {
    sql "DROP TABLE IF EXISTS gj_no_agg_left"
    sql "DROP TABLE IF EXISTS gj_no_agg_right"
    sql """CREATE TABLE gj_no_agg_left (k INT NULL, s VARCHAR(32) NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 4
        PROPERTIES ("replication_num"="1")"""
    sql "CREATE TABLE gj_no_agg_right LIKE gj_no_agg_left"
    sql "INSERT INTO gj_no_agg_left VALUES (1,'a'),(1,'a'),(2,'b'),(3,'c'),(NULL,'n'),(4,NULL)"
    sql "INSERT INTO gj_no_agg_right VALUES (1,'a'),(1,'a'),(2,'b'),(5,'e'),(NULL,'n'),(4,NULL)"
    sql "SET enable_sql_cache=false"
    sql "SET query_cache_force_refresh=true"
    sql "SET enable_bucket_shuffle_join=false"
    sql "SET enable_runtime_filter_prune=false"
    def query = """SELECT l.k FROM gj_no_agg_left l
        JOIN [shuffle] gj_no_agg_right r ON l.k=r.k GROUP BY l.k ORDER BY l.k"""
    for (serial in [false, true]) {
        sql "SET experimental_use_serial_exchange=${serial}"
        for (mode in ['OFF', 'GLOBAL']) {
            sql "SET runtime_filter_mode='${mode}'"
            sql "SET experimental_enable_group_join_fusion=false"
            def reference = sql query
            sql "SET experimental_enable_group_join_fusion=true"
            explain {
                sql query
                contains "VGROUP JOIN"
            }
            for (i in 0..<5) {
                assertEquals(reference, sql(query))
            }
            qt_keys query
            qt_string """SELECT l.s FROM gj_no_agg_left l
                JOIN [shuffle] gj_no_agg_right r ON l.s=r.s GROUP BY l.s ORDER BY l.s"""
            qt_composite """SELECT l.s,l.k FROM gj_no_agg_left l
                JOIN [shuffle] gj_no_agg_right r ON l.k=r.k AND l.s=r.s
                GROUP BY l.s,l.k ORDER BY l.s,l.k"""
        }
    }
    sql "SET runtime_filter_mode='OFF'"
    // More groups than a single output batch, with duplicate input keys.
    sql "TRUNCATE TABLE gj_no_agg_left"
    sql "TRUNCATE TABLE gj_no_agg_right"
    sql "INSERT INTO gj_no_agg_left SELECT number % 5000,'x' FROM numbers('number'='10000')"
    sql "INSERT INTO gj_no_agg_right SELECT number % 5000,'x' FROM numbers('number'='10000')"
    sql "SET batch_size=1024"
    sql "SET experimental_enable_group_join_fusion=false"
    def reference = sql query
    sql "SET experimental_enable_group_join_fusion=true"
    explain {
        sql query
        contains "VGROUP JOIN"
    }
    assertEquals(reference, sql(query))
    // Empty build and empty probe must both produce no groups.
    sql "TRUNCATE TABLE gj_no_agg_right"
    qt_empty_build query
    sql "INSERT INTO gj_no_agg_right VALUES (1,'a')"
    sql "TRUNCATE TABLE gj_no_agg_left"
    qt_empty_probe query
}
