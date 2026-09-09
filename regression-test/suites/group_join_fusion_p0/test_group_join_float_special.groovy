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

suite("test_group_join_float_special") {
    sql "SET enable_sql_cache=false"
    sql "SET query_cache_force_refresh=true"
    sql "SET enable_bucket_shuffle_join=false"
    sql "SET experimental_use_serial_exchange=false"
    sql "SET runtime_filter_mode='OFF'"
    sql "SET experimental_enable_group_join_fusion=true"
    // Isolate key normalization in GroupJoin from upstream exchange hashing.
    sql "SET parallel_pipeline_task_num=1"
    for (type in ["FLOAT", "DOUBLE"]) {
        sql "DROP TABLE IF EXISTS gj_float_special_left"
        sql "DROP TABLE IF EXISTS gj_float_special_right"
        sql """CREATE TABLE gj_float_special_left (
            id INT NOT NULL, k ${type} NULL, v BIGINT NULL
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES ("replication_num"="1")"""
        sql "CREATE TABLE gj_float_special_right LIKE gj_float_special_left"
        sql """INSERT INTO gj_float_special_left VALUES
            (1,CAST('-0.0' AS ${type}),20),(2,CAST('0.0' AS ${type}),30)"""
        sql """INSERT INTO gj_float_special_right VALUES
            (11,CAST('-0.0' AS ${type}),2),(12,CAST('0.0' AS ${type}),3)"""
        def query = """SELECT l.k, COUNT(*), SUM(l.v), SUM(r.v)
            FROM gj_float_special_left l JOIN [shuffle] gj_float_special_right r
            ON l.k=r.k GROUP BY l.k ORDER BY l.k"""
        explain {
            sql query
            contains "VGROUP JOIN"
        }
        qt_signed_zero query
        qt_no_agg """SELECT l.k FROM gj_float_special_left l
            JOIN [shuffle] gj_float_special_right r ON l.k=r.k
            GROUP BY l.k ORDER BY l.k"""
        // NULL must still be excluded by ordinary equality. Infinities remain distinct.
        sql """INSERT INTO gj_float_special_left VALUES
            (3,NULL,100),(4,CAST('NaN' AS ${type}),40),
            (5,CAST('Infinity' AS ${type}),50),(6,CAST('-Infinity' AS ${type}),60)"""
        sql """INSERT INTO gj_float_special_right VALUES
            (13,NULL,10),(14,CAST('NaN' AS ${type}),4),
            (15,CAST('Infinity' AS ${type}),5),(16,CAST('-Infinity' AS ${type}),6)"""
        qt_special_values query
    }
}
