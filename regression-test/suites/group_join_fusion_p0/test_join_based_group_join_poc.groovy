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

suite("test_join_based_group_join_poc") {
    sql "DROP TABLE IF EXISTS gj_join_poc_left"
    sql "DROP TABLE IF EXISTS gj_join_poc_right"
    sql """CREATE TABLE gj_join_poc_left (k BIGINT NOT NULL, v DOUBLE NULL)
           DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
           PROPERTIES("replication_num"="1")"""
    sql "CREATE TABLE gj_join_poc_right LIKE gj_join_poc_left"
    sql """INSERT INTO gj_join_poc_left VALUES
           (0,2), (0,3), (1,10), (1,NULL), (2,NULL), (-7,4), (99,100)"""
    sql """INSERT INTO gj_join_poc_right VALUES
           (0,5), (0,7), (1,2), (1,3), (2,NULL), (-7,6), (100,100)"""

    sql "SET agg_phase=1"
    sql "SET eager_aggregation_mode=-1"
    sql "SET enable_bucketed_hash_agg=false"
    sql "SET enable_bucket_shuffle_join=false"
    sql "SET enable_spill=false"
    sql "SET runtime_filter_mode='OFF'"
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET parallel_pipeline_task_num=1"
    sql "SET experimental_use_serial_exchange=false"

    def query = """SELECT l.k, SUM(l.v), SUM(r.v)
                   FROM gj_join_poc_left l JOIN [shuffle] gj_join_poc_right r ON l.k=r.k
                   GROUP BY l.k ORDER BY l.k"""
    sql "SET experimental_enable_group_join_fusion=false"
    order_qt_reference query
    sql "SET experimental_enable_group_join_fusion=true"
    explain {
        sql query
        contains "implementation: JOIN_BASED"
    }
    order_qt_join_based query

    def probeOnly = """SELECT l.k, SUM(l.v)
                       FROM gj_join_poc_left l JOIN [shuffle] gj_join_poc_right r ON l.k=r.k
                       GROUP BY l.k ORDER BY l.k"""
    order_qt_probe_only probeOnly
    def buildOnly = """SELECT r.k, SUM(r.v)
                       FROM gj_join_poc_left l JOIN [shuffle] gj_join_poc_right r ON l.k=r.k
                       GROUP BY r.k ORDER BY r.k"""
    order_qt_build_only buildOnly
    order_qt_empty """SELECT l.k, SUM(l.v), SUM(r.v)
                      FROM gj_join_poc_left l JOIN [shuffle]
                      (SELECT * FROM gj_join_poc_right WHERE k=100) r ON l.k=r.k
                      GROUP BY l.k ORDER BY l.k"""
    def generalAgg = """SELECT l.k, COUNT(*), COUNT(l.v), AVG(l.v), MIN(r.v), MAX(r.v),
                        SUM(COALESCE(r.v, 0) + 2), STDDEV_POP(l.v), COVAR_POP(l.v, l.v + 1)
                        FROM gj_join_poc_left l JOIN [shuffle] gj_join_poc_right r
                        ON l.k=r.k GROUP BY l.k ORDER BY l.k"""
    sql "SET experimental_enable_group_join_fusion=false"
    order_qt_general_reference generalAgg
    sql "SET experimental_enable_group_join_fusion=true"
    explain {
        sql generalAgg
        contains "implementation: JOIN_BASED"
    }
    order_qt_general_join generalAgg
    def bothSides = """SELECT l.k, SUM(l.v * r.v), COUNT(IF(l.v > r.v, 1, NULL)),
                       SUM(l.v), SUM(r.v) FROM gj_join_poc_left l
                       JOIN [shuffle] gj_join_poc_right r ON l.k=r.k
                       GROUP BY l.k ORDER BY l.k"""
    sql "SET experimental_enable_group_join_fusion=false"
    order_qt_both_sides_reference bothSides
    sql "SET experimental_enable_group_join_fusion=true"
    explain {
        sql bothSides
        contains "implementation: JOIN_BASED"
    }
    order_qt_both_sides_join bothSides
    def errorCheck = """SELECT l.k,COUNT(assert_true(r.v IS NULL OR r.v < 99,
                        'unmatched build row must not be evaluated'))
                        FROM gj_join_poc_left l JOIN [shuffle] gj_join_poc_right r
                        ON l.k=r.k GROUP BY l.k ORDER BY l.k"""
    sql "SET experimental_enable_group_join_fusion=false"
    order_qt_error_check_reference errorCheck
    sql "SET experimental_enable_group_join_fusion=true"
    explain {
        sql errorCheck
        notContains "VGROUP JOIN"
    }
    order_qt_error_check_fallback errorCheck

    sql "DROP TABLE IF EXISTS gj_join_poc_fanout_left"
    sql "DROP TABLE IF EXISTS gj_join_poc_fanout_right"
    sql "CREATE TABLE gj_join_poc_fanout_left LIKE gj_join_poc_left"
    sql "CREATE TABLE gj_join_poc_fanout_right LIKE gj_join_poc_left"
    sql "INSERT INTO gj_join_poc_fanout_left VALUES (0,2), (0,3), (1,NULL), (2,7)"
    sql """INSERT INTO gj_join_poc_fanout_right
           SELECT CAST(number % 2 AS BIGINT),
                  IF(number % 2 = 1 OR number % 3 = 0, NULL, CAST(number % 97 AS DOUBLE))
           FROM numbers('number'='16386')"""
    sql "SET disable_join_reorder=true"
    sql "SET broadcast_row_count_limit=0"
    def fanout = """SELECT l.k, SUM(l.v), SUM(r.v)
                    FROM gj_join_poc_fanout_left l JOIN [shuffle] gj_join_poc_fanout_right r
                    ON l.k=r.k GROUP BY l.k ORDER BY l.k"""
    sql "SET experimental_enable_group_join_fusion=false"
    order_qt_fanout_reference fanout
    sql "SET experimental_enable_group_join_fusion=true"
    explain {
        sql fanout
        contains "implementation: JOIN_BASED"
    }
    order_qt_fanout_join fanout

    sql "DROP TABLE IF EXISTS gj_join_poc_nullable_left"
    sql "DROP TABLE IF EXISTS gj_join_poc_nullable_right"
    sql """CREATE TABLE gj_join_poc_nullable_left (k BIGINT NULL, v DOUBLE NULL)
           DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
           PROPERTIES("replication_num"="1")"""
    sql "CREATE TABLE gj_join_poc_nullable_right LIKE gj_join_poc_nullable_left"
    sql "INSERT INTO gj_join_poc_nullable_left VALUES (NULL,1000), (0,2), (0,NULL), (-7,4)"
    sql "INSERT INTO gj_join_poc_nullable_right VALUES (NULL,2000), (0,3), (0,5), (-7,NULL)"
    def nullableKey = """SELECT l.k, SUM(l.v), SUM(r.v)
                         FROM gj_join_poc_nullable_left l
                         JOIN [shuffle] gj_join_poc_nullable_right r ON l.k=r.k
                         GROUP BY l.k ORDER BY l.k"""
    sql "SET experimental_enable_group_join_fusion=false"
    order_qt_nullable_key_reference nullableKey
    sql "SET experimental_enable_group_join_fusion=true"
    explain {
        sql nullableKey
        contains "implementation: JOIN_BASED"
    }
    order_qt_nullable_key_join nullableKey
    order_qt_null_key_build """SELECT l.k, SUM(l.v), SUM(r.v)
                              FROM gj_join_poc_nullable_left l JOIN [shuffle]
                              (SELECT * FROM gj_join_poc_nullable_right WHERE k IS NULL) r
                              ON l.k=r.k GROUP BY l.k ORDER BY l.k"""
    sql "DROP TABLE IF EXISTS gj_join_keys_left"
    sql "DROP TABLE IF EXISTS gj_join_keys_right"
    sql """CREATE TABLE gj_join_keys_left (k VARCHAR(64) NULL, n INT NULL, v DECIMAL(18,2) NULL)
           DUPLICATE KEY(k,n) DISTRIBUTED BY HASH(k,n) BUCKETS 3
           PROPERTIES("replication_num"="1")"""
    sql "CREATE TABLE gj_join_keys_right LIKE gj_join_keys_left"
    sql """INSERT INTO gj_join_keys_left VALUES
           ('a',1,2.5),('a',1,NULL),('a',2,3.75),('',0,4.5),
           ('a\\0b',3,6.25),(NULL,1,99),('a',NULL,100)"""
    sql """INSERT INTO gj_join_keys_right VALUES
           ('a',1,5.5),('a',1,6.25),('a',2,NULL),('',0,7.5),
           ('a\\0b',3,8.25),(NULL,1,99),('a',NULL,100)"""
    def composite = """SELECT l.n,l.k,COUNT(*),AVG(l.v),SUM(r.v),SUM(l.v * r.v)
                       FROM gj_join_keys_left l JOIN [shuffle] gj_join_keys_right r
                       ON l.k=r.k AND l.n=r.n GROUP BY l.n,l.k ORDER BY l.n,l.k"""
    sql "SET experimental_enable_group_join_fusion=false"
    order_qt_composite_reference composite
    sql "SET experimental_enable_group_join_fusion=true"
    explain {
        sql composite
        contains "implementation: JOIN_BASED"
    }
    order_qt_composite_join composite
    def stringKey = """SELECT l.k,COUNT(*),AVG(l.v),SUM(r.v)
                       FROM gj_join_keys_left l JOIN [shuffle] gj_join_keys_right r
                       ON l.k=r.k GROUP BY l.k ORDER BY l.k"""
    sql "SET experimental_enable_group_join_fusion=false"
    order_qt_string_reference stringKey
    sql "SET experimental_enable_group_join_fusion=true"
    explain {
        sql stringKey
        contains "implementation: JOIN_BASED"
    }
    order_qt_string_join stringKey
    sql "SET runtime_filter_mode='GLOBAL'"
    sql "SET runtime_filter_type=2"
    order_qt_composite_rf composite
    sql "SET parallel_pipeline_task_num=4"
    order_qt_composite_parallel composite
    sql "SET runtime_filter_mode='OFF'"
    def residual = """SELECT l.k,COUNT(*) FROM gj_join_poc_left l
                      JOIN [shuffle] gj_join_poc_right r ON l.k=r.k AND l.v>r.v
                      GROUP BY l.k ORDER BY l.k"""
    explain {
        sql residual
        notContains "VGROUP JOIN"
    }
    order_qt_residual_fallback residual
    explain {
        sql """SELECT l.k,SUM(RANDOM()) FROM gj_join_poc_left l
               JOIN [shuffle] gj_join_poc_right r ON l.k=r.k GROUP BY l.k"""
        notContains "VGROUP JOIN"
    }
    sql "SET enable_spill=true"
    explain {
        sql query
        notContains "VGROUP JOIN"
    }
    order_qt_spill_fallback query
    sql "SET enable_spill=false"
    sql "SET agg_phase=2"
    explain {
        sql query
        notContains "VGROUP JOIN"
    }
    order_qt_two_phase_fallback query
    sql "SET agg_phase=1"
    sql "DROP TABLE IF EXISTS gj_join_float_left"
    sql "DROP TABLE IF EXISTS gj_join_float_right"
    sql """CREATE TABLE gj_join_float_left (id INT NOT NULL, k DOUBLE NULL, v INT NULL)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES("replication_num"="1")"""
    sql "CREATE TABLE gj_join_float_right LIKE gj_join_float_left"
    sql "INSERT INTO gj_join_float_left VALUES (1,-0.0,2),(2,0.0,3),(3,1.5,4),(4,NULL,99)"
    sql "INSERT INTO gj_join_float_right VALUES (1,0.0,5),(2,-0.0,6),(3,1.5,NULL),(4,NULL,99)"
    def floatingKey = """SELECT l.k,COUNT(*),SUM(l.v),SUM(r.v)
                         FROM gj_join_float_left l JOIN [shuffle] gj_join_float_right r
                         ON l.k=r.k GROUP BY l.k ORDER BY l.k"""
    sql "SET experimental_enable_group_join_fusion=false"
    order_qt_float_reference floatingKey
    sql "SET experimental_enable_group_join_fusion=true"
    explain {
        sql floatingKey
        // GroupJoin normalization happens after shuffle; signed zero must retain ordinary Agg.
        notContains "VGROUP JOIN"
    }
    order_qt_float_join floatingKey
    sql "SET runtime_filter_mode='GLOBAL'"
    order_qt_float_rf floatingKey
    sql "SET runtime_filter_mode='OFF'"
    sql "DROP TABLE IF EXISTS gj_join_int_left"
    sql "DROP TABLE IF EXISTS gj_join_int_right"
    sql """CREATE TABLE gj_join_int_left (k INT NULL, n SMALLINT NULL, v INT NULL)
           DUPLICATE KEY(k,n) DISTRIBUTED BY HASH(k,n) BUCKETS 3
           PROPERTIES("replication_num"="1")"""
    sql "CREATE TABLE gj_join_int_right LIKE gj_join_int_left"
    sql """INSERT INTO gj_join_int_left VALUES
           (-1,-2,3),(-1,-2,4),(0,0,NULL),(0,1,5),(1,2,6),
           (-2147483648,-32768,7),(2147483647,32767,8),(NULL,1,99),(1,NULL,99)"""
    sql """INSERT INTO gj_join_int_right VALUES
           (-1,-2,9),(-1,-2,NULL),(0,0,10),(0,1,11),(1,2,12),
           (-2147483648,-32768,13),(2147483647,32767,14),(NULL,1,99),(1,NULL,99)"""
    def intKey = """SELECT l.k,COUNT(*),SUM(l.v),SUM(r.v)
                    FROM gj_join_int_left l JOIN [shuffle] gj_join_int_right r
                    ON l.k=r.k GROUP BY l.k ORDER BY l.k"""
    sql "SET experimental_enable_group_join_fusion=false"
    order_qt_int_reference intKey
    sql "SET experimental_enable_group_join_fusion=true"
    explain {
        sql intKey
        contains "implementation: JOIN_BASED"
    }
    order_qt_int_join intKey
    // Six payload bytes exercise zero-filled padding in the eight-byte packed key.
    def packedKey = """SELECT l.n,l.k,COUNT(*),AVG(l.v),SUM(r.v),SUM(l.v*r.v)
                       FROM gj_join_int_left l JOIN [shuffle] gj_join_int_right r
                       ON l.k=r.k AND l.n=r.n GROUP BY l.n,l.k ORDER BY l.n,l.k"""
    sql "SET experimental_enable_group_join_fusion=false"
    order_qt_packed_reference packedKey
    sql "SET experimental_enable_group_join_fusion=true"
    explain {
        sql packedKey
        contains "implementation: JOIN_BASED"
    }
    order_qt_packed_join packedKey
    sql "SET runtime_filter_mode='GLOBAL'"
    order_qt_packed_rf packedKey
    sql "SET runtime_filter_mode='OFF'"
    for (def keyType : ["TINYINT", "SMALLINT"]) {
        def narrowKey = """SELECT l.g,COUNT(*),SUM(l.v),SUM(r.v)
                           FROM (SELECT CAST(k AS ${keyType}) AS g,v FROM gj_join_int_left
                                 WHERE k BETWEEN -1 AND 1 OR k IS NULL) l
                           JOIN [shuffle]
                           (SELECT CAST(k AS ${keyType}) AS g,v FROM gj_join_int_right
                            WHERE k BETWEEN -1 AND 1 OR k IS NULL) r
                           ON l.g=r.g GROUP BY l.g ORDER BY l.g"""
        sql "SET experimental_enable_group_join_fusion=false"
        order_qt_narrow_reference narrowKey
        sql "SET experimental_enable_group_join_fusion=true"
        explain {
            sql narrowKey
            contains "implementation: JOIN_BASED"
        }
        order_qt_narrow_join narrowKey
    }
    sql "DROP TABLE IF EXISTS gj_join_empty_build"
    sql "CREATE TABLE gj_join_empty_build LIKE gj_join_int_right"
    sql "SET runtime_filter_mode='GLOBAL'"
    order_qt_empty_build """SELECT l.k,COUNT(*),SUM(l.v)
                           FROM gj_join_int_left l JOIN [shuffle] gj_join_empty_build r
                           ON l.k=r.k GROUP BY l.k ORDER BY l.k"""
}
