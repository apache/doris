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

suite("cast_string_as_array") {
    sql "unset variable all;"
    sql "set enable_common_expr_pushdown=true;"

    // L2 table: dim=3
    sql "drop table if exists ann_cast_rhs_l2"
    sql """
        CREATE TABLE ann_cast_rhs_l2 (
            id INT NOT NULL,
            embedding ARRAY<FLOAT> NOT NULL,
            INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="3"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS AUTO
        PROPERTIES ("replication_num" = "1");
    """
    sql """
        INSERT INTO ann_cast_rhs_l2 VALUES
        (1, [1.0, 2.0, 3.0]),
        (2, [0.5, 2.1, 2.9]),
        (3, [10.0, 10.0, 10.0]),
        (4, [2.0, 3.0, 4.0]);
    """

    // Success: CAST(string AS array<float>) on RHS
    qt_sql_0 "select id from ann_cast_rhs_l2 order by l2_distance_approximate(embedding, cast('[1.0,2.0,3.0]' as array<float>)) limit 3;"

    // Success: extra spaces in the string and integer literals should parse fine
    test {
        sql "select id from ann_cast_rhs_l2 order by l2_distance_approximate(embedding, cast(' [1, 2 , 3 ] ' as array<float>)) limit 3;"

        exception "Ann query vector cannot be NULL"
    }
    
    // Success: nested cast(string->string->array<float>) should also work
    qt_sql_1 "select id from ann_cast_rhs_l2 order by l2_distance_approximate(embedding, cast(cast('[1.0,2.0,3.0]' as string) as array<float>)) limit 3;"

    // Failure: empty array is not allowed for ANN query vector
    test {
        sql "select id from ann_cast_rhs_l2 order by l2_distance_approximate(embedding, cast('[]' as array<float>)) limit 1;"
        exception "Ann topn query vector cannot be empty"
    }

    // A special case.
    // Constant propagation may optimize l2_distance_approximate(embedding, NULL) to NULL before reaching the
    // runtime of ANN topn. So here we will get null directly...
    test {
        sql "select id from ann_cast_rhs_l2 order by l2_distance_approximate(embedding, cast(NULL as array<float>)) limit 1;"
        exception "Constant must be ArrayLiteral or CAST to array"
    }
    
        
    // Failure: dim mismatch (2 vs table dim=3)
    test {
        sql "select id from ann_cast_rhs_l2 order by l2_distance_approximate(embedding, cast('[1.0,2.0]' as array<float>)) limit 1;"
        exception "[INVALID_ARGUMENT]"
    }

    // Inner product table: dim=4
    sql "drop table if exists ann_cast_rhs_ip"
    sql """
        CREATE TABLE ann_cast_rhs_ip (
            id INT NOT NULL,
            embedding ARRAY<FLOAT> NOT NULL,
            INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="inner_product",
                "dim"="4"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS AUTO
        PROPERTIES ("replication_num" = "1");
    """

    sql "truncate table ann_cast_rhs_ip"
    sql """
        INSERT INTO ann_cast_rhs_ip VALUES
        (1, [0.1, 0.2, 0.3, 0.4]),
        (2, [0.5, 0.6, 0.7, 0.8]),
        (3, [1.0, 1.0, 1.0, 1.0]);
    """

    // Success: DESC for inner_product
    qt_sql_3 "select id from ann_cast_rhs_ip order by inner_product_approximate(embedding, cast('[0.1,0.2,0.3,0.4]' as array<float>)) desc limit 3;"

    // Failure: dim mismatch (3 vs table dim=4)
    test {
        sql "select id from ann_cast_rhs_ip order by inner_product_approximate(embedding, cast('[0.1,0.2,0.3]' as array<float>)) desc limit 1;"
        exception "[INVALID_ARGUMENT]"
    }

    // ----------------------
    // Range search cases (CAST string -> array<float>)
    // ----------------------

    // L2 range search with <= radius: expect ids 1 and 2 (distance to [1,2,3] is <= 1.0)
    qt_sql_rs_l2_le "select id from ann_cast_rhs_l2 where l2_distance_approximate(embedding, cast('[1,2,3]' as array<float>)) <= 1.0 order by id;"

    // L2 range search with >= radius: expect ids 3 and 4 (distance to [1,2,3] is >= 1.0)
    qt_sql_rs_l2_ge "select id from ann_cast_rhs_l2 where l2_distance_approximate(embedding, cast('[1,2,3]' as array<float>)) >= 1.0 order by id;"

    // L2 range search: dim mismatch should error
    test {
        sql "select id from ann_cast_rhs_l2 where l2_distance_approximate(embedding, cast('[1,2]' as array<float>)) <= 1.0 order by id;"
        exception "[INVALID_ARGUMENT]"
    }

    // Inner product range search with >= threshold: expect ids 2 and 3
    qt_sql_rs_ip_ge "select id from ann_cast_rhs_ip where inner_product_approximate(embedding, cast('[0.1,0.2,0.3,0.4]' as array<float>)) >= 0.6 order by id;"

    // Inner product range search with < threshold: expect id 1 only
    qt_sql_rs_ip_lt "select id from ann_cast_rhs_ip where inner_product_approximate(embedding, cast('[0.1,0.2,0.3,0.4]' as array<float>)) < 0.6 order by id;"

    // Inner product range search: dim mismatch should error
    test {
        sql "select id from ann_cast_rhs_ip where inner_product_approximate(embedding, cast('[0.1,0.2,0.3]' as array<float>)) >= 0.6 order by id;"
        exception "[INVALID_ARGUMENT]"
    }

    // ----------------------
    // Non-constant RHS behavior
    // ----------------------
    
    // Fall back to full scan if RHS is not constant
    qt_sql_fall_back "select l2_distance_approximate(embedding, embedding) from ann_cast_rhs_l2 order by l2_distance_approximate(embedding, embedding) limit 10;"
        
    // Range search with non-constant RHS should execute without index pushdown
    // L2: distance(embedding, embedding) == 0, so <= 0 selects all rows
    qt_sql_rs_l2_nonconst_le "select id from ann_cast_rhs_l2 where l2_distance_approximate(embedding, embedding) <= 0.0 order by id;"

    // IP: inner_product(embedding, embedding) is sum of squares; with threshold 1.5 expect ids 2 and 3
    qt_sql_rs_ip_nonconst_ge "select id from ann_cast_rhs_ip where inner_product_approximate(embedding, embedding) >= 1.5 order by id;"
}