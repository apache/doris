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

suite("small_segment") {
    sql "set enable_common_expr_pushdown=true;"

    // Test that ANN index is not built when segment size is smaller than required training rows
    sql "drop table if exists tbl_small_segment"
    sql """
    CREATE TABLE tbl_small_segment (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="ivf",
                "metric_type"="l2_distance",
                "nlist"="10",
                "dim"="3"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    // Insert fewer rows than nlist (10), this should not throw an exception
    // and should skip building the ANN index
    sql """
    INSERT INTO tbl_small_segment VALUES
    (1, [1.0, 2.0, 3.0]),
    (2, [0.5, 2.1, 2.9]),
    (3, [10.0, 10.0, 10.0]);
    """

    // Verify data is inserted successfully
    qt_sql "select * from tbl_small_segment order by id;"

    // Test with HNSW index as well - HNSW should work even with small segments
    sql "drop table if exists tbl_small_segment_hnsw"
    sql """
    CREATE TABLE tbl_small_segment_hnsw (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="3"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    // Insert small amount of data - HNSW should work
    sql """
    INSERT INTO tbl_small_segment_hnsw VALUES
    (1, [1.0, 2.0, 3.0]),
    (2, [0.5, 2.1, 2.9]),
    (3, [10.0, 10.0, 10.0]);
    """

    qt_sql "select * from tbl_small_segment_hnsw order by id;"

    // Test approximate search with HNSW (should work)
    sql "select id, l2_distance_approximate(embedding, [1.0,2.0,3.0]) as dist from tbl_small_segment_hnsw order by dist limit 2;"

    // Clean up
    sql "drop table if exists tbl_small_segment"
    sql "drop table if exists tbl_small_segment_hnsw"
}