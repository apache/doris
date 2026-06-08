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

suite ("ann_pq_on_disk_basic") {
    sql "set enable_common_expr_pushdown=true;"

    // ========== PQ_ON_DISK with L2 distance ==========
    sql "drop table if exists tbl_pq_on_disk_l2"
    sql """
    CREATE TABLE tbl_pq_on_disk_l2 (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="pq_on_disk",
                "metric_type"="l2_distance",
                "pq_m"="3",
                "dim"="3"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    sql """
    INSERT INTO tbl_pq_on_disk_l2 VALUES
    (1, [1.0, 2.0, 3.0]),
    (2, [0.5, 2.1, 2.9]),
    (3, [10.0, 10.0, 10.0]),
    (4, [20.0, 20.0, 20.0]),
    (5, [50.0, 20.0, 20.0]),
    (6, [60.0, 20.0, 20.0]);
    """
    order_qt_pq_l2_select "select * from tbl_pq_on_disk_l2 order by id;"
    // Approximate search with l2_distance (may fallback to brute force if not enough training data)
    qt_sql_pq_l2_query "select id from tbl_pq_on_disk_l2 order by l2_distance_approximate(embedding, [1.0,2.0,3.0]) limit 2;"

    // ========== PQ_ON_DISK with inner product ==========
    sql "drop table if exists tbl_pq_on_disk_ip"
    sql """
    CREATE TABLE tbl_pq_on_disk_ip (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="pq_on_disk",
                "metric_type"="inner_product",
                "pq_m"="3",
                "dim"="3"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    sql """
    INSERT INTO tbl_pq_on_disk_ip VALUES
    (1, [1.0, 2.0, 3.0]),
    (2, [0.5, 2.1, 2.9]),
    (3, [10.0, 10.0, 10.0]),
    (4, [20.0, 20.0, 20.0]),
    (5, [50.0, 20.0, 20.0]),
    (6, [60.0, 20.0, 20.0]);
    """
    order_qt_pq_ip_select "select * from tbl_pq_on_disk_ip order by id;"
    // Approximate search with inner_product
    qt_sql_pq_ip_query "select id from tbl_pq_on_disk_ip order by inner_product_approximate(embedding, [1.0,2.0,3.0]) desc limit 2;"

    // ========== Error: missing pq_m for pq_on_disk ==========
    sql "drop table if exists tbl_pq_on_disk_err1"
    test {
        sql """
        CREATE TABLE tbl_pq_on_disk_err1 (
            id INT NOT NULL,
            embedding ARRAY<FLOAT> NOT NULL,
            INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                    "index_type"="pq_on_disk",
                    "metric_type"="l2_distance",
                    "dim"="6"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
        """
        exception """pq_m of ann index must be specified for pq_on_disk type"""
    }

    // ========== Error: dim % pq_m != 0 ==========
    sql "drop table if exists tbl_pq_on_disk_err2"
    test {
        sql """
        CREATE TABLE tbl_pq_on_disk_err2 (
            id INT NOT NULL,
            embedding ARRAY<FLOAT> NOT NULL,
            INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                    "index_type"="pq_on_disk",
                    "metric_type"="l2_distance",
                    "pq_m"="4",
                    "dim"="6"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
        """
        exception """The dimension of the vector (dim) should be a multiple of the number of subquantizers (pq_m) for pq_on_disk type"""
    }

    sql "drop table if exists tbl_pq_on_disk_nbits4"
    sql """
    CREATE TABLE tbl_pq_on_disk_nbits4 (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="pq_on_disk",
                "metric_type"="l2_distance",
                "pq_m"="4",
                "pq_nbits"="4",
                "dim"="8"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    sql """
    INSERT INTO tbl_pq_on_disk_nbits4 VALUES
    (1, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]),
    (2, [0.5, 2.1, 2.9, 3.5, 4.5, 5.5, 6.5, 7.5]),
    (3, [10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0]),
    (4, [20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0]);
    """
    qt_sql_pq_nbits4_query "select id from tbl_pq_on_disk_nbits4 order by l2_distance_approximate(embedding, [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0]) limit 2;"

    // ========== PQ_ON_DISK with WHERE filter (key use case) ==========
    sql "drop table if exists tbl_pq_on_disk_filter"
    sql """
    CREATE TABLE tbl_pq_on_disk_filter (
        user_id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="pq_on_disk",
                "metric_type"="l2_distance",
                "pq_m"="3",
                "dim"="3"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(user_id)
    DISTRIBUTED BY HASH(user_id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    sql """
    INSERT INTO tbl_pq_on_disk_filter VALUES
    (1, [1.0, 2.0, 3.0]),
    (1, [0.5, 2.1, 2.9]),
    (1, [1.1, 2.2, 3.3]),
    (2, [10.0, 10.0, 10.0]),
    (2, [20.0, 20.0, 20.0]),
    (2, [50.0, 20.0, 20.0]),
    (3, [60.0, 20.0, 20.0]),
    (3, [100.0, 100.0, 100.0]);
    """

    // This is the key PQ_ON_DISK use case: WHERE filter + ORDER BY distance + LIMIT
    // Even though the segment may be too small for PQ training (fallback to brute force),
    // the SQL path should work correctly.
    // Note: do NOT select distance values in order_qt checks -- the float32-computed
    // distance is displayed as a double, and hand-writing the expected precision is fragile.
    // The .out file must be auto-generated.
    order_qt_pq_filter "select user_id from tbl_pq_on_disk_filter where user_id = 1 order by l2_distance_approximate(embedding, [1.0,2.0,3.0]) limit 2;"

    // ========== PQ_ON_DISK range search with l2_distance ==========
    order_qt_pq_range "select user_id from tbl_pq_on_disk_filter where l2_distance_approximate(embedding, [1.0,2.0,3.0]) < 5.0 order by user_id;"

    // ========== PQ_ON_DISK with explicit pq_nbits=8 (default, supported) ==========
    sql "drop table if exists tbl_pq_on_disk_nbits"
    sql """
    CREATE TABLE tbl_pq_on_disk_nbits (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="pq_on_disk",
                "metric_type"="l2_distance",
                "pq_m"="4",
                "pq_nbits"="8",
                "dim"="8"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    sql """
    INSERT INTO tbl_pq_on_disk_nbits VALUES
    (1, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]),
    (2, [0.5, 2.1, 2.9, 3.5, 4.5, 5.5, 6.5, 7.5]),
    (3, [10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0]);
    """
    order_qt_pq_nbits "select * from tbl_pq_on_disk_nbits order by id;"
}
