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

suite("quantizer_min_train_rows") {
    sql "set enable_common_expr_pushdown=true;"

    // Test PQ quantizer minimum training rows requirement
    // PQ with pq_m=2, pq_nbits=8 requires 256 * 2^8 * 2 = 131072 training vectors
    sql "drop table if exists tbl_pq_insufficient_data"
    sql """
    CREATE TABLE tbl_pq_insufficient_data (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="ivf",
                "metric_type"="l2_distance",
                "nlist"="10",
                "dim"="4",
                "quantizer"="pq",
                "pq_m"="2",
                "pq_nbits"="8"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    // Insert fewer rows than required (256), this should not throw an exception
    // and should skip building the ANN index
    sql """
    INSERT INTO tbl_pq_insufficient_data VALUES
    (1, [1.0, 2.0, 3.0, 4.0]),
    (2, [2.0, 3.0, 4.0, 5.0]),
    (3, [3.0, 4.0, 5.0, 6.0]);
    """

    // Verify data is inserted successfully
    qt_sql "select * from tbl_pq_insufficient_data order by l2_distance_approximate(embedding, [1.0,2.0,3.0,4.0]) limit 100;"

    // Test SQ quantizer minimum training rows requirement
    // SQ requires 20 training vectors
    sql "drop table if exists tbl_sq_insufficient_data"
    sql """
    CREATE TABLE tbl_sq_insufficient_data (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="ivf",
                "metric_type"="l2_distance",
                "nlist"="10",
                "dim"="4",
                "quantizer"="sq8"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    // Insert fewer rows than required (10), this should not throw an exception
    // and should skip building the ANN index
    sql """
    INSERT INTO tbl_sq_insufficient_data VALUES
    (1, [1.0, 2.0, 3.0, 4.0]),
    (2, [2.0, 3.0, 4.0, 5.0]),
    (3, [3.0, 4.0, 5.0, 6.0]);
    """

    // Verify data is inserted successfully
    qt_sql "select * from tbl_sq_insufficient_data order by l2_distance_approximate(embedding, [1.0,2.0,3.0,4.0]) limit 100;"

    // Test PQ with sufficient data - should build index successfully
    sql "drop table if exists tbl_pq_sufficient_data"
    sql """
    CREATE TABLE tbl_pq_sufficient_data (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="ivf",
                "metric_type"="l2_distance",
                "nlist"="10",
                "dim"="4",
                "quantizer"="pq",
                "pq_m"="2",
                "pq_nbits"="2"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    // PQ with pq_m=2, pq_nbits=2 requires pq_m * (1 << pq_nbits) * 256 = 2 * 4 * 256 = 2048 training vectors
    // Insert exactly 2048 rows to meet the requirement
    def insert_data = []
    for (int i = 1; i <= 2048; i++) {
        insert_data.add("(${i}, [${i % 10}.0, ${(i + 1) % 10}.0, ${(i + 2) % 10}.0, ${(i + 3) % 10}.0])")
    }
    sql "INSERT INTO tbl_pq_sufficient_data VALUES ${insert_data.join(', ')};"

    // Verify data is inserted successfully
    qt_sql "select count(*) from tbl_pq_sufficient_data;"

    // Test SQ with sufficient data - should build index successfully
    sql "drop table if exists tbl_sq_sufficient_data"
    sql """
    CREATE TABLE tbl_sq_sufficient_data (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="ivf",
                "metric_type"="l2_distance",
                "nlist"="5",
                "dim"="4",
                "quantizer"="sq4"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    // SQ requires 20 training vectors
    // Insert more than 20 rows to meet the requirement
    sql """
    INSERT INTO tbl_sq_sufficient_data VALUES
    (1, [1.0, 2.0, 3.0, 4.0]),
    (2, [2.0, 3.0, 4.0, 5.0]),
    (3, [3.0, 4.0, 5.0, 6.0]),
    (4, [4.0, 5.0, 6.0, 7.0]),
    (5, [5.0, 6.0, 7.0, 8.0]),
    (6, [6.0, 7.0, 8.0, 9.0]),
    (7, [7.0, 8.0, 9.0, 10.0]),
    (8, [8.0, 9.0, 10.0, 11.0]),
    (9, [9.0, 10.0, 11.0, 12.0]),
    (10, [10.0, 11.0, 12.0, 13.0]),
    (11, [11.0, 12.0, 13.0, 14.0]),
    (12, [12.0, 13.0, 14.0, 15.0]);
    """

    // Verify data is inserted successfully
    qt_sql "select count(*) from tbl_sq_sufficient_data;"

    // Test HNSW PQ quantizer minimum training rows requirement
    sql "drop table if exists tbl_hnsw_pq_insufficient_data"
    sql """
    CREATE TABLE tbl_hnsw_pq_insufficient_data (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="4",
                "quantizer"="pq",
                "pq_m"="2",
                "pq_nbits"="8",
                "max_degree"="16",
                "ef_construction"="200"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    // Insert fewer rows than required (131072), this should not throw an exception
    // and should skip building the ANN index
    sql """
    INSERT INTO tbl_hnsw_pq_insufficient_data VALUES
    (1, [1.0, 2.0, 3.0, 4.0]),
    (2, [2.0, 3.0, 4.0, 5.0]),
    (3, [3.0, 4.0, 5.0, 6.0]);
    """

    // Verify data is inserted successfully
    qt_sql "select * from tbl_hnsw_pq_insufficient_data order by id;"

    // Test HNSW SQ quantizer minimum training rows requirement
    sql "drop table if exists tbl_hnsw_sq_insufficient_data"
    sql """
    CREATE TABLE tbl_hnsw_sq_insufficient_data (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="4",
                "quantizer"="sq8",
                "max_degree"="16",
                "ef_construction"="200"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    // Insert fewer rows than required (20), this should not throw an exception
    // and should skip building the ANN index
    sql """
    INSERT INTO tbl_hnsw_sq_insufficient_data VALUES
    (1, [1.0, 2.0, 3.0, 4.0]),
    (2, [2.0, 3.0, 4.0, 5.0]),
    (3, [3.0, 4.0, 5.0, 6.0]);
    """

    // Verify data is inserted successfully
    qt_sql "select * from tbl_hnsw_sq_insufficient_data order by id;"

    // Test HNSW with sufficient data - should build index successfully
    sql "drop table if exists tbl_hnsw_pq_sufficient_data"
    sql """
    CREATE TABLE tbl_hnsw_pq_sufficient_data (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="4",
                "quantizer"="pq",
                "pq_m"="2",
                "pq_nbits"="2",
                "max_degree"="16",
                "ef_construction"="200"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    // PQ with pq_m=2, pq_nbits=2 requires 256 * 2^2 * 2 = 2048 training vectors
    // Insert exactly 2048 rows to meet the requirement
    def insert_data_hnsw_pq = []
    for (int i = 1; i <= 2048; i++) {
        insert_data_hnsw_pq.add("(${i}, [${i % 10}.0, ${(i + 1) % 10}.0, ${(i + 2) % 10}.0, ${(i + 3) % 10}.0])")
    }
    sql "INSERT INTO tbl_hnsw_pq_sufficient_data VALUES ${insert_data_hnsw_pq.join(', ')};"

    // Verify data is inserted successfully
    qt_sql "select count(*) from tbl_hnsw_pq_sufficient_data;"

    // Test HNSW SQ with sufficient data - should build index successfully
    sql "drop table if exists tbl_hnsw_sq_sufficient_data"
    sql """
    CREATE TABLE tbl_hnsw_sq_sufficient_data (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="4",
                "quantizer"="sq4",
                "max_degree"="16",
                "ef_construction"="200"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    // SQ requires 20 training vectors
    // Insert more than 20 rows to meet the requirement
    sql """
    INSERT INTO tbl_hnsw_sq_sufficient_data VALUES
    (1, [1.0, 2.0, 3.0, 4.0]),
    (2, [2.0, 3.0, 4.0, 5.0]),
    (3, [3.0, 4.0, 5.0, 6.0]),
    (4, [4.0, 5.0, 6.0, 7.0]),
    (5, [5.0, 6.0, 7.0, 8.0]),
    (6, [6.0, 7.0, 8.0, 9.0]),
    (7, [7.0, 8.0, 9.0, 10.0]),
    (8, [8.0, 9.0, 10.0, 11.0]),
    (9, [9.0, 10.0, 11.0, 12.0]),
    (10, [10.0, 11.0, 12.0, 13.0]),
    (11, [11.0, 12.0, 13.0, 14.0]),
    (12, [12.0, 13.0, 14.0, 15.0]);
    """

    // Verify data is inserted successfully
    qt_sql "select count(*) from tbl_hnsw_sq_sufficient_data;"

    // Clean up
    sql "drop table if exists tbl_pq_insufficient_data"
    sql "drop table if exists tbl_sq_insufficient_data"
    sql "drop table if exists tbl_pq_sufficient_data"
    sql "drop table if exists tbl_sq_sufficient_data"
    sql "drop table if exists tbl_hnsw_pq_insufficient_data"
    sql "drop table if exists tbl_hnsw_sq_insufficient_data"
    sql "drop table if exists tbl_hnsw_pq_sufficient_data"
    sql "drop table if exists tbl_hnsw_sq_sufficient_data"
}