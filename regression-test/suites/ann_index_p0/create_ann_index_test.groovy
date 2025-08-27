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

suite("create_ann_index_test") {
    sql "set enable_common_expr_pushdown=true;"
    // Test that CREATE INDEX for ANN is not supported
    sql "drop table if exists tbl_not_null"
    sql """
    CREATE TABLE `tbl_not_null` (
      `id` int NOT NULL COMMENT "",
      `embedding` array<float> NOT NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`) COMMENT "OLAP"
    DISTRIBUTED BY HASH(`id`) BUCKETS 2
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    test {
        sql """
            CREATE INDEX idx_test_ann ON tbl_not_null(`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="1"
            );
        """
        exception "ANN index can only be created during table creation, not through CREATE INDEX"
    }

    // Test cases for creating tables with ANN indexes

    // 1. Case for nullable column
    sql "drop table if exists tbl_nullable_ann"
    test {
        sql """
            CREATE TABLE tbl_nullable_ann (
                id INT NOT NULL COMMENT "",
                embedding ARRAY<FLOAT> NULL COMMENT "",
                INDEX idx_nullable_ann (`embedding`) USING ANN PROPERTIES(
                    "index_type"="hnsw",
                    "metric_type"="l2_distance",
                    "dim"="1"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id) COMMENT "OLAP"
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "ANN index must be built on a column that is not nullable"
    }

    // 2. Invalid properties cases
    // dim is not a positive integer
    sql "drop table if exists tbl_ann_invalid_dim"
    test {
        sql """
            CREATE TABLE tbl_ann_invalid_dim (
                id INT NOT NULL COMMENT "",
                embedding ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX idx_test_ann (`embedding`) USING ANN PROPERTIES(
                    "index_type"="hnsw",
                    "metric_type"="l2_distance",
                    "dim"="-1"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id) COMMENT "OLAP"
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "dim of ann index must be a positive integer"
    }

    // dim is not a number
    sql "drop table if exists tbl_ann_invalid_dim_str"
    test {
        sql """
            CREATE TABLE tbl_ann_invalid_dim_str (
                id INT NOT NULL COMMENT "",
                embedding ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX idx_test_ann (`embedding`) USING ANN PROPERTIES(
                    "index_type"="hnsw",
                    "metric_type"="l2_distance",
                    "dim"="abc"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id) COMMENT "OLAP"
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "dim of ann index must be a positive integer"
    }

    // dim is missing
    sql "drop table if exists tbl_ann_missing_dim"
    test {
        sql """
            CREATE TABLE tbl_ann_missing_dim (
                id INT NOT NULL COMMENT "",
                embedding ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX idx_test_ann (`embedding`) USING ANN PROPERTIES(
                    "index_type"="hnsw",
                    "metric_type"="l2_distance"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id) COMMENT "OLAP"
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "dim of ann index must be specified"
    }

    // index_type is missing
    sql "drop table if exists tbl_ann_missing_index_type"
    test {
        sql """
            CREATE TABLE tbl_ann_missing_index_type (
                id INT NOT NULL COMMENT "",
                embedding ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX idx_test_ann (`embedding`) USING ANN PROPERTIES(
                    "metric_type"="l2_distance",
                    "dim"="1"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id) COMMENT "OLAP"
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "index_type of ann index be specified."
    }

    // metric_type is missing
    sql "drop table if exists tbl_ann_missing_metric_type"
    test {
        sql """
            CREATE TABLE tbl_ann_missing_metric_type (
                id INT NOT NULL COMMENT "",
                embedding ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX idx_test_ann (`embedding`) USING ANN PROPERTIES(
                    "index_type"="hnsw",
                    "dim"="1"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id) COMMENT "OLAP"
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "metric_type of ann index must be specified."
    }

    // index_type is incorrect
    sql "drop table if exists tbl_ann_invalid_index_type"
    test {
        sql """
            CREATE TABLE tbl_ann_invalid_index_type (
                id INT NOT NULL COMMENT "",
                embedding ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX idx_test_ann (`embedding`) USING ANN PROPERTIES(
                    "index_type"="ivf",
                    "metric_type"="l2_distance",
                    "dim"="1"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id) COMMENT "OLAP"
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "only support ann index with type hnsw"
    }

    // metric_type is incorrect
    sql "drop table if exists tbl_ann_invalid_metric_type"
    test {
        sql """
            CREATE TABLE tbl_ann_invalid_metric_type (
                id INT NOT NULL COMMENT "",
                embedding ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX idx_test_ann (`embedding`) USING ANN PROPERTIES(
                    "index_type"="hnsw",
                    "metric_type"="cosine",
                    "dim"="1"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id) COMMENT "OLAP"
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "only support ann index with metric l2_distance or inner_product"
    }

    // 不支持的属性 quantization (已移除)
    sql "drop table if exists tbl_ann_invalid_quantization"
    test {
        sql """
            CREATE TABLE tbl_ann_invalid_quantization (
                id INT NOT NULL COMMENT "",
                embedding ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX idx_test_ann (`embedding`) USING ANN PROPERTIES(
                    "index_type"="hnsw",
                    "metric_type"="l2_distance",
                    "dim"="1",
                    "quantization"="flat"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id) COMMENT "OLAP"
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "unknown ann index property"
    }

    // Unknown property
    sql "drop table if exists tbl_ann_unknown_property"
    test {
        sql """
            CREATE TABLE tbl_ann_unknown_property (
                id INT NOT NULL COMMENT "",
                embedding ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX idx_test_ann (`embedding`) USING ANN PROPERTIES(
                    "index_type"="hnsw",
                    "metric_type"="l2_distance",
                    "dim"="1",
                    "unknown"="xxx"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id) COMMENT "OLAP"
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "unknown ann index property: unknown"
    }

    // 3. Valid CREATE TABLE with ANN index (l2_distance)
    sql "drop table if exists tbl_ann_valid_l2"
    sql """
        CREATE TABLE tbl_ann_valid_l2 (
            id INT NOT NULL COMMENT "",
            embedding ARRAY<FLOAT> NOT NULL COMMENT "",
            INDEX idx_test_ann (`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="128"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id) COMMENT "OLAP"
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    // 4. Valid CREATE TABLE with ANN index (inner_product)
    sql "drop table if exists tbl_ann_valid_inner_product"
    sql """
        CREATE TABLE tbl_ann_valid_inner_product (
            id INT NOT NULL COMMENT "",
            embedding ARRAY<FLOAT> NOT NULL COMMENT "",
            INDEX idx_test_ann (`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="inner_product",
                "dim"="128"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id) COMMENT "OLAP"
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql "drop table if exists tbl_ann_unique_key"
    test {
        sql """
            CREATE TABLE tbl_ann_unique_key (
                id INT NOT NULL COMMENT "",
                embedding ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX idx_test_ann (`embedding`) USING ANN PROPERTIES(
                    "index_type"="hnsw",
                    "metric_type"="inner_product",
                    "dim"="128"
                )
            ) ENGINE=OLAP
            UNIQUE KEY(id) COMMENT "OLAP"
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "ANN index can only be used in DUP_KEYS table"
    }
}