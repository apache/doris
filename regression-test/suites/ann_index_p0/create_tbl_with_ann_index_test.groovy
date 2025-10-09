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

suite("create_tbl_with_ann_index_test") {
    sql "set enable_common_expr_pushdown=true;"
    sql "drop table if exists ann_tbl1"
    test {
        sql """
            CREATE TABLE ann_tbl1 (
                id INT NOT NULL COMMENT "",
                vec ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX ann_idx1 (vec) USING ANN PROPERTIES(
                    "index_type" = "hnsw",
                    "metric_type" = "l2_distance",
                    "dim" = "128"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id) COMMENT "OLAP"
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );
        """
    }

    sql "drop table if exists ann_tbl2"
    test {
        sql """
            CREATE TABLE ann_tbl2 (
                id INT NOT NULL COMMENT "",
                vec ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX ann_idx2 (vec) USING ANN PROPERTIES(
                    "index_type" = "ivf",
                    "metric_type" = "l2_distance",
                    "dim" = "128"
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

    // metric_type 错误
    sql "drop table if exists ann_tbl3"
    test {
        sql """
            CREATE TABLE ann_tbl3 (
                id INT NOT NULL COMMENT "",
                vec ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX ann_idx3 (vec) USING ANN PROPERTIES(
                    "index_type" = "hnsw",
                    "metric_type" = "cosine",
                    "dim" = "128"
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

    // dim 非正整数
    sql "drop table if exists ann_tbl4"
    test {
        sql """
            CREATE TABLE ann_tbl4 (
                id INT NOT NULL COMMENT "",
                vec ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX ann_idx4 (vec) USING ANN PROPERTIES(
                    "index_type" = "hnsw",
                    "metric_type" = "l2_distance",
                    "dim" = "-1"
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

    // dim 非数字
    sql "drop table if exists ann_tbl5"
    test {
        sql """
            CREATE TABLE ann_tbl5 (
                id INT NOT NULL COMMENT "",
                vec ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX ann_idx5 (vec) USING ANN PROPERTIES(
                    "index_type" = "hnsw",
                    "metric_type" = "l2_distance",
                    "dim" = "abc"
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

    // 不支持的属性 quantization (已移除)
    sql "drop table if exists ann_tbl6"
    test {
        sql """
            CREATE TABLE ann_tbl6 (
                id INT NOT NULL COMMENT "",
                vec ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX ann_idx6 (vec) USING ANN PROPERTIES(
                    "index_type" = "hnsw",
                    "metric_type" = "l2_distance",
                    "dim" = "128",
                    "quantization" = "flat"
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

    // 缺少 index_type
    sql "drop table if exists ann_tbl7"
    test {
        sql """
            CREATE TABLE ann_tbl7 (
                id INT NOT NULL COMMENT "",
                vec ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX ann_idx7 (vec) USING ANN PROPERTIES(
                    "metric_type" = "l2_distance",
                    "dim" = "128"
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

    // 缺少 metric_type
    sql "drop table if exists ann_tbl8"
    test {
        sql """
            CREATE TABLE ann_tbl8 (
                id INT NOT NULL COMMENT "",
                vec ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX ann_idx8 (vec) USING ANN PROPERTIES(
                    "index_type" = "hnsw",
                    "dim" = "128"
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

    // 缺少 dim
    sql "drop table if exists ann_tbl9"
    test {
        sql """
            CREATE TABLE ann_tbl9 (
                id INT NOT NULL COMMENT "",
                vec ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX ann_idx9 (vec) USING ANN PROPERTIES(
                    "index_type" = "hnsw",
                    "metric_type" = "l2_distance"
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

    // 未知属性
    sql "drop table if exists ann_tbl10"
    test {
        sql """
            CREATE TABLE ann_tbl10 (
                id INT NOT NULL COMMENT "",
                vec ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX ann_idx10 (vec) USING ANN PROPERTIES(
                    "index_type" = "hnsw",
                    "metric_type" = "l2_distance",
                    "dim" = "128",
                    "unknown" = "xxx"
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

    // 不支持的属性 quantization (已移除)
    sql "drop table if exists ann_tbl12"
    test {
        sql """
            CREATE TABLE ann_tbl12 (
                id INT NOT NULL COMMENT "",
                vec ARRAY<FLOAT> NOT NULL COMMENT "",
                INDEX ann_idx12 (vec) USING ANN PROPERTIES(
                    "index_type" = "hnsw",
                    "metric_type" = "inner_product",
                    "dim" = "128",
                    "quantization" = "pq"
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

    // 成功创建 ANN 索引 - 基本配置
    sql "drop table if exists ann_tbl13"
    sql """
        CREATE TABLE ann_tbl13 (
            id INT NOT NULL COMMENT "",
            vec ARRAY<FLOAT> NOT NULL COMMENT "",
            INDEX ann_idx13 (vec) USING ANN PROPERTIES(
                "index_type" = "hnsw",
                "metric_type" = "l2_distance",
                "dim" = "128"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id) COMMENT "OLAP"
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    // 成功创建 ANN 索引 - inner_product
    sql "drop table if exists ann_tbl14"
    sql """
        CREATE TABLE ann_tbl14 (
            id INT NOT NULL COMMENT "",
            vec ARRAY<FLOAT> NOT NULL COMMENT "",
            INDEX ann_idx14 (vec) USING ANN PROPERTIES(
                "index_type" = "hnsw",
                "metric_type" = "inner_product",
                "dim" = "256"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id) COMMENT "OLAP"
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
    """

}