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

    sql "drop table if exists tbl_nullable"
    sql """
        CREATE TABLE tbl_nullable (
            id INT NOT NULL COMMENT "",
            embedding ARRAY<FLOAT> NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(id) COMMENT "OLAP"
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    sql "drop index if exists idx_nullable_ann ON tbl_nullable"
    // 1. Case for nullable column
    test {
        sql """
            CREATE INDEX idx_nullable_ann ON tbl_nullable(`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="1"
            );
        """
        exception "ANN index must be built on a column that is not nullable"
    }

    // 2. Invalid properties cases
    // dim is not a positive integer
    sql "drop index if exists idx_test_ann2 ON tbl_not_null"
    test {
        sql """
            CREATE INDEX idx_test_ann2 ON tbl_not_null(`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="-1"
            );
        """
        exception "dim of ann index must be a positive integer"
    }

    // dim is not a number
    sql "drop index if exists idx_test_ann3 ON tbl_not_null"
    test {
        sql """
            CREATE INDEX idx_test_ann3 ON tbl_not_null(`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="abc"
            );
        """
        exception "dim of ann index must be a positive integer"
    }

    // dim is missing
    test {
        sql """
            CREATE INDEX idx_test_ann4 ON tbl_not_null(`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance"
            );
        """
        exception "dim of ann index must be specified"
    }

    // index_type is missing
    test {
        sql """
            CREATE INDEX idx_test_ann5 ON tbl_not_null(`embedding`) USING ANN PROPERTIES(
                "metric_type"="l2_distance",
                "dim"="1"
            );
        """
        exception "index_type of ann index be specified."
    }

    // metric_type is missing
    test {
        sql """
            CREATE INDEX idx_test_ann6 ON tbl_not_null(`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "dim"="1"
            );
        """
        exception "metric_type of ann index must be specified."
    }

    // index_type is incorrect
    test {
        sql """
            CREATE INDEX idx_test_ann7 ON tbl_not_null(`embedding`) USING ANN PROPERTIES(
                "index_type"="ivf",
                "metric_type"="l2_distance",
                "dim"="1"
            );
        """
        exception "only support ann index with type hnsw"
    }

    // metric_type is incorrect
    test {
        sql """
            CREATE INDEX idx_test_ann8 ON tbl_not_null(`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="cosine",
                "dim"="1"
            );
        """
        exception "only support ann index with metric l2_distance or inner_product"
    }

    // quantization is incorrect
    test {
        sql """
            CREATE INDEX idx_test_ann9 ON tbl_not_null(`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="1",
                "quantization"="bad"
            );
        """
        exception "only support ann index with quantization flat or pq"
    }

    // Unknown property
    test {
        sql """
            CREATE INDEX idx_test_ann12 ON tbl_not_null(`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="1",
                "unknown"="xxx"
            );
        """
        exception "unknown ann index property: unknown"
    }

    // Since drop index can not interupt the execution of create index, so below cases are ignored.

    // // quantization = flat
    // test {
    //     sql """
    //         CREATE INDEX idx_test_ann10 ON tbl_not_null(`embedding`) USING ANN PROPERTIES(
    //             "index_type"="hnsw",
    //             "metric_type"="l2_distance",
    //             "dim"="1",
    //             "quantization"="flat"
    //         );
    //     """
    // }

    // sql "drop index idx_test_ann10 ON tbl_not_null"

    // // quantization = pq
    // test {
    //     sql """
    //         CREATE INDEX idx_test_ann11 ON tbl_not_null(`embedding`) USING ANN PROPERTIES(
    //             "index_type"="hnsw",
    //             "metric_type"="inner_product",
    //             "dim"="1",
    //             "quantization"="pq"
    //         );
    //     """
    // }

    // 3. Valid CREATE INDEX syntax (execute last)
    test {
        sql """
            CREATE INDEX idx_test_ann ON tbl_not_null(`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="1"
            );
        """
    }
}