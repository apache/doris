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

suite ("ivf_on_disk_index_test") {
    sql "set enable_common_expr_pushdown=true;"

    // ========== IVF_ON_DISK with L2 distance ==========
    sql "drop table if exists tbl_ivf_on_disk_l2"
    sql """
    CREATE TABLE tbl_ivf_on_disk_l2 (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="ivf_on_disk",
                "metric_type"="l2_distance",
                "nlist"="3",
                "dim"="3"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    sql """
    INSERT INTO tbl_ivf_on_disk_l2 VALUES
    (1, [1.0, 2.0, 3.0]),
    (2, [0.5, 2.1, 2.9]),
    (3, [10.0, 10.0, 10.0]),
    (4, [20.0, 20.0, 20.0]),
    (5, [50.0, 20.0, 20.0]),
    (6, [60.0, 20.0, 20.0]);
    """
    qt_sql "select * from tbl_ivf_on_disk_l2 order by id;"
    qt_sql_l2_topn "select id from tbl_ivf_on_disk_l2 order by l2_distance_approximate(embedding, [1.0,2.0,3.0]) limit 2;"

    // ========== Error: missing nlist for ivf_on_disk ==========
    sql "drop table if exists tbl_ivf_on_disk_l2"
    test {
        sql """
        CREATE TABLE tbl_ivf_on_disk_l2 (
            id INT NOT NULL,
            embedding ARRAY<FLOAT> NOT NULL,
            INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                    "index_type"="ivf_on_disk",
                    "metric_type"="l2_distance",
                    "dim"="3"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
        """
        exception """nlist of ann index must be specified for ivf/ivf_on_disk type"""
    }

    // ========== Error: not enough training points ==========
    sql """
    CREATE TABLE tbl_ivf_on_disk_l2 (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="ivf_on_disk",
                "metric_type"="l2_distance",
                "nlist"="3",
                "dim"="3"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """
    test {
        sql """
        INSERT INTO tbl_ivf_on_disk_l2 VALUES
        (1, [1.0, 2.0, 3.0]),
        (2, [0.5, 2.1, 2.9]);
        """
        exception """exception occurred during training"""
    }

    // ========== IVF_ON_DISK with inner product ==========
    sql "drop table if exists tbl_ivf_on_disk_ip"
    sql """
    CREATE TABLE tbl_ivf_on_disk_ip (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="ivf_on_disk",
                "metric_type"="inner_product",
                "nlist"="3",
                "dim"="3"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    sql """
    INSERT INTO tbl_ivf_on_disk_ip VALUES
    (1, [1.0, 2.0, 3.0]),
    (2, [0.5, 2.1, 2.9]),
    (3, [10.0, 10.0, 10.0]),
    (4, [20.0, 20.0, 20.0]),
    (5, [50.0, 20.0, 20.0]),
    (6, [60.0, 20.0, 20.0]);
    """
    qt_sql "select * from tbl_ivf_on_disk_ip order by id;"
    qt_sql_ip_topn "select id from tbl_ivf_on_disk_ip order by inner_product_approximate(embedding, [1.0,2.0,3.0]) desc limit 2;"

    // ========== IVF_ON_DISK with stream load ==========
    sql "drop table if exists tbl_ivf_on_disk_stream_load"
    sql """
    CREATE TABLE tbl_ivf_on_disk_stream_load (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="ivf_on_disk",
                "metric_type"="l2_distance",
                "nlist"="3",
                "dim"="3"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    // Stream load to ivf_on_disk should succeed.
    streamLoad {
        table "tbl_ivf_on_disk_stream_load"
        file "ivf_on_disk_stream_load.json"
        set 'format', 'json'
        set 'strip_outer_array', 'true'
        time 10000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(6, json.NumberTotalRows)
            assertEquals(6, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    qt_sql_stream_load_rows "select * from tbl_ivf_on_disk_stream_load order by id;"
    qt_sql_stream_load_topn "select id from tbl_ivf_on_disk_stream_load order by l2_distance_approximate(embedding, [1.0,2.0,3.0]) limit 2;"

    // ========== IVF_ON_DISK with larger dataset (more rows than nlist) ==========
    sql "drop table if exists tbl_ivf_on_disk_large"
    sql """
    CREATE TABLE tbl_ivf_on_disk_large (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="ivf_on_disk",
                "metric_type"="l2_distance",
                "nlist"="4",
                "dim"="3"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    sql """
    INSERT INTO tbl_ivf_on_disk_large VALUES
    (1, [1.0, 2.0, 3.0]),
    (2, [0.5, 2.1, 2.9]),
    (3, [10.0, 10.0, 10.0]),
    (4, [20.0, 20.0, 20.0]),
    (5, [50.0, 20.0, 20.0]),
    (6, [60.0, 20.0, 20.0]),
    (7, [5.0, 5.0, 5.0]),
    (8, [100.0, 100.0, 100.0]),
    (9, [0.0, 0.0, 0.0]),
    (10, [30.0, 30.0, 30.0]);
    """
    qt_sql "select * from tbl_ivf_on_disk_large order by id;"
    qt_sql_large_topn "select id from tbl_ivf_on_disk_large order by l2_distance_approximate(embedding, [1.0,2.0,3.0]) limit 3;"

    // ========== IVF_ON_DISK range search with l2_distance ==========
    sql "drop table if exists tbl_ivf_on_disk_range"
    sql """
    CREATE TABLE tbl_ivf_on_disk_range (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="ivf_on_disk",
                "metric_type"="l2_distance",
                "nlist"="3",
                "dim"="3"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """

    sql """
    INSERT INTO tbl_ivf_on_disk_range VALUES
    (1, [1.0, 2.0, 3.0]),
    (2, [0.5, 2.1, 2.9]),
    (3, [10.0, 10.0, 10.0]),
    (4, [20.0, 20.0, 20.0]),
    (5, [50.0, 20.0, 20.0]),
    (6, [60.0, 20.0, 20.0]);
    """
    qt_sql_range_search "select id from tbl_ivf_on_disk_range where l2_distance_approximate(embedding, [1.0, 2.0, 3.0]) < 20.0 order by id;"
}
