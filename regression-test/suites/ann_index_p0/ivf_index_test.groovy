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

suite ("ivf_index_test") {
    sql "set enable_common_expr_pushdown=true;"

    // IVF index
    sql "drop table if exists ivf_tbl_ann_l2"
    sql """
    CREATE TABLE ivf_tbl_ann_l2 (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="ivf",
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
    INSERT INTO ivf_tbl_ann_l2 VALUES
    (1, [1.0, 2.0, 3.0]),
    (2, [0.5, 2.1, 2.9]),
    (3, [10.0, 10.0, 10.0]),
    (4, [20.0, 20.0, 20.0]),
    (5, [50.0, 20.0, 20.0]),
    (6, [60.0, 20.0, 20.0]);
    """
    qt_sql "select * from ivf_tbl_ann_l2 order by id;"
    qt_sql_l2_topn "select id from ivf_tbl_ann_l2 order by l2_distance_approximate(embedding, [1.0,2.0,3.0]) limit 2;"

    sql """drop table if exists ivf_tbl_ann_l2"""
    test {
        // missing nlist
        sql """
        CREATE TABLE ivf_tbl_ann_l2 (
            id INT NOT NULL,
            embedding ARRAY<FLOAT> NOT NULL,
            INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                    "index_type"="ivf",
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

    sql """
    CREATE TABLE ivf_tbl_ann_l2 (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="ivf",
                "metric_type"="l2_distance",
                "nlist"="3",
                "dim"="3"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ("replication_num" = "1");
    """
    // Not enough training points: should not throw exception anymore, just skip index building.
    sql """
    INSERT INTO ivf_tbl_ann_l2 VALUES
    (1, [1.0, 2.0, 3.0]),
    (2, [0.5, 2.1, 2.9]);
    """
    qt_sql_l2_insufficient_train_rows "select id from ivf_tbl_ann_l2 order by l2_distance_approximate(embedding, [1.0,2.0,3.0]) limit 2;"

    sql "drop table if exists ivf_tbl_ann_ip"
    sql """
    CREATE TABLE ivf_tbl_ann_ip (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="ivf",
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
    INSERT INTO ivf_tbl_ann_ip VALUES
    (1, [1.0, 2.0, 3.0]),
    (2, [0.5, 2.1, 2.9]),
    (3, [10.0, 10.0, 10.0]),
    (4, [20.0, 20.0, 20.0]),
    (5, [50.0, 20.0, 20.0]),
    (6, [60.0, 20.0, 20.0]);
    """
    qt_sql "select * from ivf_tbl_ann_ip order by id;"
    qt_sql_ip_topn "select id from ivf_tbl_ann_ip order by inner_product_approximate(embedding, [1.0,2.0,3.0]) desc limit 2;"
}
