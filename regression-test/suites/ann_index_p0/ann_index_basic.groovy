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

// Doris ANN Index Basic Test Suite
// Includes range search, topn search, and compound search

suite ("ann_index_basic") {
		sql "set enable_common_expr_pushdown=true;"

		// 1) Basic L2 ANN table: dim=3
		sql "drop table if exists tbl_ann_l2"
		sql """
		CREATE TABLE tbl_ann_l2 (
			id INT NOT NULL,
			embedding ARRAY<FLOAT> NOT NULL,
			INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
				"index_type"="hnsw",
				"metric_type"="l2_distance",
				"dim"="3"
			)
		) ENGINE=OLAP
		DUPLICATE KEY(id)
		DISTRIBUTED BY HASH(id) BUCKETS 2
		PROPERTIES ("replication_num" = "1");
		"""

		qt_sql_l2_insert """
		INSERT INTO tbl_ann_l2 VALUES
		(1, [1.0, 2.0, 3.0]),
		(2, [0.5, 2.1, 2.9]),
		(3, [10.0, 10.0, 10.0]);
		"""

		// Query: l2 distance ascending (closest first)
		qt_sql_l2_query "select id, l2_distance_approximate(embedding, [1.0,2.0,3.0]) as dist from tbl_ann_l2 order by dist limit 3;"

		// 2) Basic inner_product ANN table: dim=4
		sql "drop table if exists tbl_ann_ip"
		sql """
		CREATE TABLE tbl_ann_ip (
			id INT NOT NULL,
			embedding ARRAY<FLOAT> NOT NULL,
			INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
				"index_type"="hnsw",
				"metric_type"="inner_product",
				"dim"="4"
			)
		) ENGINE=OLAP
		DUPLICATE KEY(id)
		DISTRIBUTED BY HASH(id) BUCKETS 2
		PROPERTIES ("replication_num" = "1");
		"""

		qt_sql_ip_insert """
		INSERT INTO tbl_ann_ip VALUES
		(1, [0.1, 0.2, 0.3, 0.4]),
		(2, [0.5, 0.6, 0.7, 0.8]),
		(3, [1.0, 1.0, 1.0, 1.0]);
		"""

		// Query: inner product descending (higher score first)
		qt_sql_ip_query "select id from tbl_ann_ip order by inner_product_approximate(embedding, [0.1,0.2,0.3,0.4]) desc limit 3;"

		// 3) Simple threshold filter using l2_distance_approximate
		qt_sql_l2_threshold "select id from tbl_ann_l2 where l2_distance_approximate(embedding, [1.0,2.0,3.0]) < 5.0 order by id;"

        // 4) Descending l2 order (should exercise path where Desc topn for l2/cosine cannot be evaluated by ann index)
        qt_sql_l2_desc "select id from tbl_ann_l2 order by l2_distance_approximate(embedding, [1.0,2.0,3.0]) desc limit 2;"

        // 5) Ascending inner_product order (should exercise path where Asc topn for inner product cannot be evaluated by ann index)
        qt_sql_ip_asc "select id from tbl_ann_ip order by inner_product_approximate(embedding, [0.1,0.2,0.3,0.4]) asc limit 2;"

        // 6) Large table to exercise predicate-input-ratio check (create many rows and run topn with small-range predicate)
        sql "drop table if exists tbl_ann_l2_large"
        sql """
        CREATE TABLE tbl_ann_l2_large (
            id INT NOT NULL,
            embedding ARRAY<FLOAT> NOT NULL,
            INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="3"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES ("replication_num" = "1");
        """

        // insert 50 rows with simple embeddings
        sql "truncate table tbl_ann_l2_large"
        def values = []
        for (i in 1..50) {
                def a = i * 1.0
                def b = (i + 1) * 1.0
                def c = (i + 2) * 1.0
                values.add("(${i}, [${a}, ${b}, ${c}])")
        }
        sql "INSERT INTO tbl_ann_l2_large VALUES ${values.join(',')};"

        // topn with small predicate (id < 5) -> selects 4/50 = 8% (<30%), should exercise "will not use ann index" path
        qt_sql_l2_small_pred "select id from tbl_ann_l2_large where id < 5 order by l2_distance_approximate(embedding, [1.0,2.0,3.0]) limit 5;"

        // topn with large predicate (id < 40) -> selects 39/50 = 78% (>30%), more likely to use ann index
        qt_sql_l2_large_pred "select id from tbl_ann_l2_large where id < 40 order by l2_distance_approximate(embedding, [1.0,2.0,3.0]) limit 5;"

        // 7) Compound search: inverted index + ann index
        sql "drop table if exists ann_compound"
        sql """
        CREATE TABLE ann_compound (
            id INT NOT NULL,
            embedding ARRAY<FLOAT> NOT NULL,
            txt STRING NOT NULL,
            INDEX idx_txt(`txt`) USING INVERTED PROPERTIES("parser"="english"),
            INDEX idx_ann(`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="3"
            )
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
        """

        sql "INSERT INTO ann_compound VALUES (1, [1.0,2.0,3.0], 'quick brown fox'), (2, [2.0,3.0,4.0], 'lazy dog fox'), (3, [10.0,10.0,10.0], 'unrelated text');"

        qt_sql_compound "select id from ann_compound where txt match_any 'fox' order by l2_distance_approximate(embedding, [1.0,2.0,3.0]) limit 3;"
}
