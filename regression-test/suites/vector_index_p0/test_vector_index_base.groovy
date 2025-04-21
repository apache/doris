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


suite("test_vector_index_base"){
    // prepare test table

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def test_vector_approx_distance_function = { indexTblName, funcName, metricType, order, opcode, range, k ->

        sql "DROP TABLE IF EXISTS ${indexTblName}"
        // create 1 replica table
        sql """
    	CREATE TABLE IF NOT EXISTS ${indexTblName}(
            id BIGINT,
            comment String,
            question_embedding array<float> NOT NULL,
            INDEX idx_comment (`comment`) USING INVERTED PROPERTIES("parser" = "english"),
            INDEX my_index (`question_embedding`) USING VECTOR PROPERTIES(
                "dim" = "4",
                "index_type" = "hnsw",
                "metric_type" = "$metricType",
                "efSearch" = "40",
                "M" = "32",
                "efConstruction" = "40",
                "is_vector_normed" = "false"
          )
    	) ENGINE=OLAP
    	DUPLICATE KEY(`id`)
    	DISTRIBUTED BY HASH(`id`) BUCKETS 1
    	PROPERTIES(
     		"replication_allocation" = "tag.location.default: 1"
    	);
        """

        def var_result = sql "show variables"
        logger.info("show variales result: " + var_result )

        sql """
            INSERT INTO $indexTblName VALUES
                (1,'oltp-next', [11,12,13,14]),
                (2, 'next', [1,2,3,4]),
                (3, 'next', [21,22,23,24]),
                (4, 'oltp', [5,6,7,8]),
                (5, 'next', [9,10,11,12]),
                (6, 'next', [211,22,23,24]),
                (7, 'oltp', [15,1,2,3]),
                (8,'oltp-next', [12,12,13,14]),
                (9, 'next', [1,21,3,4]),
                (10, 'next', [21,2,23,24]),
                (11, 'oltp', [5,6,7,17]),
                (12, 'next', [19,10,11,12]),
                (13, 'next', [10,0,11,1]);
            """

        // order by
        qt_sql "select id from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select id, $funcName([1,2,3,4], question_embedding) from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select * from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select *, $funcName([1,2,3,4], question_embedding) from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select $funcName([1,2,3,4], question_embedding) from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"

        qt_sql "select id from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select id, $funcName([1,2,3,4], question_embedding) from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select * from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select *, $funcName([1,2,3,4], question_embedding) from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select $funcName([1,2,3,4], question_embedding) from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"

        // order by z
        qt_sql "select id from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select id, $funcName([1,2,3,4], question_embedding) as z from $indexTblName order by z $order limit $k;"
        qt_sql "select * from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select *, $funcName([1,2,3,4], question_embedding) as z from $indexTblName order by z $order limit $k;"
        qt_sql "select $funcName([1,2,3,4], question_embedding) as z from $indexTblName order by z $order limit $k;"

        qt_sql "select id from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select id, $funcName([1,2,3,4], question_embedding) as z from $indexTblName where comment MATCH_ANY 'OLTP' order by z $order limit $k;"
        qt_sql "select * from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select *, $funcName([1,2,3,4], question_embedding) as z from $indexTblName where comment MATCH_ANY 'OLTP' order by z $order limit $k;"
        qt_sql "select $funcName([1,2,3,4], question_embedding) as z from $indexTblName where comment MATCH_ANY 'OLTP' order by z $order limit $k;"

        // ROUND() as z order by z
        qt_sql "select id from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select id, ROUND($funcName([1,2,3,4], question_embedding), 1) as z from $indexTblName order by z $order limit $k;"
        qt_sql "select * from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select *, ROUND($funcName([1,2,3,4], question_embedding), 1) as z from $indexTblName order by z $order limit $k;"
        qt_sql "select ROUND($funcName([1,2,3,4], question_embedding), 1) as z from $indexTblName order by z $order limit $k;"

        qt_sql "select id from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select id, $funcName([1,2,3,4], question_embedding) as z from $indexTblName where comment MATCH_ANY 'OLTP' order by z $order limit $k;"
        qt_sql "select * from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select *, $funcName([1,2,3,4], question_embedding) as z from $indexTblName where comment MATCH_ANY 'OLTP' order by z $order limit $k;"
        qt_sql "select $funcName([1,2,3,4], question_embedding) as z from $indexTblName where comment MATCH_ANY 'OLTP' order by z $order limit $k;"

        // where
        qt_sql "select id from $indexTblName where $funcName([1,2,3,4], question_embedding) $opcode $range;"
        qt_sql "select id, $funcName([1,2,3,4], question_embedding) from $indexTblName where $funcName([1,2,3,4], question_embedding) $opcode $range;"
        qt_sql "select * from $indexTblName where $funcName([1,2,3,4], question_embedding) $opcode $range;"
        qt_sql "select *, $funcName([1,2,3,4], question_embedding) from $indexTblName where $funcName([1,2,3,4], question_embedding) $opcode $range;"
        qt_sql "select $funcName([1,2,3,4], question_embedding) from $indexTblName where $funcName([1,2,3,4], question_embedding) $opcode $range;"

        qt_sql "select id from $indexTblName where comment MATCH_ANY 'OLTP' and $funcName([1,2,3,4], question_embedding) $opcode $range;"
        qt_sql "select id, $funcName([1,2,3,4], question_embedding) from $indexTblName where comment MATCH_ANY 'OLTP' and $funcName([1,2,3,4], question_embedding) $opcode $range;"
        qt_sql "select * from $indexTblName where comment MATCH_ANY 'OLTP' and $funcName([1,2,3,4], question_embedding) $opcode $range;"
        qt_sql "select *, $funcName([1,2,3,4], question_embedding) from $indexTblName where comment MATCH_ANY 'OLTP' and $funcName([1,2,3,4], question_embedding) $opcode $range;"
        qt_sql "select $funcName([1,2,3,4], question_embedding) from $indexTblName where comment MATCH_ANY 'OLTP' and $funcName([1,2,3,4], question_embedding) $opcode $range;"


        sql "DELETE FROM $indexTblName WHERE id < 2;"

        // order by
        qt_sql "select id from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select id from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select id, $funcName([1,2,3,4], question_embedding) from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select * from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select *, $funcName([1,2,3,4], question_embedding) from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select $funcName([1,2,3,4], question_embedding) from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"

        qt_sql "select id from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select id, $funcName([1,2,3,4], question_embedding) from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select * from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select *, $funcName([1,2,3,4], question_embedding) from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select $funcName([1,2,3,4], question_embedding) from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"

        // order by z
        qt_sql "select id from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select id, $funcName([1,2,3,4], question_embedding) as z from $indexTblName order by z $order limit $k;"
        qt_sql "select * from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select *, $funcName([1,2,3,4], question_embedding) as z from $indexTblName order by z $order limit $k;"
        qt_sql "select $funcName([1,2,3,4], question_embedding) as z from $indexTblName order by z $order limit $k;"

        qt_sql "select id from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select id, $funcName([1,2,3,4], question_embedding) as z from $indexTblName where comment MATCH_ANY 'OLTP' order by z $order limit $k;"
        qt_sql "select * from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select *, $funcName([1,2,3,4], question_embedding) as z from $indexTblName where comment MATCH_ANY 'OLTP' order by z $order limit $k;"
        qt_sql "select $funcName([1,2,3,4], question_embedding) as z from $indexTblName where comment MATCH_ANY 'OLTP' order by z $order limit $k;"

        // ROUND() as z order by z
        qt_sql "select id from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select id, ROUND($funcName([1,2,3,4], question_embedding), 1) as z from $indexTblName order by z $order limit $k;"
        qt_sql "select * from $indexTblName order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select *, ROUND($funcName([1,2,3,4], question_embedding), 1) as z from $indexTblName order by z $order limit $k;"
        qt_sql "select ROUND($funcName([1,2,3,4], question_embedding), 1) as z from $indexTblName order by z $order limit $k;"

        qt_sql "select id from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select id, $funcName([1,2,3,4], question_embedding) as z from $indexTblName where comment MATCH_ANY 'OLTP' order by z $order limit $k;"
        qt_sql "select * from $indexTblName where comment MATCH_ANY 'OLTP' order by $funcName([1,2,3,4], question_embedding) $order limit $k;"
        qt_sql "select *, $funcName([1,2,3,4], question_embedding) as z from $indexTblName where comment MATCH_ANY 'OLTP' order by z $order limit $k;"
        qt_sql "select $funcName([1,2,3,4], question_embedding) as z from $indexTblName where comment MATCH_ANY 'OLTP' order by z $order limit $k;"

        // where
        qt_sql "select id from $indexTblName where $funcName([1,2,3,4], question_embedding) $opcode $range;"
        qt_sql "select id, $funcName([1,2,3,4], question_embedding) from $indexTblName where $funcName([1,2,3,4], question_embedding) $opcode $range;"
        qt_sql "select * from $indexTblName where $funcName([1,2,3,4], question_embedding) $opcode $range;"
        qt_sql "select *, $funcName([1,2,3,4], question_embedding) from $indexTblName where $funcName([1,2,3,4], question_embedding) $opcode $range;"
        qt_sql "select $funcName([1,2,3,4], question_embedding) from $indexTblName where $funcName([1,2,3,4], question_embedding) $opcode $range;"

        qt_sql "select id from $indexTblName where comment MATCH_ANY 'OLTP' and $funcName([1,2,3,4], question_embedding) $opcode $range;"
        qt_sql "select id, $funcName([1,2,3,4], question_embedding) from $indexTblName where comment MATCH_ANY 'OLTP' and $funcName([1,2,3,4], question_embedding) $opcode $range;"
        qt_sql "select * from $indexTblName where comment MATCH_ANY 'OLTP' and $funcName([1,2,3,4], question_embedding) $opcode $range;"
        qt_sql "select *, $funcName([1,2,3,4], question_embedding) from $indexTblName where comment MATCH_ANY 'OLTP' and $funcName([1,2,3,4], question_embedding) $opcode $range;"
        qt_sql "select $funcName([1,2,3,4], question_embedding) from $indexTblName where comment MATCH_ANY 'OLTP' and $funcName([1,2,3,4], question_embedding) $opcode $range;"
    }

    // not push down to vector index
    test_vector_approx_distance_function.call("test_approx_l2_distance", "approx_l2_distance", "l2_distance", "DESC", ">", 100, 6);
    test_vector_approx_distance_function.call("test_approx_cosine_distance", "approx_cosine_distance", "cosine_similarity", "DESC", ">", 1, 6);
    test_vector_approx_distance_function.call("test_approx_cosine_distance_2", "approx_cosine_distance", "cosine_similarity", "ASC", "<", 1, 6);
    test_vector_approx_distance_function.call("test_approx_inner_product", "approx_inner_product", "inner_product", "ASC", "<", 100, 6);
    test_vector_approx_distance_function.call("test_approx_cosine_similarity", "approx_cosine_similarity", "cosine_similarity", "ASC", "<", 0, 6);

    // push down to vector index
    test_vector_approx_distance_function.call("test_approx_l2_distance", "approx_l2_distance", "l2_distance", "ASC", "<", 100, 6);
    test_vector_approx_distance_function.call("test_approx_inner_product", "approx_inner_product","inner_product", "DESC", ">", 100, 6);
    test_vector_approx_distance_function.call("test_approx_cosine_similarity", "approx_cosine_similarity", "cosine_similarity", "DESC", ">", 0, 6);


}
