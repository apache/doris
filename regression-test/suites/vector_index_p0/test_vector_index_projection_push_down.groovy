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
// specific language governing permissions and LIMITations
// under the License.

suite("test_vector_index_projection_push_down"){
    // prepare test table

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def indexTblName = "vector_index_push_down_test"
    def funcName = "approx_l2_distance"

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    // create 1 replica table
    sql """
	CREATE TABLE IF NOT EXISTS ${indexTblName}(
        `id` varchar(100) NOT NULL COMMENT '',
        `pk` bigint(20) NULL COMMENT '',
        `question_embedding` array<float> NOT NULL,
        `text` varchar(100) NULL COMMENT '',
        INDEX inverted_idx(`text`) USING INVERTED PROPERTIES("parser" = "chinese", "parser_mode" = "coarse_grained", "support_phrase" = "true"),
        INDEX vector_idx(`question_embedding`) USING VECTOR PROPERTIES(
            "dim" = "4",
            "index_type" = "hnsw",
                "metric_type" = "l2_distance",
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
    INSERT INTO ${indexTblName} VALUES
      ('1', 1, [0.01,0.02,0.03,0.04], '北京在中国'),
      ('2', 2, [0.02,0.03,0.04,0.05], '上海在中国'),
      ('3', 3, [0.03,0.04,0.05,0.06], '南京在中国'),
      ('4', 4, [0.04,0.05,0.06,0.07], '天津在中国'),
      ('5', 5, [0.05,0.06,0.07,0.08], '天津在中国'),
      ('6', 6, [0.06,0.07,0.08,0.09], '漠河在中国'),
      ('7', 7, [0.07,0.08,0.09,0.10], '漠河在中国'),
      ('8', 8, [0.08,0.09,0.10,0.11], '漠河在中国'),
      ('9', 9, [0.09,0.10,0.11,0.12], '漠河在中国'),
      ('10', 10, [0.10,0.11,0.12,0.13], '漠河在中国'),
      ('11', 11, [0.11,0.12,0.13,0.14], '漠河在中国'),
      ('12', 12, [0.12,0.13,0.14,0.15], '漠河在中国'),
      ('13', 13, [0.13,0.14,0.15,0.16], '漠河在中国'),
      ('14', 14, [0.14,0.15,0.16,0.17], '漠河在中国'),
      ('15', 15, [0.15,0.16,0.17,0.18], '漠河在中国'),
      ('16', 16, [0.16,0.17,0.18,0.19], '漠河在中国'),
      ('17', 17, [0.17,0.18,0.19,0.20], '漠河在中国'),
      ('18', 18, [0.18,0.19,0.20,0.21], '漠河在中国'),
      ('19', 19, [0.19,0.20,0.21,0.22], '漠河在中国'),
      ('20', 20, [0.20,0.21,0.22,0.23], '漠河在中国'),
      ('21', 21, [0.21,0.22,0.23,0.24], '漠河在中国'),
      ('22', 22, [0.22,0.23,0.24,0.25], '漠河在中国'),
      ('23', 23, [0.23,0.24,0.25,0.26], '漠河在中国'),
      ('24', 24, [0.24,0.25,0.26,0.27], '漠河在中国');
    """

    qt_sql "SELECT * FROM $indexTblName WHERE pk < 100 ORDER BY $funcName(question_embedding, [0.01,0.02,0.03,0.04]) ASC LIMIT 5;"
    qt_sql "SELECT *, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) as distance FROM $indexTblName WHERE pk < 100 ORDER BY distance ASC LIMIT 5;"
    qt_sql "SELECT *, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) FROM $indexTblName WHERE $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.05;"
    qt_sql "SELECT * FROM $indexTblName WHERE $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.2;"

    qt_sql "SELECT * FROM $indexTblName WHERE text MATCH_ANY '漠河'  ORDER BY round(bm25(), 4), $funcName(question_embedding, [0.01,0.02,0.03,0.04]) ASC LIMIT 5;"
    qt_sql "SELECT *, round(bm25(), 4) as score, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) as distance FROM $indexTblName WHERE text MATCH_ANY '漠河 中国' and $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.05 ORDER BY score ASC LIMIT 5;"
    qt_sql "SELECT *, round(bm25(), 4) as score, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) as distance FROM $indexTblName WHERE text MATCH_ANY '漠河 中国' and $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.05 ORDER BY distance ASC LIMIT 5;"

    qt_sql "SELECT id FROM $indexTblName WHERE pk < 100 ORDER BY $funcName(question_embedding, [0.01,0.02,0.03,0.04]) ASC LIMIT 5;"
    qt_sql "SELECT id, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) as z FROM $indexTblName WHERE pk < 100 ORDER BY z ASC LIMIT 5;"
    qt_sql "SELECT id, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) FROM $indexTblName WHERE $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.05;"
    qt_sql "SELECT id FROM $indexTblName WHERE $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.2;"

    qt_sql "SELECT id FROM $indexTblName WHERE text MATCH_ANY '漠河'  ORDER BY round(bm25(), 4), $funcName(question_embedding, [0.01,0.02,0.03,0.04]) ASC LIMIT 5;"
    qt_sql "SELECT id, round(bm25(), 4) as score, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) as distance FROM $indexTblName WHERE text MATCH_ANY '漠河 中国' and $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.05 ORDER BY score ASC LIMIT 5;"
    qt_sql "SELECT id, round(bm25(), 4) as score, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) as distance FROM $indexTblName WHERE text MATCH_ANY '漠河 中国' and $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.05 ORDER BY distance ASC LIMIT 5;"

    sql "DELETE FROM $indexTblName where pk < 10;"

    qt_sql "SELECT *, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) FROM $indexTblName WHERE $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.05;"
    qt_sql "SELECT * FROM $indexTblName WHERE $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.2;"

    qt_sql "SELECT *, round(bm25(), 4) as score, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) as distance FROM $indexTblName WHERE text MATCH_ANY '漠河 中国' and $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.05 ORDER BY score ASC LIMIT 5;"
    qt_sql "SELECT *, round(bm25(), 4) as score, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) as distance FROM $indexTblName WHERE text MATCH_ANY '漠河 中国' and $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.05 ORDER BY distance ASC LIMIT 5;"

    qt_sql "SELECT id, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) FROM $indexTblName WHERE $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.05;"
    qt_sql "SELECT id FROM $indexTblName WHERE $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.2;"

    qt_sql "SELECT id, round(bm25(), 4) as score, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) as distance FROM $indexTblName WHERE text MATCH_ANY '漠河 中国' and $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.05 ORDER BY score ASC LIMIT 5;"
    qt_sql "SELECT id, round(bm25(), 4) as score, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) as distance FROM $indexTblName WHERE text MATCH_ANY '漠河 中国' and $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.05 ORDER BY distance ASC LIMIT 5;"


sql """
    INSERT INTO ${indexTblName} VALUES
      ('1', 1, [0.01,0.02,0.03,0.04], '北京在中国'),
      ('2', 2, [0.02,0.03,0.04,0.05], '上海在中国'),
      ('3', 3, [0.03,0.04,0.05,0.06], '南京在中国'),
      ('4', 4, [0.04,0.05,0.06,0.07], '天津在中国'),
      ('5', 5, [0.05,0.06,0.07,0.08], '天津在中国'),
      ('6', 6, [0.06,0.07,0.08,0.09], '漠河在中国'),
      ('7', 7, [0.07,0.08,0.09,0.10], '漠河在中国'),
      ('8', 8, [0.08,0.09,0.10,0.11], '漠河在中国'),
      ('9', 9, [0.09,0.10,0.11,0.12], '漠河在中国'),
      ('10', 10, [0.10,0.11,0.12,0.13], '漠河在中国'),
      ('11', 11, [0.11,0.12,0.13,0.14], '漠河在中国'),
      ('12', 12, [0.12,0.13,0.14,0.15], '漠河在中国'),
      ('13', 13, [0.13,0.14,0.15,0.16], '漠河在中国'),
      ('14', 14, [0.14,0.15,0.16,0.17], '漠河在中国'),
      ('15', 15, [0.15,0.16,0.17,0.18], '漠河在中国'),
      ('16', 16, [0.16,0.17,0.18,0.19], '漠河在中国'),
      ('17', 17, [0.17,0.18,0.19,0.20], '漠河在中国'),
      ('18', 18, [0.18,0.19,0.20,0.21], '漠河在中国'),
      ('19', 19, [0.19,0.20,0.21,0.22], '漠河在中国'),
      ('20', 20, [0.20,0.21,0.22,0.23], '漠河在中国'),
      ('21', 21, [0.21,0.22,0.23,0.24], '漠河在中国'),
      ('22', 22, [0.22,0.23,0.24,0.25], '漠河在中国'),
      ('23', 23, [0.23,0.24,0.25,0.26], '漠河在中国'),
      ('24', 24, [0.24,0.25,0.26,0.27], '漠河在中国');
    """

    qt_sql "SELECT *, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) as distance FROM $indexTblName WHERE pk < 100 ORDER BY distance ASC LIMIT 5;"
    qt_sql "SELECT *, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) FROM $indexTblName WHERE $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.05;"

    qt_sql "SELECT * FROM $indexTblName WHERE text MATCH_ANY '漠河'  ORDER BY round(bm25(), 4), $funcName(question_embedding, [0.01,0.02,0.03,0.04]) ASC LIMIT 5;"
    qt_sql "SELECT *, round(bm25(), 4) as score, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) as distance FROM $indexTblName WHERE text MATCH_ANY '漠河 中国' and $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.05 ORDER BY distance ASC LIMIT 5;"

    qt_sql "SELECT id, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) as z FROM $indexTblName WHERE pk < 100 ORDER BY z ASC LIMIT 5;"
    qt_sql "SELECT id, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) FROM $indexTblName WHERE $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.05;"
    qt_sql "SELECT id FROM $indexTblName WHERE $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.2;"

    qt_sql "SELECT id FROM $indexTblName WHERE text MATCH_ANY '漠河'  ORDER BY round(bm25(), 4), $funcName(question_embedding, [0.01,0.02,0.03,0.04]) ASC LIMIT 5;"
    qt_sql "SELECT id, round(bm25(), 4) as score, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) as distance FROM $indexTblName WHERE text MATCH_ANY '漠河 中国' and $funcName(question_embedding, [0.01,0.02,0.03,0.04]) < 0.05 ORDER BY distance ASC LIMIT 5;"

    qt_sql "SELECT /*+SET_VAR(enable_scan_vector_column=false)*/ id, $funcName(question_embedding, [0.01,0.02,0.03,0.04]) as distance FROM $indexTblName ORDER BY distance ASC LIMIT 30;"
    qt_sql "SELECT /*+SET_VAR(enable_scan_vector_column=false)*/ id FROM $indexTblName ORDER BY $funcName(question_embedding, [0.01,0.02,0.03,0.04]) ASC LIMIT 30;"
}
