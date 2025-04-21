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


suite("test_bm25"){
    // prepare test table

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def indexTblName = "test_bm25"

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    // create 1 replica table
    sql """
    CREATE TABLE IF NOT EXISTS ${indexTblName} (
        `id` BIGINT NOT NULL,
        `c`  TEXT NOT NULL,
        INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser" = "chinese", "parser_mode" = "coarse_grained", "support_phrase" = "true")
    ) ENGINE=OLAP
    UNIQUE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES(
        "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql "set batch_size = 2;"

    def var_result = sql "show variables"
    logger.info("show variales result: " + var_result )

    sql """
    INSERT INTO ${indexTblName} VALUES
         (1, '我来到中国北京'),
         (2, '我爱你中国，人民万岁！'),
         (3, '中国人民可以得到更多实惠'),
         (4, '激发创造 丰富生活 永远创业'),
         (5, 'AI 就是人工智能');
    """

    qt_sql "SELECT id, c FROM $indexTblName WHERE c MATCH_ANY '人民' OR id < 4 ORDER BY round(bm25(), 4), id;"
    qt_sql "SELECT id, c, bm25() AS score FROM $indexTblName WHERE c MATCH_ANY '人民' OR id < 4 ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score FROM $indexTblName WHERE c MATCH_ANY '人民' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score FROM $indexTblName WHERE c MATCH_ANY '中国' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score FROM $indexTblName WHERE c MATCH_ANY '中国 北京' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score FROM $indexTblName WHERE c MATCH_ANY '人民 中国' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score FROM $indexTblName WHERE c MATCH_ANY '创造 实惠' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score, round(bm25() + 0.9, 4) AS score2 FROM $indexTblName WHERE c MATCH_ANY '创造 实惠' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25() * 3.11, 4) AS score1, round(bm25() + 0.9, 4) AS score2 FROM $indexTblName WHERE c MATCH_ANY '中国 中国' ORDER BY round(bm25(), 4), id;"
    qt_sql "SELECT id, c, round(bm25() + 1.0, 4) as score, round(bm25() + bm25(), 4) as score2 FROM $indexTblName WHERE c MATCH_ANY '中国 创业' AND id = 4 ORDER BY score, id;"

    qt_sql "SELECT id, c FROM $indexTblName WHERE c MATCH_ALL '人民' ORDER BY round(bm25(), 4), id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score FROM $indexTblName WHERE c MATCH_ALL '人民' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score FROM $indexTblName WHERE c MATCH_ALL '中国' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score FROM $indexTblName WHERE c MATCH_ALL '中国 北京' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score FROM $indexTblName WHERE c MATCH_ALL '人民 中国' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score FROM $indexTblName WHERE c MATCH_ALL '创造 实惠' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score, round(bm25() + 0.9, 4) AS score2 FROM $indexTblName WHERE c MATCH_ALL '创造 实惠' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25() * 3.11, 4) AS score1, round(bm25() + 0.9, 4) AS score2 FROM $indexTblName WHERE c MATCH_ALL '中国 中国' ORDER BY round(bm25(), 4), id;"
    qt_sql "SELECT id, c, round(bm25() + 1.0, 4) as score, round(bm25() + bm25(), 4) as score2 FROM $indexTblName WHERE c MATCH_ALL '生活 创业' AND id = 4 ORDER BY score, id;"

    qt_sql "SELECT id, c FROM $indexTblName WHERE c MATCH_PHRASE '人民' ORDER BY round(bm25(), 4), id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score FROM $indexTblName WHERE c MATCH_PHRASE '人民' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score FROM $indexTblName WHERE c MATCH_PHRASE '中国' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score FROM $indexTblName WHERE c MATCH_PHRASE '中国 北京 ~1+' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score FROM $indexTblName WHERE c MATCH_PHRASE '人民 中国' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score FROM $indexTblName WHERE c MATCH_PHRASE '创造 创业 ~10+' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25(), 4) AS score, round(bm25() + 0.9, 4) AS score2 FROM $indexTblName WHERE c MATCH_PHRASE '创造 实惠' ORDER BY score, id;"
    qt_sql "SELECT id, c, round(bm25() * 3.11, 4) AS score1, round(bm25() + 0.9, 4) AS score2 FROM $indexTblName WHERE c MATCH_PHRASE '中国 中国' ORDER BY round(bm25(), 4), id;"
    qt_sql "SELECT id, c, round(bm25() + 1.0, 4) as score, round(bm25() + bm25(), 4) as score2 FROM $indexTblName WHERE c MATCH_PHRASE '中国 万岁 ~10+' AND id = 2 ORDER BY score, id;"
    qt_sql "SELECT/*+SET_VAR(use_fix_load_balance_replica=true)*/ id, c, round(bm25() * 3.11, 4) AS score1, round(bm25() + 0.9, 4) AS score2 FROM $indexTblName WHERE c MATCH_PHRASE '中国 中国' ORDER BY round(bm25(), 4), id;"
    qt_sql "SELECT/*+SET_VAR(use_fix_load_balance_replica=true)*/ id, c, round(bm25() + 1.0, 4) as score, round(bm25() + bm25(), 4) as score2 FROM $indexTblName WHERE c MATCH_PHRASE '中国 万岁 ~10+' AND id = 2 ORDER BY score, id;"

    sql "DELETE FROM ${indexTblName} WHERE id IN (1, 2);"
    qt_sql "SELECT id, c, bm25() AS score FROM $indexTblName WHERE c MATCH_ANY '中国' ORDER BY id;"
    qt_sql "SELECT 1 AS constant_col FROM $indexTblName WHERE c MATCH_ANY '中国' ORDER BY id;"

    sql "DROP TABLE IF EXISTS bm25_test_mow_multi_segment;"
    sql """
    CREATE TABLE IF NOT EXISTS bm25_test_mow_multi_segment (
        `id` BIGINT NOT NULL,
        `c`  TEXT NOT NULL,
        INDEX c_idx(`c`)USING INVERTED PROPERTIES("parser" = "chinese", "parser_mode" = "coarse_grained", "support_phrase" = "true")
    )
    UNIQUE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES(
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
    );
    """

    sql """
    INSERT INTO bm25_test_mow_multi_segment VALUES
    (1, '我是 中国'),
    (2, '我是 美国');
    """

    sql """
    INSERT INTO bm25_test_mow_multi_segment VALUES
    (3, '我是 德国'),
    (4, '我是 中国 中国');
    """

    sql """
    INSERT INTO bm25_test_mow_multi_segment VALUES
    (5, '我是 美国 中国');
    """
    qt_sql "SELECT c, bm25() as score FROM bm25_test_mow_multi_segment WHERE c MATCH_ANY '中国 美国' OR id < 3 ORDER BY score;"
    qt_sql "SELECT c, bm25() as score FROM bm25_test_mow_multi_segment WHERE c MATCH_ANY '中国' OR c MATCH_ANY '美国'ORDER BY score;"
    qt_sql "SELECT c, round(bm25(), 4) as score FROM bm25_test_mow_multi_segment WHERE c MATCH_ANY '中国 美国' ORDER BY score;"
    qt_sql "SELECT c, round(bm25(), 4) as score FROM bm25_test_mow_multi_segment WHERE c MATCH_ANY '中国' OR c MATCH_ANY '美国' ORDER BY score;"
    // TODO: this sql get something wrong, the predictes will be rewritten as c MATCH_ANY "中国 美国 中国 美国" or c MATCH_ANY "中国 美国 中国 美国 中国 美国 中国 美国"
    // qt_sql "SELECT c, round(bm25(), 4) as score FROM bm25_test_mow_multi_segment WHERE (c MATCH_ANY '中国' OR c MATCH_ANY '美国') AND c MATCH_ANY '中国 美国' ORDER BY score;"
    qt_sql "SELECT c, round(bm25(), 4) as score FROM bm25_test_mow_multi_segment WHERE c MATCH_ALL '中国 美国' ORDER BY score;"
    qt_sql "SELECT c, round(bm25(), 4) as score FROM bm25_test_mow_multi_segment WHERE c MATCH_PHRASE '美国 中国' ORDER BY score;"
    qt_sql "SELECT c, round(bm25(), 4) as score FROM bm25_test_mow_multi_segment WHERE c MATCH_PHRASE '美国 中国~2+' ORDER BY score;"
    qt_sql "SELECT/*+SET_VAR(use_fix_load_balance_replica=true)*/ c, round(bm25(), 4) as score FROM bm25_test_mow_multi_segment WHERE c MATCH_PHRASE '美国 中国' ORDER BY score;"
    qt_sql "SELECT/*+SET_VAR(use_fix_load_balance_replica=true)*/ c, round(bm25(), 4) as score FROM bm25_test_mow_multi_segment WHERE c MATCH_PHRASE '美国 中国~2+' ORDER BY score;"

    sql "DELETE FROM bm25_test_mow_multi_segment WHERE id IN (3, 4);"

    sql """
    INSERT INTO bm25_test_mow_multi_segment VALUES
    (3, '我是 德国'),
    (4, '我是 中国 中国');
    """
    qt_sql "SELECT c, bm25() as score FROM bm25_test_mow_multi_segment WHERE c MATCH_ANY '中国 美国' ORDER BY score;"
    qt_sql "SELECT c, bm25() as score FROM bm25_test_mow_multi_segment WHERE c MATCH_ALL '中国 美国' ORDER BY score;"
    qt_sql "SELECT c, round(bm25(), 4) as score FROM bm25_test_mow_multi_segment WHERE c MATCH_ANY '中国 美国' ORDER BY score;"
    qt_sql "SELECT c, round(bm25(), 4) as score FROM bm25_test_mow_multi_segment WHERE c MATCH_ALL '中国 美国' ORDER BY score;"
    qt_sql "SELECT c, round(bm25(), 4) as score FROM bm25_test_mow_multi_segment WHERE c MATCH_PHRASE '美国 中国' ORDER BY score;"
    qt_sql "SELECT c, round(bm25(), 4) as score FROM bm25_test_mow_multi_segment WHERE c MATCH_PHRASE '美国 中国~2+' ORDER BY score;"
    qt_sql "SELECT/*+SET_VAR(use_fix_load_balance_replica=true)*/ c, round(bm25(), 4) as score FROM bm25_test_mow_multi_segment WHERE c MATCH_PHRASE '美国 中国' ORDER BY score;"
    qt_sql "SELECT/*+SET_VAR(use_fix_load_balance_replica=true)*/ c, round(bm25(), 4) as score FROM bm25_test_mow_multi_segment WHERE c MATCH_PHRASE '美国 中国~2+' ORDER BY score;"


    sql "DROP TABLE IF EXISTS bm25_test_dup_multi_segment;"
    sql """
    CREATE TABLE IF NOT EXISTS bm25_test_dup_multi_segment (
        `id` BIGINT NOT NULL,
        `c`  TEXT NOT NULL,
        INDEX c_idx(`c`)USING INVERTED PROPERTIES("parser" = "chinese", "parser_mode" = "coarse_grained", "support_phrase" = "true")
    )
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES(
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
    );
    """

    sql """
    INSERT INTO bm25_test_dup_multi_segment VALUES
    (1, '我是 中国'),
    (2, '我是 美国');
    """

    sql """
    INSERT INTO bm25_test_dup_multi_segment VALUES
    (3, '我是 德国'),
    (4, '我是 中国 中国');
    """

    sql "set enable_delete_for_duplicate_table_with_vector_index=true"
    sql "DELETE FROM bm25_test_dup_multi_segment WHERE id IN (3, 4);"

    sql """
    INSERT INTO bm25_test_dup_multi_segment VALUES
    (3, '我是 德国'),
    (4, '我是 中国 中国'),
    (5, '我是 美国 中国');
    """
    qt_sql "SELECT c, bm25() as score FROM bm25_test_dup_multi_segment WHERE c MATCH_ANY '中国 美国' OR id < 4 ORDER BY id LIMIT 5;"
    qt_sql "SELECT id, c, bm25() as score FROM bm25_test_dup_multi_segment WHERE c MATCH_ANY '中国 美国' OR id < 4 ORDER BY id LIMIT 5;"
    qt_sql "SELECT c, round(bm25(), 4) as score FROM bm25_test_dup_multi_segment WHERE c MATCH_ANY '中国 美国' ORDER BY id LIMIT 5;"
    qt_sql "SELECT id, c, round(bm25(), 4) as score FROM bm25_test_dup_multi_segment WHERE c MATCH_ANY '中国 美国' ORDER BY id LIMIT 5;"
    qt_sql "SELECT c, round(BM25(), 4) as score FROM bm25_test_dup_multi_segment WHERE c MATCH_ALL '中国 美国' ORDER BY id LIMIT 5;"
    qt_sql "SELECT c, round(Bm25(), 4) as score FROM bm25_test_dup_multi_segment WHERE c MATCH_PHRASE '美国 中国' ORDER BY id LIMIT 5;"
    qt_sql "SELECT c, round(bM25(), 4) as score FROM bm25_test_dup_multi_segment WHERE c MATCH_PHRASE '美国 中国~2+' ORDER BY id LIMIT 5;"
    qt_sql "SELECT/*+SET_VAR(use_fix_load_balance_replica=true)*/ c, round(Bm25(), 4) as score FROM bm25_test_dup_multi_segment WHERE c MATCH_PHRASE '美国 中国' ORDER BY id LIMIT 5;"
    qt_sql "SELECT/*+SET_VAR(use_fix_load_balance_replica=true)*/ c, round(bM25(), 4) as score FROM bm25_test_dup_multi_segment WHERE c MATCH_PHRASE '美国 中国~2+' ORDER BY id LIMIT 5;"


    sql "DROP TABLE IF EXISTS bm25_test_mor_multi_segment;"
    sql """
    CREATE TABLE IF NOT EXISTS bm25_test_mor_multi_segment (
        `id` BIGINT NOT NULL,
        `c`  VARCHAR(128) NOT NULL,
        `counter` INT SUM,
        INDEX c_idx(`c`)USING INVERTED PROPERTIES("parser" = "chinese", "parser_mode" = "coarse_grained", "support_phrase" = "true")
    )
    AGGREGATE KEY(`id`, `c`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES(
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
    );
    """

    sql """
    INSERT INTO bm25_test_mor_multi_segment VALUES
    (1, '我是 中国', 1),
    (2, '我是 美国', 1);
    """

    sql """
    INSERT INTO bm25_test_mor_multi_segment VALUES
    (3, '我是 德国', 1),
    (4, '我是 中国 中国', 1);
    """

    sql """
    DELETE FROM bm25_test_mor_multi_segment WHERE id IN (3, 4);
    """

    sql """
    INSERT INTO bm25_test_mor_multi_segment VALUES
    (3, '我是 德国', 1),
    (4, '我是 中国 中国', 1),
    (5, '我是 美国 中国', 1);
    """
    qt_sql "SELECT c, bm25() as score FROM bm25_test_mor_multi_segment WHERE c MATCH_ANY '中国 美国' OR id < 4 ORDER BY score;"
    qt_sql "SELECT id, c, bm25() as score FROM bm25_test_mor_multi_segment WHERE c MATCH_ANY '中国 美国' OR id < 4 ORDER BY score;"
    qt_sql "SELECT c, round(bm25(), 4) as score FROM bm25_test_mor_multi_segment WHERE c MATCH_ANY '中国 美国' ORDER BY score;"
    qt_sql "SELECT id, c, round(bm25(), 4) as score FROM bm25_test_mor_multi_segment WHERE c MATCH_ANY '中国 美国' ORDER BY score;"
    qt_sql "SELECT c, round(bm25(), 4) as score FROM bm25_test_mor_multi_segment WHERE c MATCH_ALL '中国 美国' ORDER BY score;"
    qt_sql "SELECT c, round(bm25(), 4) as score FROM bm25_test_mor_multi_segment WHERE c MATCH_PHRASE '美国 中国' ORDER BY score;"
    qt_sql "SELECT c, round(bm25(), 4) as score FROM bm25_test_mor_multi_segment WHERE c MATCH_PHRASE '美国 中国~2+' ORDER BY score;"
    qt_sql "SELECT/*+SET_VAR(use_fix_load_balance_replica=true)*/ c, round(bm25(), 4) as score FROM bm25_test_mor_multi_segment WHERE c MATCH_PHRASE '美国 中国' ORDER BY score;"
    qt_sql "SELECT/*+SET_VAR(use_fix_load_balance_replica=true)*/ c, round(bm25(), 4) as score FROM bm25_test_mor_multi_segment WHERE c MATCH_PHRASE '美国 中国~2+' ORDER BY score;"

}
