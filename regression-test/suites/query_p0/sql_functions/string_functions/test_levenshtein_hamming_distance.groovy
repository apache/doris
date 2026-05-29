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

suite("test_levenshtein_hamming_distance") {
    // LEVENSHTEIN tests
    qt_levenshtein_331 "SELECT levenshtein('', ''), levenshtein('kitten', 'sitting'), levenshtein('flaw', 'lawn'), levenshtein('你好', '你们'), levenshtein('数据库', '数据');"
    testFoldConst("SELECT levenshtein('', ''), levenshtein('kitten', 'sitting'), levenshtein('flaw', 'lawn'), levenshtein('你好', '你们'), levenshtein('数据库', '数据');")
    qt_levenshtein_332 "SELECT levenshtein('abc', 'abc'), levenshtein('abc', ''), levenshtein('', 'abc'), levenshtein(NULL, 'abc'), levenshtein('abc', NULL);"
    testFoldConst("SELECT levenshtein('abc', 'abc'), levenshtein('abc', ''), levenshtein('', 'abc'), levenshtein(NULL, 'abc'), levenshtein('abc', NULL);")
    qt_levenshtein_333 "SELECT levenshtein('abcd', 'abdc'), levenshtein('你好呀', '你好'), levenshtein('a你b', 'a们b');"
    testFoldConst("SELECT levenshtein('abcd', 'abdc'), levenshtein('你好呀', '你好'), levenshtein('a你b', 'a们b');")
    qt_levenshtein_334 "SELECT levenshtein(NULL, NULL), levenshtein('', '你好'), levenshtein('你好世界', '你好世间');"
    testFoldConst("SELECT levenshtein(NULL, NULL), levenshtein('', '你好'), levenshtein('你好世界', '你好世间');")

    sql """DROP TABLE IF EXISTS string_distance_lv_test"""
    sql """
        CREATE TABLE IF NOT EXISTS string_distance_lv_test (
            id int,
            s1 VARCHAR,
            s2 VARCHAR
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num"="1")
    """
    sql """
        INSERT INTO string_distance_lv_test VALUES
        (1, 'kitten', 'sitting'),
        (2, 'abc', 'abc'),
        (3, '数据库', '数据'),
        (4, null, 'abc'),
        (5, '你好呀', '你好'),
        (6, 'abcd', 'abdc'),
        (7, '', '数据库'),
        (8, '你好', ''),
        (9, '数据', '数据库')
    """
    qt_levenshtein_tbl "SELECT id, levenshtein(s1, s2) FROM string_distance_lv_test ORDER BY id"

    sql """DROP TABLE IF EXISTS string_distance_nn_test"""
    sql """
        CREATE TABLE IF NOT EXISTS string_distance_nn_test (
            id int,
            s1 VARCHAR(20) NOT NULL,
            s2 VARCHAR(20) NOT NULL
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num"="1")
    """
    sql """
        INSERT INTO string_distance_nn_test VALUES
        (1, 'abc', 'abd'),
        (2, 'abcd', 'wxyz'),
        (3, '你好', '你们'),
        (4, '数据库', '数库据')
    """
    qt_levenshtein_nn_vector_vector "SELECT id, levenshtein(s1, s2) FROM string_distance_nn_test ORDER BY id"
    qt_levenshtein_nn_vector_scalar_ascii "SELECT id, levenshtein(s1, 'abc') FROM string_distance_nn_test WHERE id = 1 ORDER BY id"
    qt_levenshtein_nn_scalar_vector_ascii "SELECT id, levenshtein('abc', s1) FROM string_distance_nn_test WHERE id = 1 ORDER BY id"
    qt_levenshtein_nn_vector_scalar_utf8 "SELECT id, levenshtein(s1, '你们') FROM string_distance_nn_test WHERE id = 3 ORDER BY id"
    qt_levenshtein_nn_scalar_vector_utf8 "SELECT id, levenshtein('你们', s1) FROM string_distance_nn_test WHERE id = 3 ORDER BY id"
    qt_levenshtein_vector_scalar_nullable "SELECT id, levenshtein(s1, 'abc') FROM string_distance_lv_test WHERE id IN (2, 4) ORDER BY id"
    qt_levenshtein_scalar_vector_nullable "SELECT id, levenshtein('abc', s1) FROM string_distance_lv_test WHERE id IN (2, 4) ORDER BY id"
    qt_levenshtein_vector_scalar_utf8 "SELECT id, levenshtein(s1, '你好') FROM string_distance_lv_test WHERE id IN (5, 7) ORDER BY id"
    qt_levenshtein_scalar_vector_utf8 "SELECT id, levenshtein('你好', s1) FROM string_distance_lv_test WHERE id IN (5, 7) ORDER BY id"
    qt_levenshtein_vector_scalar_empty_utf8 "SELECT id, levenshtein(s1, '') FROM string_distance_lv_test WHERE id IN (5, 8) ORDER BY id"
    qt_levenshtein_scalar_vector_empty_utf8 "SELECT id, levenshtein('', s1) FROM string_distance_lv_test WHERE id IN (5, 8) ORDER BY id"

    sql """DROP TABLE IF EXISTS string_distance_lv_nn_test"""
    sql """
        CREATE TABLE IF NOT EXISTS string_distance_lv_nn_test (
            id int,
            s1 VARCHAR(20) NOT NULL,
            s2 VARCHAR(20) NOT NULL
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num"="1")
    """
    sql """
        INSERT INTO string_distance_lv_nn_test VALUES
        (1, 'abc', 'abcdef'),
        (2, 'abcdef', 'abc'),
        (3, '', 'abc'),
        (4, 'abc', ''),
        (5, '数据', '数据库'),
        (6, '数据库', '数据'),
        (7, '', '数据库'),
        (8, '数据库', '')
    """
    qt_levenshtein_lv_nn_vector_vector "SELECT id, levenshtein(s1, s2) FROM string_distance_lv_nn_test ORDER BY id"
    qt_levenshtein_lv_nn_vector_scalar_empty "SELECT id, levenshtein(s1, '') FROM string_distance_lv_nn_test ORDER BY id"
    qt_levenshtein_lv_nn_scalar_vector_empty "SELECT id, levenshtein('', s1) FROM string_distance_lv_nn_test ORDER BY id"
    qt_levenshtein_lv_nn_vector_scalar_ascii "SELECT id, levenshtein(s1, 'abcdef') FROM string_distance_lv_nn_test WHERE id IN (1, 2, 3, 4) ORDER BY id"
    qt_levenshtein_lv_nn_scalar_vector_ascii "SELECT id, levenshtein('abcdef', s1) FROM string_distance_lv_nn_test WHERE id IN (1, 2, 3, 4) ORDER BY id"
    qt_levenshtein_lv_nn_vector_scalar_utf8 "SELECT id, levenshtein(s1, '数据库') FROM string_distance_lv_nn_test WHERE id IN (5, 6, 7, 8) ORDER BY id"
    qt_levenshtein_lv_nn_scalar_vector_utf8 "SELECT id, levenshtein('数据库', s1) FROM string_distance_lv_nn_test WHERE id IN (5, 6, 7, 8) ORDER BY id"

    // HAMMING_DISTANCE tests
    qt_hamming_distance_333 "SELECT hamming_distance('', ''), hamming_distance('abc', 'abc'), hamming_distance('abc', 'abd'), hamming_distance('你好', '你们');"
    testFoldConst("SELECT hamming_distance('', ''), hamming_distance('abc', 'abc'), hamming_distance('abc', 'abd'), hamming_distance('你好', '你们');")
    qt_hamming_distance_334 "SELECT hamming_distance('abc', 'abc'), hamming_distance(NULL, 'abc'), hamming_distance('abc', NULL);"
    testFoldConst("SELECT hamming_distance('abc', 'abc'), hamming_distance(NULL, 'abc'), hamming_distance('abc', NULL);")
    qt_hamming_distance_335 "SELECT hamming_distance('abcd', 'wxyz'), hamming_distance('你好吗', '你们吗'), hamming_distance('数据库', '数库据');"
    testFoldConst("SELECT hamming_distance('abcd', 'wxyz'), hamming_distance('你好吗', '你们吗'), hamming_distance('数据库', '数库据');")
    qt_hamming_distance_336 "SELECT hamming_distance(NULL, NULL), hamming_distance(NULL, 'addd'), hamming_distance('addd', NULL);"
    testFoldConst("SELECT hamming_distance(NULL, NULL), hamming_distance(NULL, 'addd'), hamming_distance('addd', NULL);")

    sql """ set debug_skip_fold_constant = false; """
    test {
        sql "SELECT hamming_distance('abc', 'ab');"
        exception "hamming_distance requires strings of the same length"
    }
    test {
        sql "SELECT hamming_distance('你好', '你');"
        exception "hamming_distance requires strings of the same length"
    }
    sql """ set debug_skip_fold_constant = true; """
    test {
        sql "SELECT hamming_distance('abc', 'ab');"
        exception "hamming_distance requires strings of the same length"
    }
    test {
        sql "SELECT hamming_distance('你好', '你');"
        exception "hamming_distance requires strings of the same length"
    }
    sql """ set debug_skip_fold_constant = false; """

    sql """DROP TABLE IF EXISTS string_distance_hd_test"""
    sql """
        CREATE TABLE IF NOT EXISTS string_distance_hd_test (
            id int,
            s1 VARCHAR,
            s2 VARCHAR
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num"="1")
    """
    sql """
        INSERT INTO string_distance_hd_test VALUES
        (1, 'abc', 'abc'),
        (2, 'abc', 'abd'),
        (3, '你好', '你们'),
        (4, null, 'abc'),
        (5, 'abcd', 'wxyz'),
        (6, '你好吗', '你们吗'),
        (7, '数据库', '数库据')
    """
    qt_hamming_distance_tbl "SELECT id, hamming_distance(s1, s2) FROM string_distance_hd_test ORDER BY id"
    qt_hamming_distance_nn_vector_vector "SELECT id, hamming_distance(s1, s2) FROM string_distance_nn_test ORDER BY id"
    qt_hamming_distance_nn_vector_scalar_ascii "SELECT id, hamming_distance(s1, 'abc') FROM string_distance_nn_test WHERE id = 1 ORDER BY id"
    qt_hamming_distance_nn_scalar_vector_ascii "SELECT id, hamming_distance('abc', s1) FROM string_distance_nn_test WHERE id = 1 ORDER BY id"
    qt_hamming_distance_nn_vector_scalar_utf8 "SELECT id, hamming_distance(s1, '你们') FROM string_distance_nn_test WHERE id = 3 ORDER BY id"
    qt_hamming_distance_nn_scalar_vector_utf8 "SELECT id, hamming_distance('你们', s1) FROM string_distance_nn_test WHERE id = 3 ORDER BY id"
    qt_hamming_distance_vector_scalar_nullable "SELECT id, hamming_distance(s1, 'abc') FROM string_distance_hd_test WHERE id IN (1, 2, 4) ORDER BY id"
    qt_hamming_distance_scalar_vector_nullable "SELECT id, hamming_distance('abc', s1) FROM string_distance_hd_test WHERE id IN (1, 2, 4) ORDER BY id"
    qt_hamming_distance_vector_scalar_nullable_utf8 "SELECT id, hamming_distance(s1, '你们') FROM string_distance_hd_test WHERE id = 3 ORDER BY id"
    qt_hamming_distance_scalar_vector_nullable_utf8 "SELECT id, hamming_distance('你们', s1) FROM string_distance_hd_test WHERE id = 3 ORDER BY id"
    qt_hamming_distance_left_const_null_nullable "SELECT id, hamming_distance(NULL, s1) FROM string_distance_hd_test WHERE id IN (1, 4) ORDER BY id"
    qt_hamming_distance_right_const_null_nullable "SELECT id, hamming_distance(s1, NULL) FROM string_distance_hd_test WHERE id IN (1, 4) ORDER BY id"
    qt_hamming_distance_left_cast_null_nullable "SELECT id, hamming_distance(CAST(NULL AS STRING), s1) FROM string_distance_hd_test WHERE id IN (1, 4) ORDER BY id"
    qt_hamming_distance_right_cast_null_nullable "SELECT id, hamming_distance(s1, CAST(NULL AS STRING)) FROM string_distance_hd_test WHERE id IN (1, 4) ORDER BY id"
    test {
        sql "SELECT hamming_distance(s1, 'ab') FROM string_distance_hd_test WHERE id = 1"
        exception "hamming_distance requires strings of the same length"
    }
    test {
        sql "SELECT hamming_distance('ab', s1) FROM string_distance_hd_test WHERE id = 1"
        exception "hamming_distance requires strings of the same length"
    }
    test {
        sql "SELECT hamming_distance(s1, '你') FROM string_distance_hd_test WHERE id = 3"
        exception "hamming_distance requires strings of the same length"
    }
    test {
        sql "SELECT hamming_distance(s1, 'ab') FROM string_distance_nn_test WHERE id = 1"
        exception "hamming_distance requires strings of the same length"
    }
    test {
        sql "SELECT hamming_distance('ab', s1) FROM string_distance_nn_test WHERE id = 1"
        exception "hamming_distance requires strings of the same length"
    }
    test {
        sql "SELECT hamming_distance(s1, '你') FROM string_distance_nn_test WHERE id = 3"
        exception "hamming_distance requires strings of the same length"
    }

    sql """ set enable_nereids_planner=true, enable_fallback_to_original_planner=false; """
    qt_nereids_levenshtein_337 "SELECT levenshtein('kitten', 'sitting'), levenshtein(NULL, 'abc'), levenshtein('你好世界', '你好世间');"
    testFoldConst("SELECT levenshtein('kitten', 'sitting'), levenshtein(NULL, 'abc'), levenshtein('你好世界', '你好世间');")
    qt_nereids_hamming_distance_338 "SELECT hamming_distance('abcd', 'abcf'), hamming_distance(NULL, 'addd'), hamming_distance('你好', '你们');"
    testFoldConst("SELECT hamming_distance('abcd', 'abcf'), hamming_distance(NULL, 'addd'), hamming_distance('你好', '你们');")
    test {
        sql "SELECT hamming_distance('abc', 'ab');"
        exception "hamming_distance requires strings of the same length"
    }
    sql """ set enable_nereids_planner=false, enable_fallback_to_original_planner=true; """
}
