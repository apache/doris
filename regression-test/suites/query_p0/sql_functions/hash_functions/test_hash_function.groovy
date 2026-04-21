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
suite("test_hash_function", "arrow_flight_sql") {
    sql "set batch_size = 4096;"
    sql "set enable_profile = true;"

    qt_sql "SELECT murmur_hash3_32(null);"
    qt_sql "SELECT murmur_hash3_32(\"hello\");"
    qt_sql "SELECT murmur_hash3_32(\"hello\", \"world\");"

    qt_sql "SELECT murmur_hash3_64(null);"
    qt_sql "SELECT murmur_hash3_64(\"hello\");"
    qt_sql "SELECT murmur_hash3_64(\"hello\", \"world\");"

    // Keep the results same with `mmh3.hash64` in python or `murmur3.Sum64` in go
    // Please dont auto genOut for this test
    qt_mmh3_64_v2_1 "SELECT MURMUR_HASH3_64_V2(NULL);"
    qt_mmh3_64_v2_2 "SELECT MURMUR_HASH3_64_V2('1000209601_1756808272');"
    qt_mmh3_64_v2_3 "SELECT MURMUR_HASH3_64_V2('hello world');"
    qt_mmh3_64_v2_4 "SELECT MURMUR_HASH3_64_V2('apache doris');"
    qt_mmh3_64_v2_5 "SELECT MURMUR_HASH3_64_V2('1013199993_1756808272');"
    qt_mmh3_64_v2_6 "SELECT MURMUR_HASH3_64_V2('1020273884_1756808272');"

    // murmur_hash3_u64_v2 tests
    qt_mmh3_u64_v2_1 "SELECT MURMUR_HASH3_U64_V2(NULL);"
    qt_mmh3_u64_v2_2 "SELECT MURMUR_HASH3_U64_V2('1000209601_1756808272');"
    qt_mmh3_u64_v2_3 "SELECT MURMUR_HASH3_U64_V2('hello world');"
    qt_mmh3_u64_v2_4 "SELECT MURMUR_HASH3_U64_V2('apache doris');"
    qt_mmh3_u64_v2_5 "SELECT MURMUR_HASH3_U64_V2('1013199993_1756808272');"
    qt_mmh3_u64_v2_6 "SELECT MURMUR_HASH3_U64_V2('1020273884_1756808272');"
    qt_mmh3_u64_v2_7 "SELECT MURMUR_HASH3_U64_V2('');"
    qt_mmh3_u64_v2_8 "SELECT MURMUR_HASH3_U64_V2('a');"
    qt_mmh3_u64_v2_9 "SELECT MURMUR_HASH3_U64_V2('hello', 'world');"
    qt_mmh3_u64_v2_10 "SELECT MURMUR_HASH3_U64_V2('hello', 'world', '!');"

    // Validation: murmur_hash3_u64_v2 should equal (murmur_hash3_64_v2 & 2^64-1)
    def validate_mmh3_u64_v2 = { String... args ->
        def argList = args.collect { "'${it}'" }.join(', ')
        def u64_res = sql "SELECT MURMUR_HASH3_U64_V2(${argList});"
        def v2_masked = sql "SELECT CAST(MURMUR_HASH3_64_V2(${argList}) AS LARGEINT) & 18446744073709551615;"
        assertEquals(u64_res, v2_masked);
    }

    validate_mmh3_u64_v2('1000209601_1756808272');
    validate_mmh3_u64_v2('hello world');
    validate_mmh3_u64_v2('apache doris');
    validate_mmh3_u64_v2('1013199993_1756808272');
    validate_mmh3_u64_v2('1020273884_1756808272');
    validate_mmh3_u64_v2('');
    validate_mmh3_u64_v2('a');
    validate_mmh3_u64_v2('你好🤣');
    validate_mmh3_u64_v2('アパッチドリス');

    // Table-based tests for mmh3_64_v2 and mmh3_u64_v2
    sql "DROP TABLE IF EXISTS test_hash_tbl;"
    sql """
        CREATE TABLE test_hash_tbl (
            id INT,
            str_col VARCHAR(100)
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    sql """
        INSERT INTO test_hash_tbl VALUES
        (1, '1000209601_1756808272'),
        (2, 'hello world'),
        (3, NULL),
        (4, ''),
        (5, 'apache doris'),
        (6, '1013199993_1756808272'),
        (7, '1020273884_1756808272'),
        (8, '你好🤣'),
        (9, 'アパッチドリス');
    """

    qt_mmh3_64_v2_table "SELECT id, MURMUR_HASH3_64_V2(str_col) FROM test_hash_tbl ORDER BY id;"
    qt_mmh3_u64_v2_table "SELECT id, MURMUR_HASH3_U64_V2(str_col) FROM test_hash_tbl ORDER BY id;"

    sql "DROP TABLE IF EXISTS test_hash_tbl;"

    // Constant folding tests
    qt_mmh3_64_v2_fold_1 "SELECT MURMUR_HASH3_64_V2('test') + 1;"
    qt_mmh3_64_v2_fold_2 "SELECT MURMUR_HASH3_64_V2('a', 'b') * 2;"
    qt_mmh3_u64_v2_fold_1 "SELECT MURMUR_HASH3_U64_V2('test') + 1;"
    qt_mmh3_u64_v2_fold_2 "SELECT MURMUR_HASH3_U64_V2('a', 'b') * 2;"

    qt_sql "SELECT xxhash_32(null);"
    qt_sql "SELECT xxhash_32(\"hello\");"
    qt_sql "SELECT xxhash_32(\"hello\", \"world\");"

    qt_sql "SELECT xxhash_64(null);"
    qt_sql "SELECT xxhash_64(\"hello\");"
    qt_sql "SELECT xxhash_64(\"hello\", \"world\");"

    def xxhash_res = sql "SELECT xxhash_64(null);"
    def xxhash3_res = sql "SELECT xxhash3_64(null);"
    assertEquals(xxhash_res, xxhash3_res);

    xxhash_res = sql "SELECT xxhash_64(\"hello\");"
    xxhash3_res = sql "SELECT xxhash3_64(\"hello\");"
    assertEquals(xxhash_res, xxhash3_res);

    xxhash_res = sql "SELECT xxhash_64(\"hello\", \"world\");"
    xxhash3_res = sql "SELECT xxhash3_64(\"hello\", \"world\");"
    assertEquals(xxhash_res, xxhash3_res);
}
