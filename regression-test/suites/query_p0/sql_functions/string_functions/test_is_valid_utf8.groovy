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

suite("test_is_valid_utf8") {
    // basic valid UTF-8 strings
    qt_valid_1 "SELECT is_valid_utf8('hello');"
    qt_valid_2 "SELECT is_valid_utf8('');"
    qt_valid_3 "SELECT is_valid_utf8('Hello, 世界');"
    qt_valid_4 "SELECT is_valid_utf8('こんにちは');"
    qt_valid_5 "SELECT is_valid_utf8('123!@#');"

    // NULL handling
    qt_null_1 "SELECT is_valid_utf8(NULL);"

    // invalid UTF-8 strings constructed via unhex
    // 0x80: lone continuation byte
    qt_invalid_1 "SELECT is_valid_utf8(unhex('80'));"
    // 0xC3 0x28: invalid 2-byte sequence (second byte not continuation)
    qt_invalid_2 "SELECT is_valid_utf8(unhex('C328'));"
    // 0xE2 0x28 0xA1: invalid 3-byte sequence (second byte not continuation)
    qt_invalid_3 "SELECT is_valid_utf8(unhex('E228A1'));"
    // 0xF0 0x28 0x8C 0xBC: invalid 4-byte sequence (second byte not continuation)
    qt_invalid_4 "SELECT is_valid_utf8(unhex('F0288CBC'));"
    // 0xFE: not valid in UTF-8
    qt_invalid_5 "SELECT is_valid_utf8(unhex('FE'));"
    // 0xFF: not valid in UTF-8
    qt_invalid_6 "SELECT is_valid_utf8(unhex('FF'));"
    // overlong encoding of '/' (U+002F): 0xC0 0xAF
    qt_invalid_7 "SELECT is_valid_utf8(unhex('C0AF'));"
    // truncated 3-byte sequence: 0xE4 0xB8
    qt_invalid_8 "SELECT is_valid_utf8(unhex('E4B8'));"

    // alias isValidUTF8
    qt_alias_1 "SELECT isValidUTF8('hello');"
    qt_alias_2 "SELECT isValidUTF8('');"
    // alias with invalid bytes
    qt_alias_3 "SELECT isValidUTF8(unhex('80'));"

    // test with table data (including invalid UTF-8 via unhex)
    sql "DROP TABLE IF EXISTS test_is_valid_utf8_tbl"
    sql """
        CREATE TABLE test_is_valid_utf8_tbl (
            id INT,
            val VARCHAR(200)
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    sql """
        INSERT INTO test_is_valid_utf8_tbl VALUES
        (1, 'hello'),
        (2, ''),
        (3, 'Hello, 世界'),
        (4, NULL);
    """
    sql "INSERT INTO test_is_valid_utf8_tbl VALUES (5, unhex('C0AF'));"
    sql "INSERT INTO test_is_valid_utf8_tbl VALUES (6, unhex('FF'));"

    order_qt_table_1 "SELECT id, is_valid_utf8(val) FROM test_is_valid_utf8_tbl ORDER BY id;"

    // test fold const
    testFoldConst("SELECT is_valid_utf8('hello');")
    testFoldConst("SELECT is_valid_utf8('');")
    testFoldConst("SELECT is_valid_utf8(NULL);")
    testFoldConst("SELECT isValidUTF8('hello');")
}
