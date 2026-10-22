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

suite("test_fl_get_binary") {
    def file = """CAST(NAMED_STRUCT('uri', 's3://bucket/a.png', 'offset', CAST(2 AS BIGINT),
        'size', CAST(3 AS BIGINT), 'content_type', 'image/png', 'checksum', 'ETAG:abc',
        'inline', X'00FF80') AS FILE)"""
    def foldMode = sql("SELECT @@enable_fold_constant_by_be")[0][0]
    try {
        for (def fold in [false, true]) {
            sql "SET enable_fold_constant_by_be = ${fold}"
            quickTest("fl_get_binary_constants_${fold}", """
                SELECT ELEMENT_AT(${file}, 'uri'), ELEMENT_AT(${file}, 'offset'),
                    ELEMENT_AT(${file}, 'size'), ELEMENT_AT(${file}, 'content_type'),
                    ELEMENT_AT(${file}, 'checksum'), FROM_BINARY(FL_GET_BINARY(${file}))
            """)
            quickTest("fl_get_binary_nulls_${fold}", """
                SELECT FL_GET_BINARY(NULL), FL_GET_BINARY(CAST(NULL AS FILE))
            """)
            for (def input in [file, "CAST(NULL AS FILE)"]) {
                for (def field in ["inline", "INLINE", "InLiNe"]) {
                    test {
                        sql "SELECT ELEMENT_AT(${input}, '${field}')"
                        exception "use FL_GET_BINARY"
                    }
                    test {
                        sql "SELECT (${input})['${field}']"
                        exception "use FL_GET_BINARY"
                    }
                }
            }
        }
    } finally {
        sql "SET enable_fold_constant_by_be = ${foldMode}"
    }

    sql "DROP TABLE IF EXISTS test_fl_get_values"
    sql """
        CREATE TABLE test_fl_get_values (id INT, uri STRING, offset_value BIGINT,
            size_value BIGINT, content_type STRING, checksum STRING, payload STRING)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')
    """
    sql """
        INSERT INTO test_fl_get_values VALUES
        (1, 's3://bucket/a.png', 2, 3, 'image/png', 'ETAG:abc', '00FF80'),
        (2, NULL, NULL, NULL, NULL, NULL, NULL),
        (3, 'urn:empty', 0, 0, NULL, NULL, ''),
        (4, 'urn:metadata', NULL, NULL, NULL, NULL, NULL)
    """
    def rows = """SELECT id, CAST(IF(id = 2, NULL, NAMED_STRUCT('uri', uri,
        'offset', offset_value, 'size', size_value, 'content_type', content_type,
        'checksum', checksum, 'inline', CAST(UNHEX(payload) AS VARBINARY))) AS FILE) AS f
        FROM test_fl_get_values"""
    qt_fl_get_binary_columns_1 """
        SELECT id, ELEMENT_AT(f, 'uri'), ELEMENT_AT(f, 'offset'), ELEMENT_AT(f, 'size'),
            ELEMENT_AT(f, 'content_type'), ELEMENT_AT(f, 'checksum'), FROM_BINARY(FL_GET_BINARY(f))
        FROM (${rows}) t ORDER BY id
    """
    qt_fl_get_binary_nested_1 """
        SELECT id, FROM_BINARY(FL_GET_BINARY(ELEMENT_AT(ARRAY(f), 1))),
            FROM_BINARY(FL_GET_BINARY(ELEMENT_AT(MAP('asset', f), 'asset'))),
            FROM_BINARY(FL_GET_BINARY(ELEMENT_AT(NAMED_STRUCT('asset', f), 'asset')))
        FROM (${rows}) t ORDER BY id
    """
    qt_fl_get_binary_aggregate_1 """
        SELECT COUNT(ELEMENT_AT(f, 'uri')), SUM(ELEMENT_AT(f, 'size')), COUNT(FL_GET_BINARY(f))
        FROM (${rows}) t
    """
    qt_fl_get_binary_struct_1 "SELECT FROM_BINARY(ELEMENT_AT(NAMED_STRUCT('inline', X'00FF80'), 'inline'))"
    qt_fl_get_binary_filter_1 """
        SELECT id, FROM_BINARY(FL_GET_BINARY(f)) FROM (${rows}) t WHERE id IN (1, 3) ORDER BY id
    """
    for (def getter in ["fl_get_binary"]) {
        testFoldConst("SELECT ${getter}(${file})")
        testFoldConst("SELECT ${getter}(NULL)")
        explain {
            sql "SELECT ${getter}(f) FROM (${rows}) t"
            contains "fl_get_binary"
        }
        for (def argument in ["1", "'uri'", "CAST('{}' AS JSON)", "CAST('{}' AS VARIANT)",
                              "NAMED_STRUCT('uri', 'urn:x')", "ARRAY(${file})"]) {
            test {
                sql "SELECT ${getter}(${argument})"
                exception "Can not find the compatibility function signature"
            }
        }
        test {
            sql "SELECT ${getter}()"
            exception "Can not found function"
        }
        test {
            sql "SELECT ${getter}(${file}, 'uri')"
            exception "Can not found function"
        }
    }
    for (def getter in ["fl_get_uri", "fl_get_offset", "fl_get_size", "fl_get_content_type",
                       "fl_get_checksum", "fl_get_inline"]) {
        test {
            sql "SELECT ${getter}(${file})"
            exception "Can not found function"
        }
    }
}
