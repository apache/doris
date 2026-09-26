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

suite("test_json_parse_invalid_number", "p0") {
    sql "DROP TABLE IF EXISTS test_json_parse_invalid_number"
    sql """
        CREATE TABLE test_json_parse_invalid_number (id INT, doc STRING) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num' = '1');
    """

    // ids 1..16 are malformed documents that must be rejected,
    // ids 21..29 are valid documents that must keep parsing.
    sql """
        INSERT INTO test_json_parse_invalid_number VALUES
            (1, '01'), (2, '[01]'), (3, '{"k":01}'),
            (4, '1.'), (5, '[1.]'), (6, '{"k":1.}'),
            (7, '00'), (8, '-01'), (9, '1e'), (10, '[1x]'), (11, '{"k":1.5x}'), (12, '1e400'),
            (13, '1 2'), (14, '18446744073709551616 0'), (15, '1.5 2'), (16, '[1] 2'),
            (21, '0'), (22, '-0'), (23, '[1 , 2 ]'), (24, '18446744073709551616'),
            (25, '{"k": -9223372036854775809 }'), (26, '1.5e3'), (27, '1e-400'),
            (28, '[18446744073709551615, 9223372036854775807]'),
            (29, '12345678901234567890123456789012345678901234567890');
    """

    order_qt_error_to_null """
        SELECT id, doc, json_parse_error_to_null(doc), json_parse_error_to_null(doc) IS NULL
        FROM test_json_parse_invalid_number ORDER BY id
    """
    order_qt_error_to_value """
        SELECT id, doc, json_parse_error_to_value(doc)
        FROM test_json_parse_invalid_number ORDER BY id
    """
    order_qt_cast """
        SELECT id, doc, CAST(doc AS JSON) FROM test_json_parse_invalid_number ORDER BY id
    """
    order_qt_strict_valid """
        SELECT id, doc, json_parse(doc) FROM test_json_parse_invalid_number WHERE id > 20 ORDER BY id
    """

    for (int id = 1; id <= 16; id++) {
        test {
            sql "SELECT json_parse(doc) FROM test_json_parse_invalid_number WHERE id = ${id}"
            exception "Parse json document failed"
        }
    }

    // constant inputs go through the same parser
    order_qt_const_error_to_null """
        SELECT json_parse_error_to_null('01'), json_parse_error_to_null('[1.]'),
               json_parse_error_to_null('{"k":01}'), json_parse_error_to_null('18446744073709551616')
    """
    order_qt_json_valid """
        SELECT id, doc, json_valid(doc) FROM test_json_parse_invalid_number ORDER BY id
    """
    test {
        sql "SELECT json_parse('{\"k\":01}')"
        exception "Parse json document failed"
    }
}
