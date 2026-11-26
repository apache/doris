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

suite("test_storage_format_snii_utf8_wildcard", "p0, nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_snii_utf8_wildcard"
    sql "DROP TABLE IF EXISTS test_v3_utf8_wildcard"

    sql """
        CREATE TABLE test_snii_utf8_wildcard (
          id INT,
          body VARCHAR(100),
          INDEX idx_body (`body`) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "inverted_index_storage_format" = "SNII"
        )
    """

    sql """
        CREATE TABLE test_v3_utf8_wildcard (
          id INT,
          body VARCHAR(100),
          INDEX idx_body (`body`) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "inverted_index_storage_format" = "V3"
        )
    """

    sql """
        INSERT INTO test_snii_utf8_wildcard VALUES
          (1, 'a猫b'),
          (2, 'a🔥b'),
          (3, 'a猫猫b'),
          (4, 'aéb'),
          (5, 'ascii'),
          (6, NULL),
          (7, CAST(UNHEX('61FF') AS STRING))
    """
    sql """
        INSERT INTO test_v3_utf8_wildcard
        SELECT * FROM test_snii_utf8_wildcard
    """
    sql "sync"

    order_qt_malformed_keyword_wildcards """
        SELECT pattern, id, HEX(body)
        FROM (
          SELECT 'prefix' AS pattern, id, body FROM test_snii_utf8_wildcard
          WHERE id = 7 AND SEARCH('body:a*', '{"mode":"standard"}')
          UNION ALL
          SELECT 'double_star' AS pattern, id, body FROM test_snii_utf8_wildcard
          WHERE id = 7 AND SEARCH('body:**', '{"mode":"standard"}')
          UNION ALL
          SELECT 'single_star' AS pattern, id, body FROM test_snii_utf8_wildcard
          WHERE id = 7 AND SEARCH('body:*', '{"mode":"standard"}')
        ) results
        ORDER BY pattern, id
    """

    order_qt_utf8_single_code_point """
        SELECT format, id
        FROM (
          SELECT 'SNII' AS format, id FROM test_snii_utf8_wildcard
          WHERE SEARCH('body:a?b', '{"mode":"standard"}')
          UNION ALL
          SELECT 'V3' AS format, id FROM test_v3_utf8_wildcard
          WHERE SEARCH('body:a?b', '{"mode":"standard"}')
        ) results
        ORDER BY format, id
    """

    order_qt_utf8_two_code_points """
        SELECT format, id
        FROM (
          SELECT 'SNII' AS format, id FROM test_snii_utf8_wildcard
          WHERE SEARCH('body:a??b', '{"mode":"standard"}')
          UNION ALL
          SELECT 'V3' AS format, id FROM test_v3_utf8_wildcard
          WHERE SEARCH('body:a??b', '{"mode":"standard"}')
        ) results
        ORDER BY format, id
    """

    order_qt_utf8_three_code_points """
        SELECT format, matched
        FROM (
          SELECT 'SNII' AS format, COUNT(*) AS matched FROM test_snii_utf8_wildcard
          WHERE SEARCH('body:a???b', '{"mode":"standard"}')
          UNION ALL
          SELECT 'V3' AS format, COUNT(*) AS matched FROM test_v3_utf8_wildcard
          WHERE SEARCH('body:a???b', '{"mode":"standard"}')
        ) results
        ORDER BY format
    """
}
