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

suite("test_regexp_function_rewrite") {
    sql "DROP TABLE IF EXISTS test_regexp_function_rewrite"
    sql """
        CREATE TABLE test_regexp_function_rewrite (
            id INT,
            url STRING
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO test_regexp_function_rewrite VALUES
            (1, 'https://www.example.com/path/index.html'),
            (2, 'http://doris.apache.org/docs'),
            (3, 'https://sub.domain.org/'),
            (4, 'ftp://www.example.com/path'),
            (5, 'https://www.example.com'),
            (6, NULL)
    """

    def shapeEnabled = sql """
        EXPLAIN VERBOSE
        SELECT regexp_replace(url, '^https?://(?:www\\\\.)?([^/]+)/.*\$', '\\\\1')
        FROM test_regexp_function_rewrite
    """
    assertTrue(shapeEnabled.toString().contains("regexp_replace_one"))

    def inlineFlagShapeEnabled = sql """
        EXPLAIN VERBOSE
        SELECT
            regexp_replace(concat(url, char(10), url), '(?m).*\$', 'x'),
            regexp_extract(concat(url, char(10), 'tail'), '(?-s)^([^/]+).*\$', 1)
        FROM test_regexp_function_rewrite
    """
    assertTrue(!inlineFlagShapeEnabled.toString().contains("regexp_replace_one"))

    def resultEnabled = sql """
        SELECT
            id,
            regexp_replace(url, '^https?://(?:www\\\\.)?([^/]+)/.*\$', '\\\\1') AS host_by_replace,
            regexp_replace(url, '.*\\\\.org\$', 'ORG') AS org_suffix,
            regexp_extract(url, '^https?://(?:www\\\\.)?([^/]+).*\$', 1) AS host_by_extract,
            regexp_replace(concat(url, char(10), url), '(?m).*\$', 'x') AS multiline_replace,
            regexp_extract(concat(url, char(10), 'tail'), '(?-s)^([^/]+).*\$', 1) AS dot_mode_extract
        FROM test_regexp_function_rewrite
        ORDER BY id
    """

    sql "SET disable_nereids_expression_rules='REGEXP_FUNCTION_REWRITE'"

    def shapeDisabled = sql """
        EXPLAIN VERBOSE
        SELECT regexp_replace(url, '^https?://(?:www\\\\.)?([^/]+)/.*\$', '\\\\1')
        FROM test_regexp_function_rewrite
    """
    assertTrue(shapeDisabled.toString().contains("regexp_replace"))
    assertTrue(!shapeDisabled.toString().contains("regexp_replace_one"))

    def resultDisabled = sql """
        SELECT
            id,
            regexp_replace(url, '^https?://(?:www\\\\.)?([^/]+)/.*\$', '\\\\1') AS host_by_replace,
            regexp_replace(url, '.*\\\\.org\$', 'ORG') AS org_suffix,
            regexp_extract(url, '^https?://(?:www\\\\.)?([^/]+).*\$', 1) AS host_by_extract,
            regexp_replace(concat(url, char(10), url), '(?m).*\$', 'x') AS multiline_replace,
            regexp_extract(concat(url, char(10), 'tail'), '(?-s)^([^/]+).*\$', 1) AS dot_mode_extract
        FROM test_regexp_function_rewrite
        ORDER BY id
    """
    assertEquals(resultEnabled, resultDisabled)
}
