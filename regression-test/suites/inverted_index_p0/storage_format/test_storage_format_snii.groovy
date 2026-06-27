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

suite("test_storage_format_snii", "p0, nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_storage_format_snii"
    sql "DROP TABLE IF EXISTS test_storage_format_snii_bkd"

    sql """
        CREATE TABLE test_storage_format_snii (
          id INT NULL,
          body TEXT NULL,
          INDEX idx_body (`body`) USING INVERTED PROPERTIES(
            "parser" = "english",
            "support_phrase" = "true",
            "lower_case" = "true"
          ) COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "disable_auto_compaction" = "true",
          "inverted_index_storage_format" = "SNII"
        );
    """

    sql """
        INSERT INTO test_storage_format_snii VALUES
          (1, 'alpha beta gamma'),
          (2, 'alpha delta'),
          (3, 'beta epsilon'),
          (4, NULL),
          (5, 'quick brown fox'),
          (6, 'quick fox');
    """
    sql "sync"

    order_qt_match_any """
        SELECT id FROM test_storage_format_snii
        WHERE body MATCH_ANY 'alpha'
        ORDER BY id
    """
    order_qt_match_all """
        SELECT id FROM test_storage_format_snii
        WHERE body MATCH_ALL 'alpha beta'
        ORDER BY id
    """
    order_qt_match_phrase """
        SELECT id FROM test_storage_format_snii
        WHERE body MATCH_PHRASE 'quick brown'
        ORDER BY id
    """
    order_qt_null_bitmap """
        SELECT id FROM test_storage_format_snii
        WHERE body IS NULL
        ORDER BY id
    """

    test {
        sql """
            CREATE TABLE test_storage_format_snii_bkd (
              id INT NULL,
              score INT NULL,
              INDEX idx_score (`score`) USING INVERTED COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "inverted_index_storage_format" = "SNII"
            );
        """
        exception "SNII inverted index storage format"
    }
}
