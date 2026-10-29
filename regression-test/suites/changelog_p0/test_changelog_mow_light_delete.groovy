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

suite("test_changelog_mow_light_delete", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_changelog_mow_light_delete_without_history FORCE"
    sql "DROP TABLE IF EXISTS test_changelog_mow_light_delete_with_history FORCE"
    sql "DROP TABLE IF EXISTS test_changelog_dup_delete FORCE"

    sql """
        CREATE TABLE test_changelog_mow_light_delete_without_history (
            k INT,
            v STRING,
            jv VARIANT NULL
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "enable_mow_light_delete" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "false"
        )
    """

    sql "INSERT INTO test_changelog_mow_light_delete_without_history VALUES (1, 'one', parse_to_variant('{\"k\": 1, \"d\": {\"row\": \"1 one\"}}')), (2, 'two', parse_to_variant('{\"k\": 2, \"d\": {\"row\": \"2 two\"}}'))"
    sql "DELETE FROM test_changelog_mow_light_delete_without_history WHERE k = 1"

    order_qt_without_history_base """
        SELECT k, v,
               jv,
               jv['d']['row']
        FROM test_changelog_mow_light_delete_without_history
    """

    qt_without_history_changelog """
        SELECT __DORIS_CHANGELOG_OP__ AS op, k, v,
               jv,
               jv['d']['row']
        FROM changelog("table" = "test_changelog_mow_light_delete_without_history")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """

    sql """
        CREATE TABLE test_changelog_mow_light_delete_with_history (
            k INT,
            v STRING,
            jv VARIANT NULL
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "enable_mow_light_delete" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """

    sql "INSERT INTO test_changelog_mow_light_delete_with_history VALUES (1, 'one', parse_to_variant('{\"k\": 1, \"d\": {\"row\": \"1 one\"}}')), (2, 'two', parse_to_variant('{\"k\": 2, \"d\": {\"row\": \"2 two\"}}'))"
    sql "DELETE FROM test_changelog_mow_light_delete_with_history WHERE k = 1"

    order_qt_with_history_base """
        SELECT k, v,
               jv,
               jv['d']['row']
        FROM test_changelog_mow_light_delete_with_history
    """

    qt_with_history_changelog """
        SELECT __DORIS_CHANGELOG_OP__ AS op, k, v, __DORIS_CHANGELOG_BEFORE_v__,
               jv,
               __DORIS_CHANGELOG_BEFORE_jv__,
               jv['d']['row']
        FROM changelog("table" = "test_changelog_mow_light_delete_with_history")
        ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
    """

    sql """
        CREATE TABLE test_changelog_dup_delete (
            k INT,
            v STRING,
            jv VARIANT NULL
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true"
        )
    """

    sql "INSERT INTO test_changelog_dup_delete VALUES (1, 'one', parse_to_variant('{\"k\": 1, \"d\": {\"row\": \"1 one\"}}'))"
    test {
        sql "DELETE FROM test_changelog_dup_delete WHERE k = 1"
        exception "DELETE with predicates is not supported when changelog is enabled"
    }
}
