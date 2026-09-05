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

// Regression test for: compaction fails on no-key duplicate table with only
// variant columns when the first variant column has unique_id=0.
// Root cause: TabletColumn::is_extracted_column() used "_parent_col_unique_id > 0"
// which incorrectly excluded subcolumns whose parent has unique_id=0.

suite("test_compaction_nokey_variant") {
    def variantV2Function = "parse_to_variant"

    sql "DROP TABLE IF EXISTS test_compaction_nokey_variant"
    sql """
        CREATE TABLE test_compaction_nokey_variant (
            v1 VARIANT,
            v2 VARIANT,
            v3 VARIANT
        )
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "enable_duplicate_without_keys_by_default" = "true"
        );
    """

    // Insert multiple batches to create multiple rowsets
    sql """INSERT INTO test_compaction_nokey_variant VALUES
        (${variantV2Function}('{"name":"Alice","age":30}'), ${variantV2Function}('{"city":"Beijing","zip":100000}'), ${variantV2Function}('{"score":95.5,"passed":true}')),
        (${variantV2Function}('{"name":"Bob","age":25}'), ${variantV2Function}('{"city":"Shanghai"}'), ${variantV2Function}('{"score":88.0,"passed":true}')),
        (${variantV2Function}('{"name":"Charlie"}'), ${variantV2Function}('{"city":"Shenzhen","zip":518000}'), ${variantV2Function}('{"score":72.3,"passed":false}'));"""

    sql """INSERT INTO test_compaction_nokey_variant VALUES (${variantV2Function}('{"name":"u1","age":10}'), ${variantV2Function}('{"city":"c1"}'), ${variantV2Function}('{"score":10.5}'));"""
    sql """INSERT INTO test_compaction_nokey_variant VALUES (${variantV2Function}('{"name":"u2","age":20}'), ${variantV2Function}('{"city":"c2"}'), ${variantV2Function}('{"score":20.5}'));"""
    sql """INSERT INTO test_compaction_nokey_variant VALUES (${variantV2Function}('{"name":"u3","age":30}'), ${variantV2Function}('{"city":"c3"}'), ${variantV2Function}('{"score":30.5}'));"""
    sql """INSERT INTO test_compaction_nokey_variant VALUES (${variantV2Function}('{"name":"u4","age":40}'), ${variantV2Function}('{"city":"c4"}'), ${variantV2Function}('{"score":40.5}'));"""
    sql """INSERT INTO test_compaction_nokey_variant VALUES (${variantV2Function}('{"name":"u5","age":50}'), ${variantV2Function}('{"city":"c5"}'), ${variantV2Function}('{"score":50.5}'));"""

    def supportedQuery = """SELECT cast(v1['name'] as text) c1, cast(v1['age'] as int),
        cast(v2['city'] as text), cast(v2['zip'] as int), cast(v3['score'] as double),
        cast(v3['passed'] as boolean) FROM test_compaction_nokey_variant ORDER BY c1"""

    qt_before_compaction_full_variant """SELECT sort_json_object_keys(cast(v1 as json)) c1,
        sort_json_object_keys(cast(v2 as json)) c2, sort_json_object_keys(cast(v3 as json)) c3
        FROM test_compaction_nokey_variant ORDER BY c1;"""
    qt_before_compaction_supported supportedQuery

    def rowCountBefore = sql "SELECT count() FROM test_compaction_nokey_variant"
    assertEquals(8, rowCountBefore[0][0])

    // Trigger cumulative compaction - reproduces the bug when is_extracted_column() is wrong
    trigger_and_wait_compaction("test_compaction_nokey_variant", "cumulative")

    // Verify data after compaction
    qt_after_compaction_full_variant """SELECT sort_json_object_keys(cast(v1 as json)) c1,
        sort_json_object_keys(cast(v2 as json)) c2, sort_json_object_keys(cast(v3 as json)) c3
        FROM test_compaction_nokey_variant ORDER BY c1;"""
    qt_after_compaction_supported supportedQuery

    def rowCountAfter = sql "SELECT count() FROM test_compaction_nokey_variant"
    assertEquals(8, rowCountAfter[0][0])
}
