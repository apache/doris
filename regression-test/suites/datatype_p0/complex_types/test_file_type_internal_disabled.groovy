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

suite("test_file_type_internal_disabled") {
    for (def type : ["FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>",
                     "ARRAY<STRUCT<f:FILE>>", "STRUCT<f:MAP<STRING,ARRAY<FILE>>>"]) {
        for (def model : ["DUPLICATE KEY(id)", "UNIQUE KEY(id)", "AGGREGATE KEY(id)"]) {
            def aggregation = model.startsWith("AGGREGATE") ? " REPLACE" : ""
            test {
                sql """
                    CREATE TABLE test_file_type_disabled (id INT, f ${type}${aggregation})
                    ${model} DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES("replication_num"="1")
                """
                exception "FILE columns are not supported in internal tables"
            }
        }
        test {
            sql """
                CREATE TABLE test_file_type_disabled_ctas
                DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")
                AS SELECT 1 AS id, CAST(NULL AS ${type}) AS f
            """
            exception "FILE columns are not supported in internal tables"
        }
    }

    sql "DROP TABLE IF EXISTS test_file_type_disabled_base"
    sql """
        CREATE TABLE test_file_type_disabled_base (id INT, f STRING)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1")
    """
    for (def type : ["FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>"]) {
        test {
            sql "ALTER TABLE test_file_type_disabled_base ADD COLUMN added ${type}"
            exception "FILE columns are not supported in internal tables"
        }
        test {
            sql "ALTER TABLE test_file_type_disabled_base MODIFY COLUMN f ${type}"
            exception "FILE columns are not supported in internal tables"
        }
    }
    for (def type : ["FILE", "ARRAY<FILE>", "STRUCT<f:FILE>", "MAP<STRING,FILE>"]) {
        test {
            sql """
                CREATE MATERIALIZED VIEW test_file_type_disabled_mv
                BUILD DEFERRED REFRESH COMPLETE ON MANUAL
                DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")
                AS SELECT id, CAST(NULL AS ${type}) AS f FROM test_file_type_disabled_base
            """
            exception "FILE columns are not supported in internal tables"
        }
    }
    qt_inline """
        SELECT f, ELEMENT_AT(f, 'uri'), HEX(CAST(ELEMENT_AT(f, 'inline') AS STRING)), f IS NULL
        FROM (SELECT CAST(NAMED_STRUCT('uri', NULL, 'offset', NULL, 'size', NULL,
                    'content_type', NULL, 'checksum', NULL, 'inline', X'00FF') AS FILE) AS f) t
    """
    qt_count "SELECT COUNT(f) FROM (SELECT CAST(NULL AS FILE) AS f) t"
    sql "DROP TABLE IF EXISTS test_file_type_disabled_scalar"
    sql """
        CREATE TABLE test_file_type_disabled_scalar
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")
        AS SELECT 1 AS id, ELEMENT_AT(f, 'uri') AS uri,
                  HEX(CAST(ELEMENT_AT(f, 'inline') AS STRING)) AS inline_hex
        FROM (SELECT CAST(NAMED_STRUCT('uri', NULL, 'offset', NULL, 'size', NULL,
                    'content_type', NULL, 'checksum', NULL, 'inline', X'00FF') AS FILE) AS f) t
    """
    qt_scalar "SELECT id, uri, inline_hex FROM test_file_type_disabled_scalar ORDER BY id"

}
