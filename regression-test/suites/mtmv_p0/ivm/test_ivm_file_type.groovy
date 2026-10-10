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

suite("test_ivm_file_type") {
    sql "DROP MATERIALIZED VIEW IF EXISTS test_ivm_file_type_mv"
    sql "DROP TABLE IF EXISTS test_ivm_file_type_base"
    sql """
        CREATE TABLE test_ivm_file_type_base (id INT NOT NULL, f FILE, files ARRAY<FILE>)
        UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                   "binlog.enable"="true", "binlog.format"="ROW", "binlog.need_historical_value"="true")
    """
    sql """
        INSERT INTO test_ivm_file_type_base VALUES
        (1, CAST(NAMED_STRUCT('uri', 'urn:first', 'size', CAST(3 AS BIGINT), 'offset', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE),
            CAST(ARRAY(NAMED_STRUCT('uri', 'urn:nested', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL), NULL) AS ARRAY<FILE>)),
        (2, NULL, NULL)
    """
    sql """
        CREATE MATERIALIZED VIEW test_ivm_file_type_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1 PROPERTIES("replication_num"="1")
        AS SELECT id, f, files, ELEMENT_AT(f, 'size') AS bytes FROM test_ivm_file_type_base
    """
    sql "REFRESH MATERIALIZED VIEW test_ivm_file_type_mv COMPLETE"
    waitingMTMVTaskFinishedByMvName("test_ivm_file_type_mv")
    qt_complete "SELECT id, f, files, bytes FROM test_ivm_file_type_mv ORDER BY id"

    sql """
        INSERT INTO test_ivm_file_type_base VALUES
        (1, CAST(NAMED_STRUCT('uri', 'urn:updated', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE), ARRAY(NULL)),
        (3, CAST(NAMED_STRUCT('uri', 'urn:inserted', 'size', CAST(0 AS BIGINT), 'offset', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE), NULL)
    """
    sql "DELETE FROM test_ivm_file_type_base WHERE id = 2"
    sql "REFRESH MATERIALIZED VIEW test_ivm_file_type_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("test_ivm_file_type_mv")
    qt_incremental "SELECT id, f, files, bytes FROM test_ivm_file_type_mv ORDER BY id"
    qt_source "SELECT id, f, files, ELEMENT_AT(f, 'size') FROM test_ivm_file_type_base ORDER BY id"
}
