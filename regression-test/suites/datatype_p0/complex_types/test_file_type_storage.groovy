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

suite("test_file_type_storage") {
    sql "DROP MATERIALIZED VIEW IF EXISTS test_file_type_async_mv"
    sql "DROP TABLE IF EXISTS test_file_type_mow"
    sql """
        CREATE TABLE test_file_type_mow (id INT NOT NULL, f FILE, other INT)
        UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                   "store_row_column"="true")
    """
    sql """
        INSERT INTO test_file_type_mow VALUES
        (1, CAST(NAMED_STRUCT('uri', 'urn:old', 'size', CAST(8 AS BIGINT), 'offset', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE), 10),
        (2, NULL, 20)
    """
    qt_point_lookup "SELECT * FROM test_file_type_mow WHERE id = 1"
    test {
        sql """UPDATE test_file_type_mow SET f = NAMED_STRUCT('uri', 'urn:updated') WHERE id = 1"""
        exception "Cannot implicitly convert"
    }
    sql """
        UPDATE test_file_type_mow SET f = CAST(NAMED_STRUCT(
            'uri', 'urn:updated', 'offset', NULL, 'size', NULL,
            'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE) WHERE id = 1
    """
    qt_atomic_update "SELECT id, f, ELEMENT_AT(f, 'size'), other FROM test_file_type_mow ORDER BY id"
    sql "SET enable_unique_key_partial_update = true"
    sql "INSERT INTO test_file_type_mow(id, other) VALUES (1, 11)"
    sql """
        INSERT INTO test_file_type_mow(id, f)
        VALUES (2, CAST(NAMED_STRUCT('uri', 'urn:partial', 'size', CAST(4 AS BIGINT), 'offset', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE))
    """
    sql "SET enable_unique_key_partial_update = false"
    qt_partial_update "SELECT * FROM test_file_type_mow ORDER BY id"

    sql "DROP TABLE IF EXISTS test_file_type_replace"
    sql """
        CREATE TABLE test_file_type_replace (
            id INT NOT NULL, f FILE REPLACE, retained FILE REPLACE_IF_NOT_NULL)
        AGGREGATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1")
    """
    sql """
        INSERT INTO test_file_type_replace VALUES
        (1, CAST(NAMED_STRUCT('uri', 'urn:old', 'size', CAST(9 AS BIGINT), 'offset', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE),
            CAST(NAMED_STRUCT('uri', 'urn:retained', 'size', CAST(3 AS BIGINT), 'offset', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE))
    """
    sql """
        INSERT INTO test_file_type_replace VALUES
        (1, CAST(NAMED_STRUCT('uri', 'urn:new', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE), NULL)
    """
    qt_replace "SELECT * FROM test_file_type_replace ORDER BY id"
    sql """
        INSERT INTO test_file_type_replace VALUES
        (1, NULL, CAST(NAMED_STRUCT('uri', 'urn:replacement', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE))
    """
    qt_replace_null "SELECT * FROM test_file_type_replace ORDER BY id"

    sql "DROP TABLE IF EXISTS test_file_type_schema"
    sql """
        CREATE TABLE test_file_type_schema (id INT NOT NULL, f FILE NOT NULL)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1")
    """
    sql """
        INSERT INTO test_file_type_schema VALUES
        (1, CAST(NAMED_STRUCT('uri', 'urn:schema', 'offset', NULL, 'size', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE))
    """
    sql "ALTER TABLE test_file_type_schema MODIFY COLUMN f FILE NULL"
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE TableName='test_file_type_schema'
               ORDER BY CreateTime DESC LIMIT 1"""
        time 600
    }
    sql "ALTER TABLE test_file_type_schema ADD COLUMN added FILE NULL DEFAULT NULL"
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE TableName='test_file_type_schema'
               ORDER BY CreateTime DESC LIMIT 1"""
        time 600
    }
    sql "ALTER TABLE test_file_type_schema RENAME COLUMN f renamed"
    sql "INSERT INTO test_file_type_schema VALUES (2, NULL, NULL)"
    qt_schema "SELECT * FROM test_file_type_schema ORDER BY id"
    test {
        sql "ALTER TABLE test_file_type_schema MODIFY COLUMN renamed STRING"
        exception "FILE"
    }
    sql "ALTER TABLE test_file_type_schema DROP COLUMN added"
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE TableName='test_file_type_schema'
               ORDER BY CreateTime DESC LIMIT 1"""
        time 600
    }
    qt_schema_drop "SELECT * FROM test_file_type_schema ORDER BY id"

    sql "DROP VIEW IF EXISTS test_file_type_view"
    sql "CREATE VIEW test_file_type_view AS SELECT id, renamed AS f FROM test_file_type_schema"
    qt_view "SELECT id, f, ELEMENT_AT(f, 'uri') FROM test_file_type_view ORDER BY id"
    sql "DROP TABLE IF EXISTS test_file_type_like"
    sql "CREATE TABLE test_file_type_like LIKE test_file_type_schema"
    sql "INSERT INTO test_file_type_like SELECT * FROM test_file_type_schema"
    qt_like "SELECT * FROM test_file_type_like ORDER BY id"
    sql "DROP TABLE IF EXISTS test_file_type_ctas"
    sql """
        CREATE TABLE test_file_type_ctas DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1") AS SELECT * FROM test_file_type_schema
    """
    qt_ctas "SELECT * FROM test_file_type_ctas ORDER BY id"
    sql "INSERT OVERWRITE TABLE test_file_type_like SELECT * FROM test_file_type_ctas"
    qt_overwrite "SELECT * FROM test_file_type_like ORDER BY id"

    sql "DROP TABLE IF EXISTS test_file_type_generated"
    sql """
        CREATE TABLE test_file_type_generated (
            id INT NOT NULL, f FILE,
            uri VARCHAR(65533) GENERATED ALWAYS AS (ELEMENT_AT(f, 'uri')),
            bytes BIGINT GENERATED ALWAYS AS (ELEMENT_AT(f, 'size')))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1")
    """
    sql """
        INSERT INTO test_file_type_generated(id, f) VALUES
        (1, CAST(NAMED_STRUCT('uri', 'urn:generated', 'size', CAST(7 AS BIGINT), 'offset', NULL, 'content_type', NULL, 'checksum', NULL, 'inline', NULL) AS FILE)), (2, NULL)
    """
    qt_generated "SELECT * FROM test_file_type_generated ORDER BY id"

    createMV("""
        CREATE MATERIALIZED VIEW test_file_type_sync_mv
        AS SELECT id AS mv_id, f AS mv_f FROM test_file_type_generated ORDER BY mv_id
    """)
    qt_sync_mv """
        SELECT mv_id AS id, mv_f AS f FROM test_file_type_generated INDEX test_file_type_sync_mv ORDER BY mv_id
    """
    sql """
        CREATE MATERIALIZED VIEW test_file_type_async_mv
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")
        AS SELECT id, f, ELEMENT_AT(f, 'size') AS bytes FROM test_file_type_generated
    """
    sql "REFRESH MATERIALIZED VIEW test_file_type_async_mv COMPLETE"
    waitingMTMVTaskFinishedByMvName("test_file_type_async_mv")
    qt_async_mv "SELECT * FROM test_file_type_async_mv ORDER BY id"
}
