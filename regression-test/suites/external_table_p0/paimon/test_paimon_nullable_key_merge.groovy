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

suite("test_paimon_nullable_key_merge", "p0,external,paimon") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enablePaimonTest"))) {
        return
    }
    def endpoint = context.config.otherConfigs.get("externalEnvIp")
    def port = context.config.otherConfigs.get("iceberg_minio_port")
    sql "DROP CATALOG IF EXISTS paimon_nullable_key_merge"
    sql """CREATE CATALOG paimon_nullable_key_merge PROPERTIES (
        'type'='paimon', 'paimon.catalog.type'='filesystem', 'fs.s3.support'='true',
        'warehouse'='s3://warehouse/wh', 's3.endpoint'='http://${endpoint}:${port}',
        's3.access_key'='admin', 's3.secret_key'='password',
        's3.region'='us-east-1', 'use_path_style'='true')"""
    sql "SWITCH paimon_nullable_key_merge"
    sql "DROP DATABASE IF EXISTS paimon_nullable_key_merge_db FORCE"
    sql "CREATE DATABASE paimon_nullable_key_merge_db"
    sql "USE paimon_nullable_key_merge_db"
    sql """CREATE TABLE nullable_target (id INT NULL, v INT) ENGINE=paimon
        PROPERTIES ('primary-key'='id', 'primary-key.nullable'='true',
            'bucket'='1', 'write-only'='true', 'deletion-vectors.enabled'='false')"""
    sql "INSERT INTO nullable_target VALUES (NULL,20), (1,200), (3,300)"

    sql """MERGE INTO nullable_target t
        USING (SELECT CAST(NULL AS INT) AS id, 99 AS v) s ON t.id <=> s.id
        WHEN MATCHED THEN UPDATE SET v=s.v"""
    order_qt_null_key_update "SELECT * FROM nullable_target ORDER BY id NULLS FIRST"

    // Ordinary equality does not match NULL keys, even with only a MATCHED branch.
    sql """MERGE INTO nullable_target t
        USING (SELECT CAST(NULL AS INT) AS id, 999 AS v) s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET v=s.v"""
    order_qt_null_key_equal_no_match "SELECT * FROM nullable_target ORDER BY id NULLS FIRST"

    sql """MERGE INTO nullable_target t
        USING (SELECT 2 AS id, 222 AS v) s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET v=s.v
        WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v)"""
    sql """MERGE INTO nullable_target t
        USING (SELECT 1 AS id, 111 AS v) s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET v=s.v
        WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v)"""
    order_qt_matched_and_insert "SELECT * FROM nullable_target ORDER BY id NULLS FIRST"

    sql """MERGE INTO nullable_target t
        USING (SELECT CAST(NULL AS INT) AS id) s ON t.id <=> s.id
        WHEN MATCHED THEN DELETE"""
    order_qt_null_key_delete "SELECT * FROM nullable_target ORDER BY id NULLS FIRST"
    sql """MERGE INTO nullable_target t
        USING (SELECT CAST(NULL AS INT) AS id, 999 AS v) s ON t.id <=> s.id
        WHEN MATCHED THEN UPDATE SET v=s.v"""
    order_qt_missing_null_key_no_match "SELECT * FROM nullable_target ORDER BY id NULLS FIRST"

    // A null-extended target must choose INSERT, including a NULL source key.
    sql """MERGE INTO nullable_target t
        USING (SELECT CAST(NULL AS INT) AS id, 77 AS v) s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET v=s.v
        WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v)"""
    order_qt_insert_null_key "SELECT * FROM nullable_target ORDER BY id NULLS FIRST"
    sql """MERGE INTO nullable_target t
        USING (SELECT CAST(NULL AS INT) AS id, 88 AS v) s ON t.id <=> s.id
        WHEN MATCHED AND s.v < 0 THEN DELETE
        WHEN MATCHED THEN UPDATE SET v=s.v"""
    order_qt_null_key_branch_priority "SELECT * FROM nullable_target ORDER BY id NULLS FIRST"
    sql """MERGE INTO nullable_target t
        USING (SELECT CAST(NULL AS INT) AS id, 999 AS v) s ON t.id <=> s.id
        WHEN MATCHED AND s.v < 0 THEN UPDATE SET v=s.v"""
    order_qt_null_key_false_condition "SELECT * FROM nullable_target ORDER BY id NULLS FIRST"
    test {
        sql """MERGE INTO nullable_target t
            USING (SELECT CAST(NULL AS INT) AS id, 101 AS v
                   UNION ALL SELECT CAST(NULL AS INT), 102) s ON t.id <=> s.id
            WHEN MATCHED THEN UPDATE SET v=s.v"""
        exception "Connector MERGE matched one target row with multiple source rows"
    }
    order_qt_null_key_duplicate_unchanged "SELECT * FROM nullable_target ORDER BY id NULLS FIRST"
    test {
        sql """MERGE INTO nullable_target t
            USING (SELECT CAST(NULL AS INT) AS id, 101 AS v
                   UNION ALL SELECT CAST(NULL AS INT), 102) s ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v)"""
        exception "Connector MERGE attempted to insert multiple rows with the same primary key"
    }
    order_qt_duplicate_null_insert_unchanged "SELECT * FROM nullable_target ORDER BY id NULLS FIRST"
    sql """MERGE INTO nullable_target t
        USING (SELECT 1 AS id, 999 AS v) s ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v)"""
    sql """MERGE INTO nullable_target t
        USING (SELECT 4 AS id, 444 AS v) s ON t.id = s.id
        WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v)"""
    order_qt_insert_only "SELECT * FROM nullable_target ORDER BY id NULLS FIRST"
    // User expressions must not bind to the private marker if a source column shares its name.
    sql """MERGE INTO nullable_target t
        USING (SELECT 5 AS id, 555 AS __DORIS_CHANGELOG_TARGET_PRESENT__) s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET v=__DORIS_CHANGELOG_TARGET_PRESENT__
        WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,__DORIS_CHANGELOG_TARGET_PRESENT__)"""
    order_qt_marker_name_collision "SELECT * FROM nullable_target ORDER BY id NULLS FIRST"

    sql """CREATE TABLE composite_target (k1 INT NULL, k2 INT NULL, v INT) ENGINE=paimon
        PROPERTIES ('primary-key'='k1,k2', 'primary-key.nullable'='true',
            'bucket'='1', 'write-only'='true', 'deletion-vectors.enabled'='false')"""
    sql "INSERT INTO composite_target VALUES (NULL,1,10), (1,NULL,20), (NULL,NULL,30), (1,1,40)"
    sql """MERGE INTO composite_target t
        USING (SELECT CAST(NULL AS INT) AS k1, 1 AS k2, 110 AS v
               UNION ALL SELECT 1, CAST(NULL AS INT), 120
               UNION ALL SELECT CAST(NULL AS INT), CAST(NULL AS INT), 130
               UNION ALL SELECT 1, 1, 140) s
        ON t.k1 <=> s.k1 AND t.k2 <=> s.k2
        WHEN MATCHED THEN UPDATE SET v=s.v"""
    order_qt_composite_update "SELECT * FROM composite_target ORDER BY k1 NULLS FIRST,k2 NULLS FIRST"
    // Include an unmatched source row: MATCHED-only DELETE must not materialize it.
    sql """MERGE INTO composite_target t
        USING (SELECT CAST(NULL AS INT) AS k1, CAST(NULL AS INT) AS k2
               UNION ALL SELECT 99, 99) s
        ON t.k1 <=> s.k1 AND t.k2 <=> s.k2
        WHEN MATCHED THEN DELETE"""
    order_qt_composite_delete "SELECT * FROM composite_target ORDER BY k1 NULLS FIRST,k2 NULLS FIRST"
}
