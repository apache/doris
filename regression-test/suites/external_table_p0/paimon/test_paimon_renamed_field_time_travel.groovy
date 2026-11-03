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

suite("test_paimon_renamed_field_time_travel", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalogName = "test_paimon_renamed_field_time_travel"
    String dbName = "paimon_renamed_field_time_travel_db"

    def latestSnapshotId = { String tableName ->
        def rows = spark_paimon """
            SELECT snapshot_id FROM paimon.${dbName}.`${tableName}\$snapshots`
            ORDER BY snapshot_id DESC LIMIT 1
        """
        assertEquals(1, rows.size())
        return rows[0][0].toString()
    }

    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type'='paimon',
            'warehouse'='s3://warehouse/wh',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.path.style.access'='true'
        )
    """

    try {
        spark_paimon_multi """
            CREATE DATABASE IF NOT EXISTS paimon.${dbName};
            DROP TABLE IF EXISTS paimon.${dbName}.sequence_timeline;
            CREATE TABLE paimon.${dbName}.sequence_timeline (
                id INT, old_sequence BIGINT, payload STRING
            ) USING paimon
            TBLPROPERTIES (
                'primary-key'='id',
                'bucket'='1',
                'file.format'='parquet',
                'sequence.field'='old_sequence'
            );
            INSERT INTO paimon.${dbName}.sequence_timeline
                VALUES (1, 10, 'base'), (2, 20, 'retained');
        """
        String sequenceBase = latestSnapshotId("sequence_timeline")
        spark_paimon_multi """
            CALL paimon.sys.create_tag(
                table => '${dbName}.sequence_timeline', tag => 'sequence_base'
            );
            ALTER TABLE paimon.${dbName}.sequence_timeline
                RENAME COLUMN old_sequence TO current_sequence;
        """

        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql """refresh table sequence_timeline"""
        // A schema-only rename updates sequence.field without creating a data snapshot.
        // Latest reads must keep those two pieces of metadata from the same schema.
        assertEquals(sequenceBase, latestSnapshotId("sequence_timeline"))
        [false, true].each { forceJni ->
            sql """set force_jni_scanner=${forceJni}"""
            "order_qt_sequence_schema_only_${forceJni}" """
                SELECT id, current_sequence, payload FROM sequence_timeline ORDER BY id
            """
        }

        spark_paimon """
            INSERT INTO paimon.${dbName}.sequence_timeline VALUES
                (1, 30, 'updated'), (2, 5, 'stale'), (3, 15, 'new')
        """
        String sequenceCurrent = latestSnapshotId("sequence_timeline")
        sql """refresh table sequence_timeline"""

        spark_paimon_multi """
            DROP TABLE IF EXISTS paimon.${dbName}.aggregation_timeline;
            CREATE TABLE paimon.${dbName}.aggregation_timeline (
                id INT, original_total BIGINT
            ) USING paimon
            TBLPROPERTIES (
                'primary-key'='id',
                'bucket'='1',
                'file.format'='parquet',
                'merge-engine'='aggregation',
                'fields.original_total.aggregate-function'='sum'
            );
            INSERT INTO paimon.${dbName}.aggregation_timeline VALUES (1, 10), (2, 20);
        """
        String aggregationBase = latestSnapshotId("aggregation_timeline")
        spark_paimon_multi """
            CALL paimon.sys.create_tag(
                table => '${dbName}.aggregation_timeline', tag => 'aggregation_base'
            );
            ALTER TABLE paimon.${dbName}.aggregation_timeline
                RENAME COLUMN original_total TO intermediate_total;
        """
        sql """refresh catalog ${catalogName}"""
        assertEquals(aggregationBase, latestSnapshotId("aggregation_timeline"))
        [false, true].each { forceJni ->
            sql """set force_jni_scanner=${forceJni}"""
            "order_qt_aggregation_schema_only_${forceJni}" """
                SELECT id, intermediate_total FROM aggregation_timeline ORDER BY id
            """
        }
        spark_paimon """
            INSERT INTO paimon.${dbName}.aggregation_timeline VALUES (1, 5), (2, 3)
        """
        String aggregationIntermediate = latestSnapshotId("aggregation_timeline")
        spark_paimon_multi """
            ALTER TABLE paimon.${dbName}.aggregation_timeline
                RENAME COLUMN intermediate_total TO current_total;
            INSERT INTO paimon.${dbName}.aggregation_timeline VALUES (1, 7), (2, 4);
        """
        sql """refresh catalog ${catalogName}"""

        spark_paimon_multi """
            DROP TABLE IF EXISTS paimon.${dbName}.scan_state_history;
            CREATE TABLE paimon.${dbName}.scan_state_history (
                id INT, value BIGINT
            ) USING paimon
            TBLPROPERTIES (
                'primary-key'='id', 'bucket'='1', 'file.format'='parquet',
                'scan.mode'='latest-full'
            );
            INSERT INTO paimon.${dbName}.scan_state_history VALUES (1, 10);
        """
        String scanStateSnapshot = latestSnapshotId("scan_state_history")
        spark_paimon """
            ALTER TABLE paimon.${dbName}.scan_state_history UNSET TBLPROPERTIES ('scan.mode')
        """
        sql """refresh catalog ${catalogName}"""

        // Historical fields must use their historical sequence/aggregation options;
        // latest options reference names that do not exist in the old schema.
        [false, true].each { forceJni ->
            sql """set force_jni_scanner=${forceJni}"""
            "order_qt_sequence_latest_${forceJni}" """
                SELECT id, current_sequence, payload FROM sequence_timeline ORDER BY id
            """
            "order_qt_sequence_old_snapshot_${forceJni}" """
                SELECT id, old_sequence, payload
                FROM sequence_timeline FOR VERSION AS OF ${sequenceBase} ORDER BY id
            """
            "order_qt_sequence_new_snapshot_${forceJni}" """
                SELECT id, current_sequence, payload
                FROM sequence_timeline FOR VERSION AS OF ${sequenceCurrent} ORDER BY id
            """
            "order_qt_sequence_options_${forceJni}" """
                SELECT id, old_sequence, payload
                FROM sequence_timeline@options('scan.snapshot-id'='${sequenceBase}')
                ORDER BY id
            """
            "order_qt_sequence_tag_${forceJni}" """
                SELECT id, old_sequence, payload
                FROM sequence_timeline FOR VERSION AS OF 'sequence_base' ORDER BY id
            """
            "order_qt_aggregation_latest_${forceJni}" """
                SELECT id, current_total FROM aggregation_timeline ORDER BY id
            """
            "order_qt_aggregation_old_snapshot_${forceJni}" """
                SELECT id, original_total
                FROM aggregation_timeline FOR VERSION AS OF ${aggregationBase} ORDER BY id
            """
            "order_qt_aggregation_intermediate_snapshot_${forceJni}" """
                SELECT id, intermediate_total
                FROM aggregation_timeline FOR VERSION AS OF ${aggregationIntermediate} ORDER BY id
            """
            "order_qt_aggregation_options_${forceJni}" """
                SELECT id, original_total
                FROM aggregation_timeline@options('scan.snapshot-id'='${aggregationBase}')
                ORDER BY id
            """
            "order_qt_aggregation_tag_${forceJni}" """
                SELECT id, original_total
                FROM aggregation_timeline@tag(aggregation_base) ORDER BY id
            """
            // Historical schema restoration must not revive a removed startup mode.
            "order_qt_removed_scan_mode_${forceJni}" """
                SELECT id, value FROM scan_state_history
                FOR VERSION AS OF ${scanStateSnapshot} ORDER BY id
            """
        }
    } finally {
        sql """set force_jni_scanner=false"""
    }
}
