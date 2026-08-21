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

suite("test_paimon_write_sequence_group", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_sequence_group_catalog"
    String dbName = "test_pw_sequence_group_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_multi_group;
        CREATE TABLE paimon.${dbName}.t_multi_group (
            id INT,
            profile_name STRING,
            profile_city STRING,
            profile_seq INT,
            total BIGINT,
            peak INT,
            metric_seq INT,
            note STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'merge-engine' = 'partial-update',
            'fields.profile_seq.sequence-group' = 'profile_name,profile_city',
            'fields.metric_seq.sequence-group' = 'total,peak',
            'fields.total.aggregate-function' = 'sum',
            'fields.peak.aggregate-function' = 'max',
            'fields.note.aggregate-function' = 'last_non_null_value'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_remove_on_delete;
        CREATE TABLE paimon.${dbName}.t_remove_on_delete (
            id INT, a STRING, b STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'merge-engine' = 'partial-update',
            'partial-update.remove-record-on-delete' = 'true'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_group_remove_on_delete;
        CREATE TABLE paimon.${dbName}.t_group_remove_on_delete (
            id INT,
            a STRING,
            seq_a INT,
            b STRING,
            seq_b INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'merge-engine' = 'partial-update',
            'fields.seq_a.sequence-group' = 'a',
            'fields.seq_b.sequence-group' = 'b',
            'partial-update.remove-record-on-sequence-group' = 'seq_a'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_property_change;
        CREATE TABLE paimon.${dbName}.t_property_change (
            id INT, a STRING, seq INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1',
            'merge-engine' = 'partial-update'
        );
    """

    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'paimon',
            'paimon.catalog.type' = 'filesystem',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        )
    """
    sql """switch ${catalogName}"""
    sql """use ${dbName}"""

    try {
        def assertSparkEquals = { String tableName, String columns, String orderBy ->
            def sparkRows = spark_paimon """
                SELECT ${columns} FROM paimon.${dbName}.${tableName} ${orderBy}
            """
            def dorisRows = sql """SELECT ${columns} FROM ${tableName} ${orderBy}"""
            assertSparkDorisResultEquals(sparkRows, dorisRows)
        }

        // P01: Each sequence group advances independently. Aggregations only
        // consume a row when the sequence of their own group is accepted.
        sql """INSERT INTO t_multi_group VALUES
            (1, 'alice', 'shanghai', 10, 5, 80, 10, 'base')
        """
        sql """INSERT INTO t_multi_group VALUES
            (1, 'stale-profile', 'beijing', 9, 7, 90, 11, NULL)
        """
        order_qt_sequence_group_stale_profile """
            SELECT * FROM t_multi_group ORDER BY id
        """

        sql """INSERT INTO t_multi_group VALUES
            (1, 'alice-new', 'shenzhen', 12, 100, 99, 10, 'new-note')
        """
        order_qt_sequence_group_new_profile """
            SELECT * FROM t_multi_group ORDER BY id
        """

        // A NULL sequence does not advance its group. A different group in the
        // same row can still advance and apply its aggregate functions.
        sql """INSERT INTO t_multi_group VALUES
            (1, 'null-sequence', 'hangzhou', NULL, 3, 88, 13, NULL)
        """
        order_qt_sequence_group_null_sequence """
            SELECT * FROM t_multi_group ORDER BY id
        """
        assertSparkEquals("t_multi_group", "*", "ORDER BY id")

        // P02: remove-record-on-delete must discard the old partial row. A
        // later partial insert creates a new row and must not revive old fields.
        sql """INSERT INTO t_remove_on_delete VALUES (1, 'old-a', 'old-b')"""
        sql """DELETE FROM t_remove_on_delete WHERE id = 1"""
        qt_remove_on_delete_empty """SELECT count(*) FROM t_remove_on_delete"""
        sql """INSERT INTO t_remove_on_delete (id, b) VALUES (1, 'new-b')"""
        order_qt_remove_on_delete_partial """
            SELECT id, a, b FROM t_remove_on_delete ORDER BY id
        """
        sql """INSERT INTO t_remove_on_delete (id, a) VALUES (1, 'new-a')"""
        order_qt_remove_on_delete_complete """
            SELECT id, a, b FROM t_remove_on_delete ORDER BY id
        """
        assertSparkEquals("t_remove_on_delete", "*", "ORDER BY id")

        // Paimon makes remove-record-on-delete and sequence groups mutually
        // exclusive. Doris must reject a whole-row DELETE on this legal
        // sequence-group configuration without changing its accumulated row.
        sql """INSERT INTO t_group_remove_on_delete VALUES
            (1, 'old-a', 100, 'old-b', 100)
        """
        test {
            sql """DELETE FROM t_group_remove_on_delete WHERE id = 1"""
            exception "partial-update.remove-record-on-delete=true"
        }
        order_qt_group_remove_on_delete_unchanged """
            SELECT * FROM t_group_remove_on_delete ORDER BY id
        """
        sql """INSERT INTO t_group_remove_on_delete VALUES
            (1, 'low-a', 1, 'low-b', 1)
        """
        sql """INSERT INTO t_group_remove_on_delete (id, a, seq_a) VALUES
            (1, 'high-a', 101)
        """
        order_qt_group_remove_on_delete_sequence """
            SELECT * FROM t_group_remove_on_delete ORDER BY id
        """
        assertSparkEquals("t_group_remove_on_delete", "*", "ORDER BY id")

        // P03: Invalid writer properties fail as metadata changes and must not
        // poison the last valid schema. A legal sequence group takes effect for
        // the next writer without recreating the catalog.
        sql """INSERT INTO t_property_change VALUES (1, 'base', 10)"""
        long propertySnapshot = (sql """
            SELECT max(snapshot_id) FROM t_property_change\$snapshots
        """)[0][0] as long
        String invalidPropertyError = null
        try {
            spark_paimon """
                ALTER TABLE paimon.${dbName}.t_property_change SET TBLPROPERTIES (
                    'fields.missing.sequence-group' = 'a')
            """
        } catch (Exception e) {
            invalidPropertyError = e.getMessage()
        }
        assertNotNull(invalidPropertyError)
        assertTrue(invalidPropertyError.toLowerCase().contains("missing"))
        assertEquals(propertySnapshot, (sql """
            SELECT max(snapshot_id) FROM t_property_change\$snapshots
        """)[0][0] as long)
        sql """INSERT INTO t_property_change (id, a) VALUES (1, 'still-writable')"""
        order_qt_property_change_still_writable """
            SELECT * FROM t_property_change ORDER BY id
        """

        /*
         * TODO(PW-ISSUE-01): Re-enable after Doris reloads sequence-group
         * properties for a writer opened after REFRESH CATALOG. See
         * ../KNOWN_WRITE_ISSUES.md.
        spark_paimon """
            ALTER TABLE paimon.${dbName}.t_property_change SET TBLPROPERTIES (
                'fields.seq.sequence-group' = 'a')
        """
        sql """REFRESH CATALOG ${catalogName}"""
        sql """USE ${dbName}"""
        sql """INSERT INTO t_property_change VALUES (1, 'high', 20)"""
        sql """INSERT INTO t_property_change VALUES (1, 'low-must-lose', 15)"""
        order_qt_property_change_sequence_group """
            SELECT * FROM t_property_change ORDER BY id
        """
        assertSparkEquals("t_property_change", "*", "ORDER BY id")
         */
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
