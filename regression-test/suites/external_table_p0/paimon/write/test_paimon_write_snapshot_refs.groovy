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

suite("test_paimon_write_snapshot_refs", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_snapshot_refs_catalog"
    String dbName = "test_pw_snapshot_refs_db"
    String tableName = "t_refs"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};
        DROP TABLE IF EXISTS paimon.${dbName}.${tableName};
        CREATE TABLE paimon.${dbName}.${tableName} (
            id INT,
            payload STRING,
            amount DECIMAL(18, 2),
            event_time TIMESTAMP_NTZ
        ) USING paimon
        TBLPROPERTIES (
            'bucket' = '-1',
            'write-only' = 'true',
            'file.format' = 'parquet'
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
            's3.path.style.access' = 'true',
            'meta.cache.paimon.table.ttl-second' = '0'
        )
    """
    sql """switch ${catalogName}"""
    sql """use ${dbName}"""

    try {
        // Doris creates the snapshot which becomes the immutable tag and branch base.
        sql """
            INSERT INTO ${tableName} VALUES
                (1, 'base', 10.25, '2026-07-01 10:11:12.123456')
        """
        long baselineSnapshot = (sql """
            SELECT MAX(snapshot_id) FROM ${tableName}\$snapshots
        """)[0][0] as long

        spark_paimon """REFRESH TABLE paimon.${dbName}.${tableName}"""
        spark_paimon_multi """
            CALL paimon.sys.create_tag(
                table => '${dbName}.${tableName}',
                tag => 'baseline_tag',
                snapshot => ${baselineSnapshot}
            );
            CALL paimon.sys.create_branch(
                '${dbName}.${tableName}',
                'audit_branch',
                'baseline_tag'
            );
        """

        sql """
            INSERT INTO ${tableName} VALUES
                (2, 'latest', 20.50, '2026-07-02 10:11:12.654321')
        """
        sql """refresh table ${tableName}"""

        def baseline = [[1, "base", "10.25", "2026-07-01 10:11:12.123456"]]
        assertEquals(baseline, sql("""
            SELECT id, payload, CAST(amount AS STRING),
                   DATE_FORMAT(event_time, '%Y-%m-%d %H:%i:%s.%f')
            FROM ${tableName} FOR VERSION AS OF ${baselineSnapshot}
            ORDER BY id
        """))
        assertEquals(baseline, sql("""
            SELECT id, payload, CAST(amount AS STRING),
                   DATE_FORMAT(event_time, '%Y-%m-%d %H:%i:%s.%f')
            FROM ${tableName}@tag(baseline_tag)
            ORDER BY id
        """))
        assertEquals(baseline, sql("""
            SELECT id, payload, CAST(amount AS STRING),
                   DATE_FORMAT(event_time, '%Y-%m-%d %H:%i:%s.%f')
            FROM ${tableName}@branch(audit_branch)
            ORDER BY id
        """))

        // A historical source relation must keep its own schema/snapshot while
        // the sink is rebound to the latest writable table generation.
        sql """
            INSERT INTO ${tableName}
            SELECT id + 100, concat(payload, '-snapshot-copy'), amount + 1, event_time
            FROM ${tableName} FOR VERSION AS OF ${baselineSnapshot}
        """
        sql """refresh table ${tableName}"""
        assertEquals([[1, "base"], [2, "latest"], [101, "base-snapshot-copy"]], sql("""
            SELECT id, payload FROM ${tableName} ORDER BY id
        """))

        // Branches are valid sources even though Doris does not currently expose
        // a Paimon branch sink.
        sql """
            INSERT INTO ${tableName}
            SELECT id + 200, concat(payload, '-branch-copy'), amount + 2, event_time
            FROM ${tableName}@branch(audit_branch)
        """
        sql """refresh table ${tableName}"""
        assertEquals([[1, "base"], [2, "latest"], [101, "base-snapshot-copy"],
                [201, "base-branch-copy"]], sql("""
            SELECT id, payload FROM ${tableName} ORDER BY id
        """))

        // Keep this unsupported boundary explicit. A rejected branch sink must
        // not fall back to the main branch or mutate the referenced branch.
        test {
            sql """
                INSERT INTO ${tableName}@branch(audit_branch)
                VALUES (9, 'branch-write', 9.00, '2026-07-09 00:00:00')
            """
            exception "Only support insert data into iceberg table's branch"
        }
        test {
            sql """
                INSERT OVERWRITE TABLE ${tableName}@branch(audit_branch)
                VALUES (9, 'branch-overwrite', 9.00, '2026-07-09 00:00:00')
            """
            exception "Only support insert overwrite into iceberg table's branch"
        }

        assertEquals(4L, (sql """SELECT COUNT(*) FROM ${tableName}""")[0][0] as long)
        assertEquals(baseline, sql("""
            SELECT id, payload, CAST(amount AS STRING),
                   DATE_FORMAT(event_time, '%Y-%m-%d %H:%i:%s.%f')
            FROM ${tableName}@tag(baseline_tag)
            ORDER BY id
        """))
        assertEquals(baseline, sql("""
            SELECT id, payload, CAST(amount AS STRING),
                   DATE_FORMAT(event_time, '%Y-%m-%d %H:%i:%s.%f')
            FROM ${tableName}@branch(audit_branch)
            ORDER BY id
        """))
        assertEquals(baseline, sql("""
            SELECT id, payload, CAST(amount AS STRING),
                   DATE_FORMAT(event_time, '%Y-%m-%d %H:%i:%s.%f')
            FROM ${tableName} FOR VERSION AS OF ${baselineSnapshot}
            ORDER BY id
        """))

        spark_paimon """REFRESH TABLE paimon.${dbName}.${tableName}"""
        def sparkRows = spark_paimon """
            SELECT id, payload, CAST(amount AS STRING),
                   DATE_FORMAT(event_time, 'yyyy-MM-dd HH:mm:ss.SSSSSS')
            FROM paimon.${dbName}.${tableName}
            ORDER BY id
        """
        def dorisRows = sql """
            SELECT id, payload, CAST(amount AS STRING),
                   DATE_FORMAT(event_time, '%Y-%m-%d %H:%i:%s.%f')
            FROM ${tableName}
            ORDER BY id
        """
        assertSparkDorisResultEquals(sparkRows, dorisRows)
    } finally {
        sql """drop catalog if exists ${catalogName}"""
    }
}
