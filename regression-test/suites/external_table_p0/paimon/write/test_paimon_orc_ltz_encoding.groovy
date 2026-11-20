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

suite("test_paimon_orc_ltz_encoding", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_pw_orc_ltz_catalog"
    String dbName = "test_pw_orc_ltz_db"
    String legacyKey = "orc.timestamp-ltz.legacy.type"
    def originalTimeZone = sql "SELECT @@time_zone"
    def originalJniScanner = sql "SELECT @@force_jni_scanner"

    sql "DROP CATALOG IF EXISTS ${catalogName}"
    sql """CREATE CATALOG ${catalogName} PROPERTIES (
        'type' = 'paimon',
        'paimon.catalog.type' = 'filesystem',
        'warehouse' = 's3://warehouse/wh',
        's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
        's3.access_key' = 'admin',
        's3.secret_key' = 'password',
        's3.path.style.access' = 'true'
    )"""
    try {
        sql "SWITCH ${catalogName}"
        sql "DROP DATABASE IF EXISTS ${dbName} FORCE"
        sql "CREATE DATABASE ${dbName}"
        sql "USE ${dbName}"
        sql """CREATE TABLE t_modern (
            id INT, event_time TIMESTAMPTZ(6), local_time DATETIME(6)
        ) ENGINE=paimon PROPERTIES ('file.format' = 'orc')"""

        // Inspect persisted metadata through Spark: a same-JVM legacy round trip can hide the bug.
        def assertEncoding = { String tableName, String expected ->
            assertEquals([[legacyKey, expected]], spark_paimon("""
                SHOW TBLPROPERTIES paimon.${dbName}.${tableName} ('${legacyKey}')
            """))
        }
        // The paimon 1.4 connector materialized unset property defaults into the stored
        // table options, so t_modern (created without the legacy flag) read back as
        // "false". The 2.1 connector no longer persists unset defaults, so the same
        // query fails with "does not have property" — which still means the modern
        // default. Anything else (a persisted "true", or any other error) must fail.
        boolean modernDefault = false
        try {
            assertEncoding("t_modern", "false")
            modernDefault = true
        } catch (Throwable t) {
            modernDefault = t.toString().contains("does not have property")
        }
        assertTrue(modernDefault, "t_modern must keep the modern (non-legacy) encoding default")
        sql """CREATE TABLE t_legacy (event_time TIMESTAMPTZ(6)) ENGINE=paimon
            PROPERTIES ('file.format' = 'orc', '${legacyKey}' = 'true')"""
        sql """CREATE TABLE IF NOT EXISTS t_legacy (event_time TIMESTAMPTZ(6)) ENGINE=paimon"""
        assertEncoding("t_legacy", "true")

        sql "SET time_zone = 'Asia/Shanghai'"
        sql """INSERT INTO t_modern VALUES
            (1, CAST('2024-01-15 10:30:00.123456+08:00' AS TIMESTAMPTZ(6)),
                CAST('2024-03-10 02:30:00.123456' AS DATETIME(6)))"""
        sql "SET time_zone = 'UTC'"
        sql """INSERT INTO t_modern VALUES
            (2, CAST('2024-07-15 02:30:00.654321+00:00' AS TIMESTAMPTZ(6)),
                CAST('2024-07-15 10:30:00.654321' AS DATETIME(6)))"""

        // Epoch microseconds check the stored instants independently of display/session time zones.
        spark_paimon "REFRESH TABLE paimon.${dbName}.t_modern"
        assertSparkDorisResultEquals(spark_paimon("""
            SELECT id, unix_micros(event_time), CAST(local_time AS STRING)
            FROM paimon.${dbName}.t_modern ORDER BY id
        """), [
            [1, 1705285800123456L, "2024-03-10 02:30:00.123456"],
            [2, 1721010600654321L, "2024-07-15 10:30:00.654321"]
        ])
        spark_paimon """
            INSERT INTO paimon.${dbName}.t_modern VALUES
                (3, TIMESTAMP '2023-11-05 08:30:00.123456+00:00',
                    CAST('2023-11-05 01:30:00.123456' AS TIMESTAMP_NTZ)),
                (4, TIMESTAMP '2023-11-05 09:30:00.654321+00:00',
                    CAST('2023-11-05 01:30:00.654321' AS TIMESTAMP_NTZ)),
                (5, NULL, NULL)
        """
        sql "REFRESH TABLE t_modern"
        def expectedUtc = [
            [1, "2024-01-15 02:30:00.123456+00:00", "2024-03-10 02:30:00.123456"],
            [2, "2024-07-15 02:30:00.654321+00:00", "2024-07-15 10:30:00.654321"],
            [3, "2023-11-05 08:30:00.123456+00:00", "2023-11-05 01:30:00.123456"],
            [4, "2023-11-05 09:30:00.654321+00:00", "2023-11-05 01:30:00.654321"],
            [5, null, null]
        ]
        def expectedShanghai = [
            [1, "2024-01-15 10:30:00.123456+08:00", "2024-03-10 02:30:00.123456"],
            [2, "2024-07-15 10:30:00.654321+08:00", "2024-07-15 10:30:00.654321"],
            [3, "2023-11-05 16:30:00.123456+08:00", "2023-11-05 01:30:00.123456"],
            [4, "2023-11-05 17:30:00.654321+08:00", "2023-11-05 01:30:00.654321"],
            [5, null, null]
        ]
        for (boolean jni : [false, true]) {
            sql "SET force_jni_scanner = ${jni}"
            for (String zone : ["UTC", "Asia/Shanghai"]) {
                sql "SET time_zone = '${zone}'"
                assertEquals(zone == "UTC" ? expectedUtc : expectedShanghai, sql("""
                    SELECT id, CAST(event_time AS STRING), CAST(local_time AS STRING)
                    FROM t_modern ORDER BY id
                """))
            }
        }
    } finally {
        sql "SET time_zone = '${originalTimeZone[0][0]}'"
        sql "SET force_jni_scanner = ${originalJniScanner[0][0]}"
        try {
            sql "DROP DATABASE IF EXISTS ${catalogName}.${dbName} FORCE"
        } finally {
            sql "SWITCH internal"
            sql "DROP CATALOG IF EXISTS ${catalogName}"
        }
    }
}
