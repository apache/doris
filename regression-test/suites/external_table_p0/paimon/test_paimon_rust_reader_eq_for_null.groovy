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

// NULL-safe equality (`<=>`, EQ_FOR_NULL) through the paimon rust reader.
// The rust predicate converter must NOT push `a <=> b` down as `a IS NULL`:
// with rows (NULL, NULL), (1, 1), (1, 2) that would wrongly drop (1, 1), and
// rows dropped by the pushed filter cannot be recovered by the residual
// conjunct. FE rewrites the literal forms (`a <=> 1` -> `a = 1`,
// `a <=> NULL` -> `a IS NULL`) before they reach the BE, so only the
// column-to-column form exercises EQ_FOR_NULL here; the literal forms still
// guard the rewrite + pushdown chain end to end.
suite("test_paimon_rust_reader_eq_for_null", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }

    String catalogName = "test_paimon_rust_eq_null"
    String dbName = "test_paimon_rust_eq_null_db"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    // Table is created via Spark because Doris does not support Paimon DDL.
    // Both columns stay nullable: `a <=> b` survives FE's NullSafeEqualToEqual
    // rewrite (which only fires when one side is non-nullable / a NULL literal)
    // precisely when both sides are nullable.
    //
    // t_frac_ts carries fractional TIMESTAMP(6) values: the rust predicate
    // converter must preserve sub-millisecond precision when pushing literals
    // (a truncated .123456 -> .123000 push wrongly drops matching rows).
    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};
        DROP TABLE IF EXISTS paimon.${dbName}.t_eq_null;
        CREATE TABLE paimon.${dbName}.t_eq_null (
            a INT, b INT
        ) USING paimon;
        INSERT INTO paimon.${dbName}.t_eq_null VALUES (NULL, NULL), (1, 1), (1, 2);

        DROP TABLE IF EXISTS paimon.${dbName}.t_frac_ts;
        CREATE TABLE paimon.${dbName}.t_frac_ts (
            id INT, ts TIMESTAMP(6)
        ) USING paimon;
        INSERT INTO paimon.${dbName}.t_frac_ts VALUES
            (1, TIMESTAMP '2024-01-01 00:00:00.123456'),
            (2, TIMESTAMP '2024-01-01 00:00:00.123000'),
            (3, TIMESTAMP '2024-01-02 00:00:00.999999');

        DROP TABLE IF EXISTS paimon.${dbName}.t_frac_ts_dim;
        CREATE TABLE paimon.${dbName}.t_frac_ts_dim (
            id INT, ts TIMESTAMP(6)
        ) USING paimon;
        INSERT INTO paimon.${dbName}.t_frac_ts_dim VALUES
            (1, TIMESTAMP '2024-01-01 00:00:00.123456'),
            (2, TIMESTAMP '2024-01-01 00:00:00.999999');
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
        );
    """

    try {
        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql """set enable_file_scanner_v2=true"""

        def testQueries = [
                // Column-to-column: the only form that reaches the BE as
                // EQ_FOR_NULL. Must keep (NULL, NULL) and (1, 1), drop (1, 2).
                """select * from t_eq_null where a <=> b order by a nulls last""",
                // FE-rewritten forms; also exercise the equality / IS NULL
                // pushdown paths of the rust predicate converter.
                """select * from t_eq_null where a <=> 1 order by a""",
                """select * from t_eq_null where a <=> NULL order by a""",
                // Fractional TIMESTAMP(6) literal: the pushed predicate must
                // preserve sub-millisecond precision — a truncated
                // .123456 -> .123000 push wrongly drops the matching row.
                """select id from t_frac_ts where ts = '2024-01-01 00:00:00.123456' order by id""",
                // The join form exercises the same conversion through a
                // runtime-filter IN predicate on the probe scan (t_frac_ts),
                // with the filter available before the split opens.
                """select p.id from t_frac_ts p join t_frac_ts_dim d on p.ts = d.ts order by p.id"""
        ]
        def expectedResults = [
                [[1, 1], [null, null]],
                [[1, 1]],
                [[null, null]],
                [[1]],
                [[1]]
        ]

        sql """set enable_paimon_rust_reader=false"""
        def jniResults = testQueries.collect { query -> sql(query) }

        sql """set enable_paimon_rust_reader=true"""
        def rustResults = testQueries.collect { query -> sql(query) }

        for (int i = 0; i < testQueries.size(); i++) {
            // The rust reader must agree with the JNI reader on every form.
            assertEquals(jniResults[i].toString(), rustResults[i].toString())
            // And both must be right, not just mutually consistent: the
            // (1, 1) row is exactly what a wrongly pushed `a IS NULL` drops.
            assertEquals(expectedResults[i].toString(), rustResults[i].toString())
        }

        // ---- TIMESTAMP_LTZ materializes as session-local civil times ----
        // The rust reader must use the session timezone like the JNI reader,
        // not a fixed default. The preinstalled table was written by Spark
        // with session timezone Asia/Shanghai, so '2025-01-01 00:00:00' wall
        // time is the UTC instant 2024-12-31 16:00:00.
        sql """set time_zone='+00:00'"""
        sql """set enable_paimon_rust_reader=false"""
        def jniLtzUtc = sql """select * from paimon_test_timestamp_tz.test_ice_timestamp_tz_orc order by id"""
        sql """set enable_paimon_rust_reader=true"""
        def rustLtzUtc = sql """select * from paimon_test_timestamp_tz.test_ice_timestamp_tz_orc order by id"""
        assertEquals(jniLtzUtc.toString(), rustLtzUtc.toString())
        assertTrue("rust reader must materialize LTZ in the session timezone",
                rustLtzUtc.toString().contains("2024-12-31 16:00:00"))

        sql """set time_zone='+08:00'"""
        sql """set enable_paimon_rust_reader=false"""
        def jniLtzSh = sql """select * from paimon_test_timestamp_tz.test_ice_timestamp_tz_orc order by id"""
        sql """set enable_paimon_rust_reader=true"""
        def rustLtzSh = sql """select * from paimon_test_timestamp_tz.test_ice_timestamp_tz_orc order by id"""
        assertEquals(jniLtzSh.toString(), rustLtzSh.toString())
        assertTrue(rustLtzSh.toString().contains("2025-01-01 00:00:00"))

        // ---- NTZ keeps wall-clock semantics under any session timezone ----
        // t_frac_ts is a plain TIMESTAMP(6) (wall clock): the same literal must
        // match under UTC and the write-timezone, and the fractional pushdown
        // (see above) stays exact.
        for (tz in ['+00:00', '+08:00']) {
            sql """set time_zone='${tz}'"""
            sql """set enable_paimon_rust_reader=false"""
            def jniNtz = sql """select id from t_frac_ts where ts = '2024-01-01 00:00:00.123456' order by id"""
            sql """set enable_paimon_rust_reader=true"""
            def rustNtz = sql """select id from t_frac_ts where ts = '2024-01-01 00:00:00.123456' order by id"""
            assertEquals(jniNtz.toString(), rustNtz.toString())
            assertEquals("[[1]]", rustNtz.toString())
        }
    } finally {
        sql """set enable_paimon_rust_reader=false"""
        sql """unset variable time_zone;"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
