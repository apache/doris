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
//
// Both differential legs run with force_jni_scanner=true: these parquet
// append tables convert to raw native splits, which getSplits() would
// otherwise prefer — both legs would silently use the native reader and
// never reach the JNI / rust converters. The actual reader path is verified
// per leg through the query profile (the rust reader's PaimonRustReader
// timer group), and the join leg forces an IN runtime filter
// (runtime_filter_type=1 + runtime_filter_wait_infinitely) that must be
// planned onto the probe scan and arrive before the split opens.
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
    // t_frac_ts uses Spark TIMESTAMP_NTZ, which maps to Paimon TIMESTAMP
    // (wall-clock, microsecond precision). Note Spark's plain TIMESTAMP maps
    // to Paimon TIMESTAMP_LTZ, so the NTZ semantics must be spelled out in the
    // DDL. spark.sql.timestampType=TIMESTAMP_NTZ makes the TIMESTAMP '...'
    // literals parse as NTZ civil times too.
    //
    // The predicate literals are millisecond-aligned on purpose: FE truncates
    // plan-time pushed-down timestamp predicates to 3 fractional digits, so a
    // 6-digit literal would reach the readers truncated to milliseconds and
    // the exact residual conjunct would then drop every row it kept — for the
    // JNI, rust and native readers alike. The rust predicate converter's
    // sub-millisecond preservation (paimon_datum int_val2 / nanos) is covered
    // by the PaimonRustPredicateConverterTest unit tests; this suite exercises
    // the full equality and runtime-filter-join pushdown chains with the
    // precision the plan can actually deliver.
    spark_paimon_multi """
        SET spark.sql.timestampType=TIMESTAMP_NTZ;
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};
        DROP TABLE IF EXISTS paimon.${dbName}.t_eq_null;
        CREATE TABLE paimon.${dbName}.t_eq_null (
            a INT, b INT
        ) USING paimon;
        INSERT INTO paimon.${dbName}.t_eq_null VALUES (NULL, NULL), (1, 1), (1, 2);

        DROP TABLE IF EXISTS paimon.${dbName}.t_frac_ts;
        CREATE TABLE paimon.${dbName}.t_frac_ts (
            id INT, ts TIMESTAMP_NTZ
        ) USING paimon;
        INSERT INTO paimon.${dbName}.t_frac_ts VALUES
            (1, TIMESTAMP '2024-01-01 00:00:00.123456'),
            (2, TIMESTAMP '2024-01-01 00:00:00.123000'),
            (3, TIMESTAMP '2024-01-02 00:00:00.999999');

        DROP TABLE IF EXISTS paimon.${dbName}.t_frac_ts_dim;
        CREATE TABLE paimon.${dbName}.t_frac_ts_dim (
            id INT, ts TIMESTAMP_NTZ
        ) USING paimon;
        INSERT INTO paimon.${dbName}.t_frac_ts_dim VALUES
            (1, TIMESTAMP '2024-01-01 00:00:00.123456'),
            (2, TIMESTAMP '2024-01-02 00:00:00.999999');
    """

    // The s3.region property is required: paimon-rust's S3 client rejects a
    // missing region, while the JNI reader falls back to the SDK default.
    sql """drop catalog if exists ${catalogName}"""
    sql """
        CREATE CATALOG ${catalogName} PROPERTIES (
            'type' = 'paimon',
            'paimon.catalog.type' = 'filesystem',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.region' = 'us-east-1',
            'use_path_style' = 'true'
        );
    """

    // Capture the settings this suite overrides so finally can restore them.
    def originalForceJni = sql("select @@force_jni_scanner")[0][0]
    def originalEnableProfile = sql("select @@enable_profile")[0][0]
    def originalRfWait = sql("select @@runtime_filter_wait_infinitely")[0][0]
    def originalRfType = sql("select @@runtime_filter_type")[0][0]

    try {
        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql """set enable_file_scanner_v2=true"""
        // These tables are parquet append tables, whose DataSplits convert to
        // raw native splits; without forcing, getSplits() would hand both legs
        // to the native reader and bypass the JNI / rust converters entirely.
        sql """set force_jni_scanner=true"""
        // Profile capture for the reader-path verification below.
        sql """set enable_profile=true"""
        // The join leg must receive its IN runtime filter before the split
        // opens, so the rust converter sees it in the conjuncts.
        sql """set runtime_filter_wait_infinitely=true"""
        // TRuntimeFilterType.IN == 1: force the IN runtime-filter shape.
        sql """set runtime_filter_type=1"""

        // Runs one query and returns its profile text via the FE REST API
        // (`show query profile "/<id>"` only lists profiles in this version).
        // The profile is finalized asynchronously after the query returns, so
        // retry briefly until the endpoint serves the finished body.
        def profileTextOf = { String query ->
            sql(query)
            def queryId = sql("select last_query_id()")[0][0]
            for (int i = 0; i < 10; i++) {
                def (code, out, err) = curl("GET",
                        "http://${context.config.feHttpAddress}/rest/v1/query_profile/text/${queryId}",
                        null, 30, "root", "")
                if (code == 0 && out.contains("FileScannerV2")) {
                    return out
                }
                Thread.sleep(1000)
            }
            throw new Exception("profile not available for query ${queryId}")
        }

        // The join must generate an IN runtime filter onto the probe ts. The
        // LIMIT on the build side is required for the planner to assign a
        // runtime filter to the paimon scan, and the RF lines only appear in
        // the verbose explain.
        def joinExplain = sql(
                """explain verbose select p.id from t_frac_ts p join
                   (select ts from t_frac_ts_dim limit 10) d on p.ts = d.ts order by p.id""")
                .flatten().join("\n")
        assertTrue(joinExplain.contains("runtime filters") && joinExplain.contains("[in]"),
                "the join must plan an IN runtime filter on the probe scan")

        def testQueries = [
                // Column-to-column: the only form that reaches the BE as
                // EQ_FOR_NULL. Must keep (NULL, NULL) and (1, 1), drop (1, 2).
                """select * from t_eq_null where a <=> b order by a nulls last""",
                // FE-rewritten forms; also exercise the equality / IS NULL
                // pushdown paths of the rust predicate converter.
                """select * from t_eq_null where a <=> 1 order by a""",
                """select * from t_eq_null where a <=> NULL order by a""",
                // Fractional TIMESTAMP(6) equality. The literal is deliberately
                // 3-digit: FE truncates plan-time pushed-down timestamp
                // predicates to milliseconds, so a 6-digit literal would reach
                // the readers truncated and the exact residual would then drop
                // every row — for the JNI, rust and native readers alike. The
                // table data keeps 6-digit values (see the join below), and
                // the rust converter's sub-millisecond handling is covered by
                // the PaimonRustPredicateConverterTest unit tests.
                """select id from t_frac_ts where ts = '2024-01-01 00:00:00.123' order by id""",
                // The join form exercises the timestamp conversion through a
                // runtime-filter IN predicate on the probe scan (t_frac_ts):
                // runtime filters are built at runtime from the build side's
                // actual values, so they bypass the plan-time millisecond
                // truncation and carry the full 6-digit precision through the
                // rust converter — both dim values must match their probe
                // rows. The LIMIT on the build side is what makes the planner
                // assign the runtime filter to the paimon scan (see the
                // explain check above); runtime_filter_wait_infinitely
                // guarantees the filter has arrived before the split opens.
                """select p.id from t_frac_ts p join (select ts from t_frac_ts_dim limit 10) d
                     on p.ts = d.ts order by p.id"""
        ]
        def expectedResults = [
                [[1, 1], [null, null]],
                [[1, 1], [1, 2]],
                [[null, null]],
                [[2]],
                [[1], [3]]
        ]
        // Representative converter query reused for the reader-path checks.
        String pushdownQuery = testQueries[3]

        sql """set enable_paimon_rust_reader=false"""
        def jniResults = testQueries.collect { query -> sql(query) }
        // The JNI leg must ride the logical-split JNI reader: the profile of a
        // representative query must not contain the rust reader's timer.
        def jniProfile = profileTextOf(pushdownQuery)
        assertFalse(jniProfile.contains("PaimonRustReader"), "JNI leg must not use the rust reader")

        sql """set enable_paimon_rust_reader=true"""
        def rustResults = testQueries.collect { query -> sql(query) }
        // The rust leg must actually run the rust reader: its profile carries
        // the PaimonRustReader timer group, which only the rust reader creates.
        def rustProfile = profileTextOf(pushdownQuery)
        assertTrue(rustProfile.contains("PaimonRustReader"),
                "rust leg must use the rust reader (profile timer missing)")
        // Same for the join leg: the IN runtime filter is applied before the
        // split opens and flows through the rust converter.
        def rustJoinProfile = profileTextOf(testQueries[4])
        assertTrue(rustJoinProfile.contains("PaimonRustReader"),
                "rust join leg must use the rust reader (profile timer missing)")

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
        // The parquet preinstalled table is used deliberately: the pinned
        // paimon-rust crate reads ORC TIMESTAMP_LTZ values shifted by the
        // writer's timezone (an upstream crate limitation), so the ORC
        // preinstalled table cannot serve as the JNI/rust differential here.
        sql """set time_zone='+00:00'"""
        sql """set enable_paimon_rust_reader=false"""
        def jniLtzUtc = sql """select * from paimon_test_timestamp_tz.test_ice_timestamp_tz_parquet order by id"""
        sql """set enable_paimon_rust_reader=true"""
        def rustLtzUtc = sql """select * from paimon_test_timestamp_tz.test_ice_timestamp_tz_parquet order by id"""
        assertEquals(jniLtzUtc.toString(), rustLtzUtc.toString())
        assertTrue(rustLtzUtc.toString().contains("2024-12-31T16:00"),
                "rust reader must materialize LTZ in the session timezone")

        sql """set time_zone='+08:00'"""
        sql """set enable_paimon_rust_reader=false"""
        def jniLtzSh = sql """select * from paimon_test_timestamp_tz.test_ice_timestamp_tz_parquet order by id"""
        sql """set enable_paimon_rust_reader=true"""
        def rustLtzSh = sql """select * from paimon_test_timestamp_tz.test_ice_timestamp_tz_parquet order by id"""
        assertEquals(jniLtzSh.toString(), rustLtzSh.toString())
        assertTrue(rustLtzSh.toString().contains("2025-01-01T00:00"))

        // ---- NTZ keeps wall-clock semantics under any session timezone ----
        // t_frac_ts is Spark TIMESTAMP_NTZ -> Paimon TIMESTAMP (wall clock,
        // microsecond precision): the same literal must match under UTC and
        // the write-timezone, and the (millisecond-aligned) pushdown (see
        // above) stays exact.
        for (tz in ['+00:00', '+08:00']) {
            sql """set time_zone='${tz}'"""
            sql """set enable_paimon_rust_reader=false"""
            def jniNtz = sql """select id from t_frac_ts where ts = '2024-01-01 00:00:00.123' order by id"""
            sql """set enable_paimon_rust_reader=true"""
            def rustNtz = sql """select id from t_frac_ts where ts = '2024-01-01 00:00:00.123' order by id"""
            assertEquals(jniNtz.toString(), rustNtz.toString())
            assertEquals("[[2]]", rustNtz.toString())
        }
    } finally {
        // `select @@runtime_filter_type` returns a display string (e.g.
        // "IN_OR_BLOOM_FILTER,MIN_MAX"), not the numeric mask `set` accepts —
        // map it back before restoring.
        def rfTypeMask = { String display ->
            def values = ['IN': 1, 'BLOOM_FILTER': 2, 'MIN_MAX': 4, 'IN_OR_BLOOM_FILTER': 8]
            return display.split(',').collect { values[it.trim()] }.findAll { it }.sum() ?: 0
        }
        sql """set enable_paimon_rust_reader=false"""
        sql """set force_jni_scanner=${originalForceJni}"""
        sql """set enable_profile=${originalEnableProfile}"""
        sql """set runtime_filter_wait_infinitely=${originalRfWait}"""
        sql """set runtime_filter_type=${rfTypeMask(originalRfType)}"""
        sql """unset variable time_zone;"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
