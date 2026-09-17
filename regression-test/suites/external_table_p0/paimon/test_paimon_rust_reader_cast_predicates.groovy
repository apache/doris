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

// CASTed operands through the paimon rust reader. The rust predicate converter
// must NOT strip a cast from a column before pushing: for a DECIMAL(10,2)
// column holding 1.24, `CAST(amount AS DECIMAL(10,1)) = 1.2` must retain the
// row, while the unwrapped `amount = 1.2` prunes it — and rows pruned by the
// pushed filter cannot be recovered by the Doris residual. The FE converter
// already rejects casted operands (PaimonPredicateConverterTest's
// rejectDecimalScaleCast / rejectStringToIntegerCast), so JNI never pushes
// them; the rust reader must match. Cast-induced NULL semantics
// (`CAST(s AS INT) IS NULL`) are covered the same way.
//
// Both differential legs run with force_jni_scanner=true: these parquet
// append tables convert to raw native splits, which getSplits() would
// otherwise prefer — both legs would silently use the native reader and
// never reach the JNI / rust converters. The actual reader path is verified
// per leg through the query profile (the rust reader's PaimonRustReader
// timer group).
suite("test_paimon_rust_reader_cast_predicates", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }

    String catalogName = "test_paimon_rust_cast"
    String dbName = "test_paimon_rust_cast_db"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    // Table is created via Spark because Doris does not support Paimon DDL.
    // t_decimal_cast mirrors the FE rejectDecimalScaleCast shape: a
    // DECIMAL(10,2) column whose scale-reducing cast must not be pushed.
    // t_cast_null covers cast-related NULL semantics on a string column.
    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};
        DROP TABLE IF EXISTS paimon.${dbName}.t_decimal_cast;
        CREATE TABLE paimon.${dbName}.t_decimal_cast (
            id INT, amount DECIMAL(10, 2)
        ) USING paimon;
        INSERT INTO paimon.${dbName}.t_decimal_cast VALUES
            (1, 1.24), (2, 1.20), (3, 1.30), (4, NULL);

        DROP TABLE IF EXISTS paimon.${dbName}.t_cast_null;
        CREATE TABLE paimon.${dbName}.t_cast_null (
            id INT, s VARCHAR(10)
        ) USING paimon;
        INSERT INTO paimon.${dbName}.t_cast_null VALUES (1, '5'), (2, NULL);
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

    try {
        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql """set enable_file_scanner_v2=true"""
        // These are parquet append tables whose DataSplits convert to raw
        // native splits; force the logical (JNI / rust) reader path so the
        // differential actually exercises both converters.
        sql """set force_jni_scanner=true"""
        sql """set enable_profile=true"""

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

        def testQueries = [
                // Scale-reducing cast: 1.24 rounds to 1.2 under the cast, so
                // the 1.24 row (id 1) must be retained — exactly the row a
                // wrongly pushed `amount = 1.2` would drop forever.
                """select id from t_decimal_cast where cast(amount as decimal(10,1)) = 1.2 order by id""",
                """select id from t_decimal_cast where cast(amount as decimal(10,1)) = 1.3 order by id""",
                // Plain (uncast) decimal equality still pushes and matches.
                """select id from t_decimal_cast where amount = 1.24 order by id""",
                // CAST IS NULL / IS NOT NULL on the casted column keep NULL
                // semantics: only the NULL row (id 4) is null under the cast.
                """select id from t_decimal_cast where cast(amount as decimal(10,1)) is null order by id""",
                """select id from t_decimal_cast where cast(amount as decimal(10,1)) is not null order by id""",
                // Cast-induced NULL: CAST('5' AS INT) is 5, not NULL.
                """select id from t_cast_null where cast(s as int) is null order by id""",
                """select id from t_cast_null where cast(s as int) = 5 order by id"""
        ]
        def expectedResults = [
                [[1], [2]],
                [[3]],
                [[1]],
                [[4]],
                [[1], [2], [3]],
                [[2]],
                [[1]]
        ]
        // Representative scale-cast query reused for the reader-path checks.
        String scaleCastQuery = testQueries[0]

        sql """set enable_paimon_rust_reader=false"""
        def jniResults = testQueries.collect { query -> sql(query) }
        // The JNI leg must ride the logical-split JNI reader: the profile of
        // the representative query must not contain the rust reader's timer.
        def jniProfile = profileTextOf(scaleCastQuery)
        assertFalse(jniProfile.contains("PaimonRustReader"), "JNI leg must not use the rust reader")

        sql """set enable_paimon_rust_reader=true"""
        def rustResults = testQueries.collect { query -> sql(query) }
        // The rust leg must actually run the rust reader: its profile carries
        // the PaimonRustReader timer group, which only the rust reader creates.
        def rustProfile = profileTextOf(scaleCastQuery)
        assertTrue(rustProfile.contains("PaimonRustReader"),
                "rust leg must use the rust reader (profile timer missing)")

        for (int i = 0; i < testQueries.size(); i++) {
            // The rust reader must agree with the JNI reader (which, like FE,
            // never pushes a casted operand) on every form.
            assertEquals(jniResults[i].toString(), rustResults[i].toString())
            // And both must be right, not just mutually consistent: id 1 in
            // the first query is exactly the row a wrongly pushed
            // `amount = 1.2` would drop.
            assertEquals(expectedResults[i].toString(), rustResults[i].toString())
        }
    } finally {
        sql """set enable_paimon_rust_reader=false"""
        sql """set force_jni_scanner=${originalForceJni}"""
        sql """set enable_profile=${originalEnableProfile}"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
