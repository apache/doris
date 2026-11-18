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

import org.apache.doris.regression.action.ProfileAction

// These are legal physical column names on an ordinary table. Vector score aliases must
// only apply to a vector-search split, including when Spark preserves mixed-case names.
suite("test_paimon_rust_reader_vector_column_names", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.test_paimon_vector_names_db;
        DROP TABLE IF EXISTS paimon.test_paimon_vector_names_db.vector_column_names;
        CREATE TABLE paimon.test_paimon_vector_names_db.vector_column_names (
            id INT,
            __PAIMON_SEARCH_SCORE FLOAT,
            __Paimon_Search_Score_To_Dis FLOAT,
            _Distance FLOAT
        ) USING paimon TBLPROPERTIES ('file.format'='parquet');
        INSERT INTO paimon.test_paimon_vector_names_db.vector_column_names VALUES
            (1, 11.0, 21.0, 31.0), (2, NULL, 22.0, NULL), (3, 13.0, NULL, 33.0);
        DROP TABLE IF EXISTS paimon.test_paimon_vector_names_db.vector_column_predicate;
        CREATE TABLE paimon.test_paimon_vector_names_db.vector_column_predicate (
            id INT, __paimon_search_score_to_dis INT
        ) USING paimon TBLPROPERTIES ('file.format'='parquet');
        INSERT INTO paimon.test_paimon_vector_names_db.vector_column_predicate
            SELECT CAST(id AS INT), CAST(id % 128 AS INT)
            FROM (SELECT explode(sequence(1, 4096)) AS id);
    """
    sql "DROP CATALOG IF EXISTS test_paimon_vector_names"
    sql """
        CREATE CATALOG test_paimon_vector_names PROPERTIES (
            'type'='paimon',
            'paimon.catalog.type'='filesystem',
            'warehouse'='s3://warehouse/wh',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.region'='us-east-1',
            'use_path_style'='true'
        )
    """
    def originalForceJni = sql("select @@force_jni_scanner")[0][0]
    def originalRust = sql("select @@enable_paimon_rust_reader")[0][0]
    def originalV2 = sql("select @@enable_file_scanner_v2")[0][0]
    def originalProfile = sql("select @@enable_profile")[0][0]
    try {
        sql "SWITCH test_paimon_vector_names"
        sql "USE test_paimon_vector_names_db"
        sql "set enable_file_scanner_v2=true"
        sql "set enable_profile=true"
        def profileAction = new ProfileAction(context)
        def query = """select id, __paimon_search_score, __paimon_search_score_to_dis,
                _distance from vector_column_names order by id"""

        sql "set force_jni_scanner=true"
        sql "set enable_paimon_rust_reader=false"
        qt_jni_physical_names query
        def jniQueryId = sql("select last_query_id()")[0][0]
        def jniProfile = profileAction.getProfile(jniQueryId.toString(),
                ["FileScannerV2", "Profile Completion State: COMPLETE"])
        assertFalse(jniProfile.contains("PaimonRustReader"), jniProfile)
        def jniRows = sql(query)

        sql "set force_jni_scanner=false"
        sql "set enable_paimon_rust_reader=true"
        qt_rust_physical_names query
        def rustQueryId = sql("select last_query_id()")[0][0]
        profileAction.getProfile(rustQueryId.toString(),
                ["FileScannerV2", "PaimonRustReader", "Profile Completion State: COMPLETE"])
        assertEquals(jniRows.toString(), sql(query).toString(),
                "ordinary Rust scans must preserve physical score/distance-named columns")
        qt_rust_score_only """select __paimon_search_score
                from vector_column_names order by id"""
        qt_rust_reordered """select _distance, id, __paimon_search_score_to_dis,
                __paimon_search_score from vector_column_names order by id"""
        qt_rust_filter """select id, __paimon_search_score, __paimon_search_score_to_dis
                from vector_column_names where __paimon_search_score >= 12
                    or __paimon_search_score_to_dis is null order by id"""
        // A legal lowercase physical name must retain native row filtering as well as values.
        def selective = """select count(*), min(id), max(id), sum(id)
                from vector_column_predicate where __paimon_search_score_to_dis = 42"""
        sql selective
        def predicateQueryId = sql("select last_query_id()")[0][0]
        def predicateProfile = profileAction.getProfile(predicateQueryId.toString(),
                ["PaimonRustReader", "RustPredicatesConverted:", "RustPredicatesApplied:",
                 "Profile Completion State: COMPLETE"])
        assertTrue((predicateProfile =~ /RustPredicatesConverted:\s*[1-9]/).find(), predicateProfile)
        assertTrue((predicateProfile =~ /RustPredicatesApplied:\s*[1-9]/).find(), predicateProfile)
        qt_lowercase_physical_predicate selective
    } finally {
        sql "set force_jni_scanner=${originalForceJni}"
        sql "set enable_paimon_rust_reader=${originalRust}"
        sql "set enable_file_scanner_v2=${originalV2}"
        sql "set enable_profile=${originalProfile}"
        sql "SWITCH internal"
    }
}
