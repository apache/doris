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

suite("test_paimon_2_1_compatibility", "p0,external,paimon") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enablePaimonTest"))) {
        return
    }
    def endpoint = context.config.otherConfigs.get("externalEnvIp")
    def port = context.config.otherConfigs.get("iceberg_minio_port")
    def settings = ["enable_paimon_rust_reader", "force_jni_scanner",
                    "enable_file_scanner_v2", "enable_profile"]
    def saved = settings.collectEntries { [(it): sql("select @@${it}")[0][0]] }

    sql "DROP CATALOG IF EXISTS paimon_2_1_compatibility"
    sql """CREATE CATALOG paimon_2_1_compatibility PROPERTIES (
        'type'='paimon', 'paimon.catalog.type'='filesystem', 'fs.s3.support'='true',
        'warehouse'='s3://warehouse/wh', 's3.endpoint'='http://${endpoint}:${port}',
        's3.access_key'='admin', 's3.secret_key'='password',
        's3.region'='us-east-1', 'use_path_style'='true')"""
    sql "SWITCH paimon_2_1_compatibility"
    sql "DROP DATABASE IF EXISTS paimon_2_1_compatibility_db FORCE"
    sql "CREATE DATABASE paimon_2_1_compatibility_db"
    sql "USE paimon_2_1_compatibility_db"

    try {
        sql "set enable_file_scanner_v2=true"
        sql "set enable_profile=true"
        sql "set force_jni_scanner=true"
        sql "set enable_paimon_rust_reader=false"

        sql """CREATE TABLE nullable_keys (id INT NULL, v INT) ENGINE=paimon
            PROPERTIES ('primary-key'='id', 'primary-key.nullable'='true',
                'bucket'='1', 'write-only'='true', 'deletion-vectors.enabled'='false')"""
        sql "INSERT INTO nullable_keys VALUES (NULL,10), (1,100)"
        sql "INSERT INTO nullable_keys VALUES (NULL,20), (1,200)"
        order_qt_nullable_keys_jni "SELECT id,v FROM nullable_keys ORDER BY id NULLS FIRST"

        // Keep a single-file positive control: a multi-file PK merge deliberately falls back
        // to JNI. Generate it with Doris so this suite also works with older Spark fixtures.
        sql """CREATE TABLE nullable_keys_single (id INT NULL, v INT) ENGINE=paimon
            PROPERTIES ('primary-key'='id', 'primary-key.nullable'='true',
                'bucket'='1', 'write-only'='true', 'deletion-vectors.enabled'='false')"""
        sql "INSERT INTO nullable_keys_single VALUES (NULL,10), (1,100), (NULL,20), (1,200)"
        order_qt_nullable_single_jni "SELECT id,v FROM nullable_keys_single ORDER BY id NULLS FIRST"
        sql "set force_jni_scanner=false"
        sql "set enable_paimon_rust_reader=true"
        order_qt_nullable_keys_rust "SELECT id,v FROM nullable_keys_single ORDER BY id NULLS FIRST"
        def queryId = sql("select last_query_id()")[0][0].toString()
        def profile = new ProfileAction(context).getProfile(queryId,
                ["PaimonRustReader", "Profile Completion State: COMPLETE"])
        assertTrue(profile.contains("PaimonRustReader"))

        sql "set force_jni_scanner=true"
        sql "set enable_paimon_rust_reader=false"
        sql """CREATE TABLE indexed_values (id INT, v STRING) ENGINE=paimon
            PROPERTIES ('bucket'='-1', 'file-index.bitmap.columns'='id')"""
        sql "INSERT INTO indexed_values VALUES (1,'one'), (2,'two')"
        order_qt_file_indexes """SELECT column_name,index_type,SUM(record_count),MIN(CAST(is_empty AS INT))
            FROM indexed_values\$file_indexes
            GROUP BY column_name,index_type ORDER BY column_name,index_type"""
        test {
            sql "SELECT id FROM indexed_values @options('scan.mode'='latest-delta')"
            exception "latest-delta' is not supported by Doris"
        }

        sql """CREATE TABLE wrapping_sum (id INT NOT NULL, v BIGINT) ENGINE=paimon
            PROPERTIES ('primary-key'='id', 'bucket'='1', 'write-only'='true',
                'deletion-vectors.enabled'='false', 'merge-engine'='aggregation',
                'fields.v.aggregate-function'='sum')"""
        sql "INSERT INTO wrapping_sum VALUES (1,9223372036854775807)"
        sql "INSERT INTO wrapping_sum VALUES (1,1)"
        order_qt_wrapping_sum "SELECT id,v FROM wrapping_sum ORDER BY id"

        sql """CREATE TABLE checked_sum (id INT NOT NULL, v BIGINT) ENGINE=paimon
            PROPERTIES ('primary-key'='id', 'bucket'='1', 'write-only'='true',
                'deletion-vectors.enabled'='false', 'merge-engine'='aggregation',
                'fields.v.aggregate-function'='sum', 'fields.v.sum.fail-on-overflow'='true')"""
        sql "INSERT INTO checked_sum VALUES (1,9223372036854775807)"
        sql "INSERT INTO checked_sum VALUES (1,1)"
        test {
            sql "SELECT id,v FROM checked_sum ORDER BY id"
            exception "overflow"
        }
    } finally {
        saved.each { key, value -> sql "set ${key}=${value}" }
    }
}
