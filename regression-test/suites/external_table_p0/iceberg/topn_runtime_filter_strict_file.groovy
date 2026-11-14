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

suite("topn_runtime_filter_strict_file", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    sql "DROP CATALOG IF EXISTS topn_strict_file"
    sql """CREATE CATALOG topn_strict_file PROPERTIES (
        'type'='iceberg', 'iceberg.catalog.type'='hadoop',
        'warehouse'='s3a://warehouse/topn_strict_file',
        's3.access_key'='admin', 's3.secret_key'='password',
        's3.endpoint'='http://${externalEnvIp}:${minioPort}', 's3.region'='us-east-1'
    )"""
    sql "CREATE DATABASE IF NOT EXISTS topn_strict_file.topn_strict"
    sql "SET force_sort_algorithm='heap'"
    sql "SET parallel_pipeline_task_num=1"
    sql "SET batch_size=1"
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"

    // FileScanNode is a separate accepted target from OlapScanNode. An S3 TVF would
    // not test it: TopnFilterPushDownVisitor does not accept PhysicalTVFRelation.
    // Use a real external table and generate its files from VALUES, so this suite
    // neither depends on a large preinstalled fixture nor filters one down afterwards.
    for (String format : ["parquet", "orc"]) {
        sql "DROP TABLE IF EXISTS topn_strict_file.topn_strict.${format}_ties"
        sql """CREATE TABLE topn_strict_file.topn_strict.${format}_ties (k INT, s STRING)
            PROPERTIES ('write.format.default'='${format}')"""
        // Exactly three rows: one non-NULL key fills the LIMIT 1 heap, another is a
        // boundary tie, and the final NULL distinguishes NULLS FIRST from NULLS LAST.
        // Two rows could test either equality or NULL ordering, but not both. Placing
        // NULL last also checks that a non-NULL bound must not hide a better NULL key.
        sql """INSERT INTO topn_strict_file.topn_strict.${format}_ties
            VALUES (1,'a'),(1,'a'),(NULL,NULL)"""

        // Cover fixed-width and binary keys in both file readers. Reader-side binary
        // predicates must use the same LT/GT and NULL rules as VTopNPred's column path.
        // The selected column is also the sole ordering key: tied rows are identical,
        // so the entire expected result is deterministic without adding a second key.
        for (String key : ["k", "s"]) {
            for (String direction : ["ASC", "DESC"]) {
                for (String nulls : ["FIRST", "LAST"]) {
                    String tag = "${format}_${key}_${direction}_${nulls}".toLowerCase()
                    String query = """SELECT ${key} FROM topn_strict_file.topn_strict.${format}_ties
                        ORDER BY ${key} ${direction} NULLS ${nulls} LIMIT 1"""
                    sql "SET topn_filter_ratio=0"
                    "qt_${tag}_disabled"(query)
                    sql "SET topn_filter_ratio=1000000"
                    explain {
                        sql query
                        verbose true
                        contains "VPluginDrivenScanNode"
                        contains "algorithm: heap sort"
                        contains "TOPN OPT:"
                    }
                    "qt_${tag}_enabled"(query)
                }
            }
        }
    }
}
