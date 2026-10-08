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

// Prove safe fallback for UUID bounds and actual pruning on the companion integer column.
suite("test_uuid_parquet_predicate", "p0,external") {
    sql "SET enable_sql_cache = false"
    sql "SET enable_query_cache = false"
    sql "SET enable_condition_cache = false"
    sql "SET enable_profile = true"
    String localPath = context.config.otherConfigs.get('uuidLocalExportPath')
    String token = UUID.randomUUID().toString()
    String source
    File fixture = new File(context.dataPath, "test_data/uuid_predicate.parquet")
    if (localPath != null) {
        File target = new File("${localPath}_${token}_predicate.parquet")
        java.nio.file.Files.copy(fixture.toPath(), target.toPath())
        String path = "${context.config.otherConfigs.get('uuidLocalTvfPath')}_${token}_predicate.parquet"
        source = """LOCAL('file_path'='${path}',
                'backend_id'='${(sql 'SHOW BACKENDS')[0][0]}','format'='parquet')"""
    } else {
        String bucket = context.config.otherConfigs.get('s3BucketName')
        String key = "uuid_parquet_predicate/${token}/predicate.parquet"
        getS3Client().putObject(bucket, key, fixture)
        source = """S3('uri'='s3://${bucket}/${key}','format'='parquet',
                's3.endpoint'='${getS3Endpoint()}','s3.region'='${getS3Region()}',
                's3.access_key'='${getS3AK()}','s3.secret_key'='${getS3SK()}')"""
    }
    for (boolean scannerV2 : [false, true]) {
        sql "SET enable_file_scanner_v2 = ${scannerV2}"
        for (boolean pruning : [false, true]) {
            sql "SET enable_parquet_filter_by_min_max = ${pruning}"
            for (String value : ['00000000-0000-0000-0000-000000000000',
                    '00112233-4455-6677-8899-aabbccddeeff', '80000000-0000-0000-0000-000000000000',
                    'ffffffff-ffff-ffff-ffff-ffffffffffff']) {
                String queryToken = "uuid_bounds_${UUID.randomUUID()}"
                qt_equality "/* ${queryToken} */ SELECT COUNT(*),SUM(id),MIN(u),MAX(u) FROM ${source} WHERE u='${value}'"
                if (pruning) {
                    checkProfileCounters(queryToken, ['RowGroupsReadNum'], ['RowGroupsFilteredByMinMax'])
                }
            }
            qt_range """SELECT COUNT(*),SUM(id) FROM ${source}
                        WHERE u>='00112233-4455-6677-8899-aabbccddeeff' AND u<'ffffffff-ffff-ffff-ffff-ffffffffffff'"""
            qt_null "SELECT COUNT(*),SUM(id) FROM ${source} WHERE u IS NULL"
            String queryToken = "uuid_id_prune_${UUID.randomUUID()}"
            qt_id_prune """/* ${queryToken} */ SELECT COUNT(u),SUM(id),MIN(u),MAX(u) FROM ${source}
                           WHERE id BETWEEN 129 AND 140"""
            if (pruning) {
                checkProfileCounters(queryToken, ['RowGroupsFilteredByMinMax'], [])
            }
        }
    }
}
