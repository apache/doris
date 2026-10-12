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

// External checklist: T04, R02, R09, Q01-Q03, W06, V01-V02.
suite("test_uuid_csv_schema", "p0,external") {
    sql "SET enable_sql_cache = false"
    sql "SET enable_query_cache = false"
    String localPath = context.config.otherConfigs.get('uuidLocalExportPath')
    String token = UUID.randomUUID().toString()
    def sources = [:]
    for (String kind : ['values', 'invalid']) {
        File fixture = new File(context.dataPath, "test_data/uuid_external_${kind}.csv")
        if (localPath != null) {
            File target = new File("${localPath}_${token}_${kind}.csv")
            java.nio.file.Files.copy(fixture.toPath(), target.toPath())
            String path = "${context.config.otherConfigs.get('uuidLocalTvfPath')}_${token}_${kind}.csv"
            sources[kind] = """LOCAL('file_path'='${path}',
                    'backend_id'='${(sql 'SHOW BACKENDS')[0][0]}','format'='csv',
                    'column_separator'=',','csv_schema'='id:int;u:uuid')"""
        } else {
            String bucket = context.config.otherConfigs.get('s3BucketName')
            String key = "uuid_csv_schema/${token}/${kind}.csv"
            getS3Client().putObject(bucket, key, fixture)
            sources[kind] = """S3('uri'='s3://${bucket}/${key}','format'='csv',
                    'column_separator'=',','csv_schema'='id:int;u:uuid',
                    's3.endpoint'='${getS3Endpoint()}','s3.region'='${getS3Region()}',
                    's3.access_key'='${getS3AK()}','s3.secret_key'='${getS3SK()}')"""
        }
    }
    for (boolean scannerV2 : [false, true]) {
        sql "SET enable_file_scanner_v2 = ${scannerV2}"
        String source = sources.values
        "order_qt_v2_${scannerV2}_schema" "DESC FUNCTION ${source}"
        for (boolean strict : [false, true]) {
            sql "SET enable_strict_cast = ${strict}"
            String prefix = "v2_${scannerV2}_strict_${strict}"
            "order_qt_${prefix}_values" "SELECT id,u FROM ${source} ORDER BY id"
            "qt_${prefix}_aggregate" "SELECT COUNT(*),COUNT(u),MIN(u),MAX(u),COUNT(DISTINCT u) FROM ${source}"
            "order_qt_${prefix}_groups" "SELECT u,COUNT(*) FROM ${source} GROUP BY u ORDER BY u"
            "order_qt_${prefix}_predicate" """SELECT id,u FROM ${source}
                    WHERE u >= CAST('80000000000000000000000000000000' AS UUID) OR u IS NULL ORDER BY id"""
            "qt_${prefix}_join" "SELECT COUNT(*) FROM ${source} a JOIN ${source} b ON a.u <=> b.u"
            "order_qt_${prefix}_window" """SELECT id,u,ROW_NUMBER() OVER(ORDER BY u,id),
                    DENSE_RANK() OVER(ORDER BY u) FROM ${source} ORDER BY id"""
            // Like other nullable CSV fields, malformed UUID text is read as NULL.
            "order_qt_${prefix}_invalid" "SELECT * FROM ${sources.invalid} ORDER BY id"
        }
    }
    sql "DROP TABLE IF EXISTS uuid_csv_schema_ctas"
    sql """CREATE TABLE uuid_csv_schema_ctas DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES('replication_num'='1') AS SELECT id,u FROM ${sources.values}"""
    order_qt_ctas_schema "DESC uuid_csv_schema_ctas"
    order_qt_ctas_values "SELECT * FROM uuid_csv_schema_ctas ORDER BY id"
}
