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

// Checklist: G09 G11 G13 H08.
// Parquet/ORC output maps UUID leaves to strings; target UUID columns restore the type.
suite("test_uuid_export", "p0,external") {

    String localPath = context.config.otherConfigs.get("uuidLocalExportPath")
    boolean localMode = localPath != null
    String token = UUID.randomUUID().toString()
    String outputPath
    String sinkProperties
    String sourceProperties
    if (localMode) {
        outputPath = "file://${localPath}_${token}_"
        sinkProperties = ""
        def backends = sql "SHOW BACKENDS"
        sourceProperties = "\"backend_id\"=\"${backends[0][0]}\""
    } else {
        outputPath = "s3://${context.config.otherConfigs.get('s3BucketName')}/uuid_file_export/${token}_"
        sinkProperties = """
                            "s3.endpoint"="${getS3Endpoint()}", "s3.region"="${getS3Region()}",
                            "s3.access_key"="${getS3AK()}", "s3.secret_key"="${getS3SK()}"
                            """
        sourceProperties = sinkProperties
    }
    sql "DROP TABLE IF EXISTS uuid_file_export"
    sql """CREATE TABLE uuid_file_export (id INT, u UUID, a ARRAY<UUID>, s STRUCT<k:UUID>)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO uuid_file_export VALUES
           (1, '00112233445566778899AABBCCDDEEFF', ['00112233445566778899AABBCCDDEEFF', NULL],
               {'80000000-0000-0000-0000-000000000000'}),
           (2, 'ffffffff-ffff-ffff-ffff-ffffffffffff', [], {NULL}),
           (3, NULL, NULL, NULL)"""
    qt_source "SELECT * FROM uuid_file_export ORDER BY id"
    String exportPath = "${outputPath}export/"
    if (localMode) {
        new File("${localPath}_${token}_export").mkdirs()
    }
    String label = "uuid_export_${token.replace('-', '_')}"
    String storageClause = localMode ? "" : "WITH S3 (${sinkProperties})"
    sql """EXPORT TABLE uuid_file_export TO "${exportPath}"
           PROPERTIES("label"="${label}", "format"="parquet")
           ${storageClause}"""
    String state = ""
    for (int attempt = 0; attempt < 60; ++attempt) {
        def jobs = sql "SHOW EXPORT WHERE LABEL='${label}'"
        state = jobs[0][2]
        if (state == "FINISHED") {
            break
        }
        if (state == "CANCELLED") {
            throw new IllegalStateException("UUID EXPORT cancelled: ${jobs[0][10]}")
        }
        sleep(2000)
    }
    assertEquals("FINISHED", state)
    String exportInput = localMode
            ? "${context.config.otherConfigs.get('uuidLocalTvfPath')}_${token}_export/*"
            : "${exportPath}*"
    String exportTvf = localMode ? "local" : "s3"
    String exportKey = localMode ? "file_path" : "uri"
    sql "DROP TABLE IF EXISTS uuid_file_export_reload"
    sql "CREATE TABLE uuid_file_export_reload LIKE uuid_file_export"
    sql """INSERT INTO uuid_file_export_reload
           SELECT * FROM ${exportTvf}("${exportKey}"="${exportInput}", "format"="parquet", ${sourceProperties})"""
    qt_export "SELECT * FROM uuid_file_export_reload ORDER BY id"
}
