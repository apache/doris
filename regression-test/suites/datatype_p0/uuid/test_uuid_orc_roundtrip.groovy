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

// Checklist: G10 G11 G13 H08.
// Parquet/ORC output maps UUID leaves to strings; target UUID columns restore the type.
suite("test_uuid_orc_roundtrip", "p0,external") {

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
        outputPath = "s3://${context.config.otherConfigs.get('s3BucketName')}/uuid_file_orc/${token}_"
        sinkProperties = """
                            "s3.endpoint"="${getS3Endpoint()}", "s3.region"="${getS3Region()}",
                            "s3.access_key"="${getS3AK()}", "s3.secret_key"="${getS3SK()}"
                            """
        sourceProperties = sinkProperties
    }
    sql "DROP TABLE IF EXISTS uuid_file_orc"
    sql """CREATE TABLE uuid_file_orc (id INT, u UUID, a ARRAY<UUID>, s STRUCT<k:UUID>)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO uuid_file_orc VALUES
           (1, '00112233445566778899AABBCCDDEEFF', ['00112233445566778899AABBCCDDEEFF', NULL],
               {'80000000-0000-0000-0000-000000000000'}),
           (2, 'ffffffff-ffff-ffff-ffff-ffffffffffff', [], {NULL}),
           (3, NULL, NULL, NULL)"""
    qt_source "SELECT * FROM uuid_file_orc ORDER BY id"
    String projection = "*"
    String properties = sinkProperties.isEmpty() ? "" : "PROPERTIES(${sinkProperties})"
    sql """SELECT ${projection} FROM uuid_file_orc ORDER BY id
           INTO OUTFILE "${outputPath}orc_" FORMAT AS orc ${properties}"""
    String tvf = localMode ? "local" : "s3"
    String pathKey = localMode ? "file_path" : "uri"
    String path = localMode
            ? "${context.config.otherConfigs.get('uuidLocalTvfPath')}_${token}_orc_*"
            : "${outputPath}orc_*"
    sql "DROP TABLE IF EXISTS uuid_file_reload_orc"
    sql "CREATE TABLE uuid_file_reload_orc LIKE uuid_file_orc"
    String columns = ""
    sql """INSERT INTO uuid_file_reload_orc ${columns}
           SELECT * FROM ${tvf}("${pathKey}"="${path}", "format"="orc", ${sourceProperties})"""
    qt_roundtrip "SELECT ${projection} FROM uuid_file_reload_orc ORDER BY id"

}
