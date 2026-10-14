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

import java.nio.file.Files
import java.nio.file.Paths

// Checklist: G08 G09 G11 G13 H08.
suite("test_uuid_parquet_roundtrip") {
    if (!getFeConfig("enable_outfile_to_local").equalsIgnoreCase("true")) {
        logger.warn("Please set enable_outfile_to_local to true to run test_uuid_parquet_roundtrip")
        return
    }
    def backend = sql_return_maparray("SHOW BACKENDS").find { it.Alive == "true" }
    String securePath = sql_return_maparray(
            "SHOW BACKEND CONFIG LIKE 'user_files_secure_path' FROM ${backend.BackendId}")[0].Value
    def outputDirectory = Files.createTempDirectory(Paths.get(securePath), "test_uuid_parquet_roundtrip_")
    try {
        sql "DROP TABLE IF EXISTS uuid_file_parquet"
        sql """CREATE TABLE uuid_file_parquet (id INT, u UUID, a ARRAY<UUID>, s STRUCT<k:UUID>)
               DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES("replication_num"="1")"""
        sql """INSERT INTO uuid_file_parquet VALUES
               (1, '00112233445566778899AABBCCDDEEFF', ['00112233445566778899AABBCCDDEEFF', NULL],
                   {'80000000-0000-0000-0000-000000000000'}),
               (2, 'ffffffff-ffff-ffff-ffff-ffffffffffff', [], {NULL}),
               (3, NULL, NULL, NULL)"""
        qt_source "SELECT * FROM uuid_file_parquet ORDER BY id"
        sql """SELECT * FROM uuid_file_parquet ORDER BY id
               INTO OUTFILE "file://${outputDirectory}/parquet_" FORMAT AS parquet"""
        String path = "${outputDirectory.fileName}/parquet_*"
        sql "DROP TABLE IF EXISTS uuid_file_reload_parquet"
        sql "CREATE TABLE uuid_file_reload_parquet LIKE uuid_file_parquet"
        sql """INSERT INTO uuid_file_reload_parquet
               SELECT * FROM local("file_path"="${path}", "format"="parquet",
                                   "backend_id"="${backend.BackendId}")"""
        qt_roundtrip "SELECT * FROM uuid_file_reload_parquet ORDER BY id"
        order_qt_inferred_types """DESC FUNCTION local(
            "file_path"="${path}", "format"="parquet", "backend_id"="${backend.BackendId}")"""
    } finally {
        assertTrue(outputDirectory.toFile().deleteDir())
    }
}
