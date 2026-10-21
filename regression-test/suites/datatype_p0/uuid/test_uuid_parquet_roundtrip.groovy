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

import java.nio.file.Paths

// Checklist: G08 G09 G11 G13 H08.
suite("test_uuid_parquet_roundtrip") {
    if (!getFeConfig("enable_outfile_to_local").equalsIgnoreCase("true")) {
        logger.warn("Please set enable_outfile_to_local to true to run test_uuid_parquet_roundtrip")
        return
    }
    def backends = sql_return_maparray("SHOW BACKENDS").findAll { it.Alive == "true" }
    assertFalse(backends.isEmpty())
    def backend = backends[0]
    String securePath = sql_return_maparray(
            "SHOW BACKEND CONFIG LIKE 'user_files_secure_path' FROM ${backend.BackendId}")[0].Value
    def outputDirectory = Paths.get(securePath, "test_uuid_parquet_roundtrip_${UUID.randomUUID()}")
    try {
        // OUTFILE runs on a BE, which may be remote from the regression runner.
        mkdirRemotePathOnAllBE("root", outputDirectory.toString())
        sql "SET enable_parallel_outfile = false"
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
        def outfile = sql """SELECT * FROM uuid_file_parquet ORDER BY id
               INTO OUTFILE "file://${outputDirectory}/parquet_" FORMAT AS parquet"""
        // Use the BE that actually wrote the file for local TVF reads.
        backend = backends.find { outfile[0][3].startsWith("file:///${it.Host}/") }
        assertNotNull(backend)
        String path = "${outputDirectory.fileName}/parquet_*"
        sql "DROP TABLE IF EXISTS uuid_file_reload_parquet"
        sql "CREATE TABLE uuid_file_reload_parquet LIKE uuid_file_parquet"
        sql """INSERT INTO uuid_file_reload_parquet
               SELECT * FROM local("file_path"="${path}", "format"="parquet",
                                   "backend_id"="${backend.BackendId}")"""
        qt_roundtrip "SELECT * FROM uuid_file_reload_parquet ORDER BY id"
        // The inferred schema must preserve the UUID annotations written by OUTFILE.
        order_qt_inferred_types """DESC FUNCTION local(
            "file_path"="${path}", "format"="parquet", "backend_id"="${backend.BackendId}")"""
    } finally {
        deleteRemotePathOnAllBE("root", outputDirectory.toString())
    }
}
