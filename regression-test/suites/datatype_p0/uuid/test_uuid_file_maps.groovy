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

// Checklist: G08 G09 G10 G13 H02 H08.
suite("test_uuid_file_maps") {
    if (!getFeConfig("enable_outfile_to_local").equalsIgnoreCase("true")) {
        logger.warn("Please set enable_outfile_to_local to true to run test_uuid_file_maps")
        return
    }
    def backend = sql_return_maparray("SHOW BACKENDS").find { it.Alive == "true" }
    String securePath = sql_return_maparray(
            "SHOW BACKEND CONFIG LIKE 'user_files_secure_path' FROM ${backend.BackendId}")[0].Value
    def outputDirectory = Files.createTempDirectory(Paths.get(securePath), "test_uuid_file_maps_")
    try {
        sql "DROP TABLE IF EXISTS uuid_file_maps"
        sql """CREATE TABLE uuid_file_maps (id INT,m MAP<UUID,ARRAY<UUID>>,v MAP<STRING,UUID>)
               DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
        sql """INSERT INTO uuid_file_maps VALUES
               (1, {'00112233445566778899AABBCCDDEEFF':['80000000000000000000000000000000',NULL],
                    'ffffffff-ffff-ffff-ffff-ffffffffffff':[]}, {'u':'00112233445566778899AABBCCDDEEFF','n':NULL}),
               (2, {}, {}), (3, NULL, NULL),
               (4, {'00000000-0000-0000-0000-000000000000':NULL}, {'zero':'00000000000000000000000000000000'})"""
        qt_source "SELECT * FROM uuid_file_maps ORDER BY id"
        for (String format : ['parquet', 'orc']) {
            sql """SELECT * FROM uuid_file_maps ORDER BY id
                   INTO OUTFILE 'file://${outputDirectory}/${format}_' FORMAT AS ${format}"""
            String path = "${outputDirectory.fileName}/${format}_*"
            sql "DROP TABLE IF EXISTS uuid_file_maps_reload"
            sql "CREATE TABLE uuid_file_maps_reload LIKE uuid_file_maps"
            sql """INSERT INTO uuid_file_maps_reload
                   SELECT * FROM local('file_path'='${path}', 'format'='${format}',
                                       'backend_id'='${backend.BackendId}')"""
            qt_roundtrip "SELECT * FROM uuid_file_maps_reload ORDER BY id"
            qt_keys_values """SELECT id,MAP_KEYS(m),MAP_VALUES(m),v['u'],v['n'],v['zero']
                             FROM uuid_file_maps_reload ORDER BY id"""
        }
    } finally {
        assertTrue(outputDirectory.toFile().deleteDir())
    }
}
