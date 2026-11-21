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

import org.apache.doris.regression.util.ExportTestHelper

// Checklist: G08 G09 G10 G13 H02 H08.
suite("test_uuid_file_maps") {
    def storage = new ExportTestHelper(delegate)
    String prefix = "regression/test_uuid_file_maps/${UUID.randomUUID()}/"
    String root = storage.uri(prefix)
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
                   INTO OUTFILE '${root}${format}_' FORMAT AS ${format} PROPERTIES (${storage.storageProperties()})"""
            String path = "${root}${format}_*"
            sql "DROP TABLE IF EXISTS uuid_file_maps_reload"
            sql "CREATE TABLE uuid_file_maps_reload LIKE uuid_file_maps"
            sql """INSERT INTO uuid_file_maps_reload
                   SELECT * FROM S3('uri'='${path}', 'format'='${format}',
                                       ${storage.storageProperties()})"""
            qt_roundtrip "SELECT * FROM uuid_file_maps_reload ORDER BY id"
            qt_keys_values """SELECT id,MAP_KEYS(m),MAP_VALUES(m),v['u'],v['n'],v['zero']
                             FROM uuid_file_maps_reload ORDER BY id"""
        }
    } finally {
        storage.deleteFiles(prefix)
    }
}
