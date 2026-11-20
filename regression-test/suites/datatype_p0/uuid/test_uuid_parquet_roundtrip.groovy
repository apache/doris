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

// Checklist: G08 G09 G11 G13 H08.
suite("test_uuid_parquet_roundtrip") {
    def storage = new ExportTestHelper(delegate)
    String prefix = "regression/test_uuid_parquet_roundtrip/${UUID.randomUUID()}/"
    String root = storage.uri(prefix)
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
               INTO OUTFILE "${root}parquet_" FORMAT AS parquet PROPERTIES (${storage.storageProperties()})"""
        String path = "${root}parquet_*"
        sql "DROP TABLE IF EXISTS uuid_file_reload_parquet"
        sql "CREATE TABLE uuid_file_reload_parquet LIKE uuid_file_parquet"
        sql """INSERT INTO uuid_file_reload_parquet
               SELECT * FROM S3("uri"="${path}", "format"="parquet",
                                   ${storage.storageProperties()})"""
        qt_roundtrip "SELECT * FROM uuid_file_reload_parquet ORDER BY id"
        order_qt_inferred_types """DESC FUNCTION S3(
            "uri"="${path}", "format"="parquet", ${storage.storageProperties()})"""
    } finally {
        storage.deleteFiles(prefix)
    }
}
