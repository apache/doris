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

import java.nio.file.Files

suite("test_map_export", "export") {
    // define the table
    def testTable = "tbl_test_map_export"

    sql "DROP TABLE IF EXISTS ${testTable}"


    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            id INT,
            m Map<STRING, INT>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // make data
    sql """ INSERT INTO ${testTable} VALUES (1, NULL); """
    sql """ INSERT INTO ${testTable} VALUES (2, {}); """
    sql """ INSERT INTO ${testTable} VALUES (3, {"  33,amory  ":2, " bet ":20, " cler ": 26}); """
    sql """ INSERT INTO ${testTable} VALUES (4, {"k3":23, null: 20, "k4": null}); """
    sql """ INSERT INTO ${testTable} VALUES (5, {null:null}); """

    // check result
    qt_select """ SELECT * FROM ${testTable} ORDER BY id; """
    qt_select_count """SELECT COUNT(m) FROM ${testTable}"""

    def storage = new ExportTestHelper(delegate)
    String prefix = "regression/test_map_export/${UUID.randomUUID()}/"
    File localDataDir = Files.createTempDirectory("test_map_export-").toFile()

    try {
        sql """SELECT * FROM ${testTable} ORDER BY id
               INTO OUTFILE "${storage.uri(prefix)}" FORMAT AS CSV
               PROPERTIES (${storage.storageProperties()})"""
        List<File> files = storage.downloadFiles(prefix, localDataDir)
        assertEquals(1, files.size())
        List<String> exported = files[0].readLines("UTF-8")
        List<String> expected = new File(context.config.dataPath, "export/test_map_export.out").readLines("UTF-8")
                .drop(2).takeWhile { !it.isEmpty() }
        assertEquals(expected, exported)
    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
        localDataDir.deleteDir()
        storage.deleteFiles(prefix)
    }
}
