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

suite("test_struct_export", "export") {
    // define the table
    def testTable = "tbl_test_struct_export"

    sql "DROP TABLE IF EXISTS ${testTable}"


    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
                      `k1` INT(11) NULL,
                      `k2` STRUCT<f1:BOOLEAN,f2:TINYINT,f3:SMALLINT,f4:INT,f5:INT,f6:BIGINT,f7:LARGEINT> NULL,
                      `k3` STRUCT<f1:FLOAT,f2:DOUBLE,f3:DECIMAL(3,3)> NULL,
                      `k4` STRUCT<f1:DATE,f2:DATETIME,f3:DATEV2,f4:DATETIMEV2> NULL,
                      `k5` STRUCT<f1:CHAR(10),f2:VARCHAR(10),f3:STRING> NOT NULL
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // make data
    sql "INSERT INTO ${testTable} VALUES(1, {1,11,111,1111,11111,11111,111111},null,null,{'','',''})"
    sql "INSERT INTO ${testTable} VALUES(2, {null,null,null,null,null,null,null},{2.1,2.22,0.333},null,{null,null,null})"
    sql "INSERT INTO ${testTable} VALUES(3, null,{null,null,null},{'2023-02-23','2023-02-23 00:10:19','2023-02-23','2023-02-23 00:10:19'},{'','',''})"
    sql "INSERT INTO ${testTable} VALUES(4, null,null,{null,null,null,null},{'abc','def','hij'})"

    // check result
    qt_select """ SELECT * FROM ${testTable} ORDER BY k1; """
    qt_select_count """SELECT COUNT(k2), COUNT(k4) FROM ${testTable}"""

    def storage = new ExportTestHelper(delegate)
    String prefix = "regression/test_struct_export/${UUID.randomUUID()}/"
    File localDataDir = Files.createTempDirectory("test_struct_export-").toFile()

    try {
        sql """SELECT * FROM ${testTable} ORDER BY k1
               INTO OUTFILE "${storage.uri(prefix)}" FORMAT AS CSV
               PROPERTIES (${storage.storageProperties()})"""
        List<File> files = storage.downloadFiles(prefix, localDataDir)
        assertEquals(1, files.size())
        List<String> exported = files[0].readLines("UTF-8")
        List<String> expected = new File(context.config.dataPath, "export/test_struct_export.out").readLines("UTF-8")
                .drop(2).takeWhile { !it.isEmpty() }
        // The CSV struct writer puts a space after each field name.
        assertEquals(expected, exported.collect { it.replace('": ', '":') })
    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
        localDataDir.deleteDir()
        storage.deleteFiles(prefix)
    }
}
