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

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_outfile_separator") {

    def dbName = context.config.getDbNameByFile(context.file)
    def tableName = "outfile_test_separator"
    def helper = new ExportTestHelper(delegate)
    def prefix = "regression/test_outfile_separator/${UUID.randomUUID()}/"
    def downloadDir = new File(System.getProperty("java.io.tmpdir"), "doris-outfile-${UUID.randomUUID()}")
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            k1 int, k2 varchar(100)
            )
            DISTRIBUTED BY HASH(k1) PROPERTIES("replication_num" = "1");
        """
        sql """ INSERT INTO ${tableName} VALUES
            (1, "abc"), (2, "def");
            """
        qt_select_1 """ SELECT * FROM ${tableName} t ORDER BY k1; """

        // check outfile
        sql """
            SELECT * FROM ${tableName} t INTO OUTFILE "${helper.uri(prefix)}"
            PROPERTIES (${helper.storageProperties()}, "column_separator" = "\\\\x01");
        """
        def files = helper.downloadFiles(prefix, downloadDir)
        assert files.size() == 1
        List<String> outLines = Files.readAllLines(Paths.get(files[0].getAbsolutePath()), StandardCharsets.UTF_8);
        assert outLines.size() == 2

        streamLoad {
            db """${dbName}"""
            table 'outfile_test_separator'
            set 'column_separator', '\\x01'
            file files[0].getAbsolutePath()
            time 10000 // limit inflight 10s
        }

        qt_select_2 """ SELECT * FROM ${tableName} t ORDER BY k1; """

    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
        downloadDir.deleteDir()
        helper.deleteFiles(prefix)
    }

}
