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

suite("test_outfile_datetimev2_scale") {

    def tableName = "test_outfile_datetimev2_scale"
    def helper = new ExportTestHelper(delegate)
    def prefix = "regression/test_outfile_datetimev2_scale/${UUID.randomUUID()}/"
    def downloadDir = new File(System.getProperty("java.io.tmpdir"), "doris-outfile-${UUID.randomUUID()}")

    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                `id` INT,
                `dt0` DATETIMEV2(0),
                `dt3` DATETIMEV2(3),
                `dt6` DATETIMEV2(6)
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES("replication_num" = "1");
        """
        sql """
            INSERT INTO ${tableName} VALUES
            (1, '2026-06-06 15:54:51', '2026-06-06 15:54:51.442', '2026-06-06 15:54:51.442000');
        """

        sql """
            SELECT * FROM ${tableName} ORDER BY id
            INTO OUTFILE "${helper.uri(prefix)}"
            FORMAT AS CSV
            PROPERTIES (${helper.storageProperties()}, "column_separator" = ",");
        """

        def files = helper.downloadFiles(prefix, downloadDir).findAll { it.isFile() && it.getName().endsWith(".csv") }
        assertEquals(1, files.size())

        List<String> outLines = Files.readAllLines(Paths.get(files[0].getAbsolutePath()), StandardCharsets.UTF_8)
        assertEquals(1, outLines.size())
        assertEquals("1,2026-06-06 15:54:51,2026-06-06 15:54:51.442,2026-06-06 15:54:51.442000", outLines.get(0))
        assertFalse(outLines.get(0).contains("2026-06-06 15:54:51.442000,2026-06-06 15:54:51.442000"))
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
        downloadDir.deleteDir()
        helper.deleteFiles(prefix)
    }
}
