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

suite("test_outfile") {
    def tableName = "outfile_test"
    def helper = new ExportTestHelper(delegate)
    def prefix = "regression/test_outfile/${UUID.randomUUID()}/"
    def downloadDir = new File(System.getProperty("java.io.tmpdir"), "doris-outfile-${UUID.randomUUID()}")
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间",
            `datetime` DATETIME NOT NULL COMMENT "数据灌入日期时间",
            `date_1` DATEV2 NOT NULL COMMENT "",
            `datetime_1` DATETIMEV2 NOT NULL COMMENT "",
            `datetime_2` DATETIMEV2(3) NOT NULL COMMENT "",
            `datetime_3` DATETIMEV2(6) NOT NULL COMMENT "",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `age` SMALLINT COMMENT "用户年龄",
            `sex` TINYINT COMMENT "用户性别",
            `bool_col` boolean COMMENT "",
            `int_col` int COMMENT "",
            `bigint_col` bigint COMMENT "",
            `largeint_col` largeint COMMENT "",
            `float_col` float COMMENT "",
            `double_col` double COMMENT "",
            `char_col` CHAR(10) COMMENT "",
            `decimal_col` decimal COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        List<String> rows = []
        int i = 1
        for (; i < 1000; i ++) {
            rows.add("""
                (${i}, '2017-10-01', '2017-10-01 00:00:00', '2017-10-01', '2017-10-01 00:00:00.111111', '2017-10-01 00:00:00.111111', '2017-10-01 00:00:00.111111', 'Beijing', ${i}, ${i % 128}, true, ${i}, ${i}, ${i}, ${i}.${i}, ${i}.${i}, 'char${i}', ${i})
            """)
        }
        rows.add("""
                (${i}, '2017-10-01', '2017-10-01 00:00:00', '2017-10-01', '2017-10-01 00:00:00.111111', '2017-10-01 00:00:00.111111', '2017-10-01 00:00:00.111111', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
            """)
        rows.collate(500).each { batch ->
            sql """ INSERT INTO ${tableName} VALUES ${batch.join(",")} """
        }
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        // check outfile
        sql """
            SELECT * FROM ${tableName} t ORDER BY user_id INTO OUTFILE "${helper.uri(prefix)}"
            PROPERTIES (${helper.storageProperties()});
        """
        def files = helper.downloadFiles(prefix, downloadDir)
        assert files.size() == 1
        List<String> outLines = Files.readAllLines(Paths.get(files[0].getAbsolutePath()), StandardCharsets.UTF_8);
        List<String> baseLines = Files.readAllLines(Paths.get("""${context.config.dataPath}/export_p0/test_outfile.out"""), StandardCharsets.UTF_8)
        for (int j = 0; j < outLines.size(); j ++) {
            String[] outLine = outLines.get(j).split("\t")
            String[] baseLine = baseLines.get(j + 2).split("\t")
            for (int slotId = 0; slotId < outLine.size(); slotId ++) {
                // FIXME: Correctness validation for Datetime doesn't work in a right way so we just skip it now
                if (outLine[slotId].contains("00:00:00")) {
                    continue
                }
                if (baseLine[slotId] == "false") {
                    assert outLine[slotId] == "0"
                } else if (baseLine[slotId] == "true") {
                    assert outLine[slotId] == "1"
                } else {
                    assert outLine[slotId] == baseLine[slotId]
                }
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
        downloadDir.deleteDir()
        helper.deleteFiles(prefix)
    }

    // test hll column outfile
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
          `k1` int(11) NOT NULL,
          `v1` hll HLL_UNION NOT NULL,
          `v2` int(11) SUM NOT NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

        sql """
            insert into ${tableName} values (7483648, hll_hash(7483648), 1), (7483648, hll_hash(7483648), 1), 
            (706432 , hll_hash(706432 ), 1), (706432 , hll_hash(706432 ), 1)
        """

        sql "set return_object_data_as_binary = false"
        sql """
            SELECT * FROM ${tableName} t ORDER BY k1, v2 INTO OUTFILE "${helper.uri(prefix)}"
            PROPERTIES (${helper.storageProperties()}, "success_file_name" = "SUCCESS")
        """

        def files = helper.downloadFiles(prefix, downloadDir)
        assert files.size() == 2 // one is outfile, the other is SUCCESS file
        File dataFile = files[0].getName().contains("SUCCESS") ? files[1] : files[0];
        List<String> outLines = Files.readAllLines(Paths.get(dataFile.getAbsolutePath()), StandardCharsets.UTF_8);
        assertEquals(2, outLines.size())
        String[] outLine1 = outLines.get(0).split("\t")
        assertEquals(3, outLine1.size())
        assert outLine1[1] == "\\N"
        assert outLines.get(1).split("\t")[1] == "\\N"
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
        downloadDir.deleteDir()
        helper.deleteFiles(prefix)
    }

    // test parallel output
    try {
        sql """drop table if exists select_into_file"""
        sql """CREATE TABLE `select_into_file` (
                    `id` int,
                     `name` varchar(30)
                 ) ENGINE=OLAP
                   DUPLICATE KEY(`id`)
                   DISTRIBUTED BY HASH(`id`) BUCKETS 16
                   PROPERTIES (
                   "replication_allocation" = "tag.location.default: 1"
                   );"""
        sql """insert into select_into_file values(1, "b"),(2, "z"),(3, "a"),
                    (4, "c"), (5, "睿"), (6, "多"), (7, "丝"), (8, "test"),
                    (100, "aa"), (111, "bb"), (123, "cc"), (222, "dd"),(1, "b"),(2, "z"),(3, "a"),
                    (44, "c"), (55, "睿"), (66, "多"), (77, "丝"), (88, "test"),
                    (1000, "aa"), (1111, "bb"), (1234, "cc"), (2222, "dd");"""
        sql "set enable_parallel_outfile = true;"
        sql "set parallel_pipeline_task_num=4;"
        def result = sql """select * from select_into_file into outfile "${helper.uri(prefix)}"
            PROPERTIES (${helper.storageProperties()});"""
        assertEquals(4, result.size())
        sql "set parallel_pipeline_task_num=8;"
        result = sql """select * from select_into_file into outfile "${helper.uri(prefix)}"
            PROPERTIES (${helper.storageProperties()}, "success_file_name" = "SUCCESS");"""
        assertEquals(8, result.size())
    } finally {
        try_sql("DROP TABLE IF EXISTS select_into_file")
        downloadDir.deleteDir()
        helper.deleteFiles(prefix)
    }

    def deleteConfig = sql "ADMIN SHOW FRONTEND CONFIG LIKE 'enable_delete_existing_files'"
    if (deleteConfig[0][1].toBoolean()) {
        try {
            sql """SELECT 1 INTO OUTFILE "${helper.uri(prefix)}old_"
            PROPERTIES (${helper.storageProperties()})"""
            assertFalse(helper.keys("${prefix}old_").isEmpty())
            sql """SELECT 2 INTO OUTFILE "${helper.uri(prefix)}new_"
                   PROPERTIES (${helper.storageProperties()}, "delete_existing_files" = "true")"""
            assertTrue(helper.keys("${prefix}old_").isEmpty())
            assertFalse(helper.keys("${prefix}new_").isEmpty())
        } finally {
            helper.deleteFiles(prefix)
        }
    }
}
