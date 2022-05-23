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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_compaction_agg_keys") {
    def tableName = "compaction_agg_keys_regression_test"

    try {
        StringBuilder showConfigCommand = new StringBuilder();
        showConfigCommand.append("curl -X GET http://")
        showConfigCommand.append(context.config.beHttpAddress)
        showConfigCommand.append("/api/show_config")
        def process = showConfigCommand.toString().execute()
        int code = process.waitFor()
        String err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        String out = process.getText()
        logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def configList = parseJson(out.trim())
        assert configList instanceof List

        int cumulativeCompactionSkipWindowSeconds = -1
        boolean disableAutoCompaction = true
        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == "cumulative_compaction_skip_window_seconds") {
                cumulativeCompactionSkipWindowSeconds = Integer.parseInt(((List<String>) ele)[2])
            } else if (((List<String>) ele)[0] == "disable_auto_compaction") {
                disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
            }
        }

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                `user_id` LARGEINT NOT NULL COMMENT "用户id",
                `date` DATE NOT NULL COMMENT "数据灌入日期时间",
                `city` VARCHAR(20) COMMENT "用户所在城市",
                `age` SMALLINT COMMENT "用户年龄",
                `sex` TINYINT COMMENT "用户性别",
                `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                `last_update_date` DATETIME REPLACE_IF_NOT_NULL DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次更新时间",
                `last_visit_date_not_null` DATETIME REPLACE NOT NULL DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
                `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
                `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间",
                `hll_col` HLL HLL_UNION NOT NULL COMMENT "HLL列",
                `bitmap_col` Bitmap BITMAP_UNION NOT NULL COMMENT "bitmap列" )
            AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
            PROPERTIES ( "replication_num" = "1" );
        """

        sql """ INSERT INTO ${tableName} VALUES
             (1, '2017-10-01', 'Beijing', 10, 1, '2020-01-01', '2020-01-01', '2020-01-01', 1, 30, 20, hll_hash(1), to_bitmap(1))
            """

        sql """ INSERT INTO ${tableName} VALUES
             (1, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2020-01-02', 1, 31, 19, hll_hash(2), to_bitmap(2))
            """

        sql """ INSERT INTO ${tableName} VALUES
             (2, '2017-10-01', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2020-01-02', 1, 31, 21, hll_hash(2), to_bitmap(2))
            """

        sql """ INSERT INTO ${tableName} VALUES
             (2, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, hll_hash(3), to_bitmap(3))
            """

        sql """ INSERT INTO ${tableName} VALUES
             (3, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 22, hll_hash(3), to_bitmap(3))
            """

        sql """ INSERT INTO ${tableName} VALUES
             (3, '2017-10-01', 'Beijing', 10, 1, '2020-01-04', '2020-01-04', '2020-01-04', 1, 33, 21, hll_hash(4), to_bitmap(4))
            """

        sql """ INSERT INTO ${tableName} VALUES
             (3, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 20, hll_hash(5), to_bitmap(5))
            """

        sql """ INSERT INTO ${tableName} VALUES
             (4, '2017-10-01', 'Beijing', 10, 1, NULL, NULL, '2020-01-05', 1, 34, 20, hll_hash(5), to_bitmap(5))
            """

        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """
        String[][] tablets = sql """ show tablets from ${tableName}; """

        if (cumulativeCompactionSkipWindowSeconds > 0) {
            logger.info("Config `cumulative_compaction_skip_window_seconds` is set to " + cumulativeCompactionSkipWindowSeconds + " seconds so sleep for a while.")
            Thread.sleep(cumulativeCompactionSkipWindowSeconds * 1000)
        }

        // trigger compactions for all tablets in ${tableName}
        for (String[] tablet in tablets) {
            String tablet_id = tablet[0]
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X POST http://")
            sb.append(context.config.beHttpAddress)
            sb.append("/api/compaction/run?tablet_id=")
            sb.append(tablet_id)
            sb.append("&compact_type=cumulative")

            String command = sb.toString()
            process = command.execute()
            code = process.waitFor()
            err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            out = process.getText()
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactJson = parseJson(out.trim())
            if (compactJson.status.toLowerCase() == "fail") {
                assertEquals(disableAutoCompaction, false)
                logger.info("Compaction was done automatically!")
            }
            if (disableAutoCompaction) {
                assertEquals("success", compactJson.status.toLowerCase())
            }
        }

        // wait for all compactions done
        for (String[] tablet in tablets) {
            boolean running = true
            do {
                Thread.sleep(1000)
                String tablet_id = tablet[0]
                StringBuilder sb = new StringBuilder();
                sb.append("curl -X GET http://")
                sb.append(context.config.beHttpAddress)
                sb.append("/api/compaction/run_status?tablet_id=")
                sb.append(tablet_id)

                String command = sb.toString()
                process = command.execute()
                code = process.waitFor()
                err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
                out = process.getText()
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)
        }

        int rowCount = 0
        for (String[] tablet in tablets) {
            String tablet_id = tablet[0]
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X GET http://")
            sb.append(context.config.beHttpAddress)
            sb.append("/api/compaction/show?tablet_id=")
            sb.append(tablet_id)
            String command = sb.toString()
            // wait for cleaning stale_rowsets
            process = command.execute()
            code = process.waitFor()
            err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            out = process.getText()
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            for (String rowset in (List<String>) tabletJson.rowsets) {
                rowCount += Integer.parseInt(rowset.split(" ")[1])
            }
        }
        assert (rowCount < 8)
        qt_select_default2 """ SELECT * FROM ${tableName} t ORDER BY user_id; """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
