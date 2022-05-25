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

suite ("test_agg_vals_schema_change") {
    def tableName = "schema_change_agg_vals_regression_test"

    try {
    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
            CREATE TABLE ${tableName} (
                `user_id` LARGEINT NOT NULL COMMENT "用户id",
                `date` DATE NOT NULL COMMENT "数据灌入日期时间",
                `city` VARCHAR(20) COMMENT "用户所在城市",
                `age` SMALLINT COMMENT "用户年龄",
                `sex` TINYINT COMMENT "用户性别",
                `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                `last_update_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次更新时间",
                `last_visit_date_not_null` DATETIME REPLACE NOT NULL DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
                `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
                `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间",
                `hll_col` HLL HLL_UNION NOT NULL COMMENT "HLL列",
                `bitmap_col` Bitmap BITMAP_UNION NOT NULL COMMENT "bitmap列")
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
    def result1 = sql """
                       select * from ${tableName}
                    """
    assertTrue(result1.size() == 2)
    assertTrue(result1[0].size() == 13)
    assertTrue(result1[0][8] == 2, "user id 1 cost should be 2")
    assertTrue(result1[1][8] == 2, "user id 2 cost should be 2")

    // add column
    sql """
        ALTER table ${tableName} ADD COLUMN new_column INT MAX default "1" 
        """

    def result2 = sql """ SELECT * FROM ${tableName} WHERE user_id=2 """
    assertTrue(result1[0][8] == 2, "user id 2 cost should be 2")

    sql """ INSERT INTO ${tableName} VALUES
             (2, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, hll_hash(4), to_bitmap(4), 2)
        """
    result2 = sql """ SELECT * FROM ${tableName} WHERE user_id=2 """
    assertTrue(result2[0][8] == 3, "user id 2 cost should be 3")


    sql """ INSERT INTO ${tableName} (`user_id`,`date`,`city`,`age`,`sex`,`last_visit_date`,`last_update_date`,
                                      `last_visit_date_not_null`,`cost`,`max_dwell_time`,`min_dwell_time`, `hll_col`, `bitmap_col`)
            VALUES
             (3, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, hll_hash(4), to_bitmap(4))
        """

    result2 = sql """ SELECT * FROM ${tableName} WHERE user_id=3 """

    assertTrue(result2.size() == 1)
    assertTrue(result2[0].size() == 14)
    assertTrue(result2[0][13] == 1, "new add column default value should be 1")

    sql """ INSERT INTO ${tableName} VALUES
             (3, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2020-01-03', 1, 32, 20, hll_hash(4), to_bitmap(4), 2)
        """
    def result3 = sql """ SELECT * FROM ${tableName} WHERE user_id = 3 """

    assertTrue(result3.size() == 1)
    assertTrue(result3[0].size() == 14)
    assertTrue(result3[0][13] == 2, "new add column value is set to 2")

    def result4 = sql """ select count(*) from ${tableName} """
    logger.info("result4.size:"+result4.size() + " result4[0].size:" + result4[0].size + " " + result4[0][0])
    assertTrue(result4.size() == 1)
    assertTrue(result4[0].size() == 1)
    assertTrue(result4[0][0] == 3, "total count is 3")

    // drop column
    sql """
          ALTER TABLE ${tableName} DROP COLUMN last_visit_date
          """
    def result5 = sql """ select * from ${tableName} where user_id = 3 """
    assertTrue(result5.size() == 1)
    assertTrue(result5[0].size() == 13)

    sql """ INSERT INTO ${tableName} VALUES
             (4, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', 1, 32, 20, hll_hash(4), to_bitmap(4), 2)
        """

    def result6 = sql """ select * from ${tableName} where user_id = 4 """
    assertTrue(result6.size() == 1)
    assertTrue(result6[0].size() == 13)

    sql """ INSERT INTO ${tableName} VALUES
             (5, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', 1, 32, 20, hll_hash(5), to_bitmap(5), 2)
        """
    sql """ INSERT INTO ${tableName} VALUES
             (5, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', 1, 32, 20, hll_hash(5), to_bitmap(5), 2)
        """
    sql """ INSERT INTO ${tableName} VALUES
             (5, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', 1, 32, 20, hll_hash(5), to_bitmap(5), 2)
        """
    sql """ INSERT INTO ${tableName} VALUES
             (5, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', 1, 32, 20, hll_hash(5), to_bitmap(5), 2)
        """
    sql """ INSERT INTO ${tableName} VALUES
             (5, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', 1, 32, 20, hll_hash(5), to_bitmap(5), 2)
        """
    sql """ INSERT INTO ${tableName} VALUES
             (5, '2017-10-01', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', 1, 32, 20, hll_hash(5), to_bitmap(5), 2)
        """

    Thread.sleep(30 * 1000)
    // compaction
    String[][] tablets = sql """ show tablets from ${tableName}; """
    for (String[] tablet in tablets) {
            String tablet_id = tablet[0]
            logger.info("run compaction:" + tablet_id)
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
            //assertEquals(code, 0)
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
    def result7 = sql """ select count(*) from ${tableName} """
    assertTrue(result7.size() == 1)
    assertTrue(result7[0][0] == 5)

    def result8 = sql """  SELECT * FROM ${tableName} WHERE user_id=2 """
    assertTrue(result8.size() == 1)
    assertTrue(result8[0].size() == 13)

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
    logger.info("size:" + rowCount)
    assertTrue(rowCount <= 8)
    } finally {
        //try_sql("DROP TABLE IF EXISTS ${tableName}")
    }

}
