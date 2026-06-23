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

suite("test_table_update_time") {
    if (!isCloudMode()) {
        return
    }

    def tbl = "test_table_update_time"

    def toMillis = { value ->
        if (value instanceof java.sql.Timestamp) {
            return value.getTime()
        }
        if (value instanceof java.util.Date) {
            return value.getTime()
        }
        if (value instanceof java.time.LocalDateTime) {
            return java.sql.Timestamp.valueOf(value).getTime()
        }
        return java.sql.Timestamp.valueOf(value.toString()).getTime()
    }

    def getInfoSchemaUpdateTime = {
        def result = sql """
            SELECT UPDATE_TIME
            FROM information_schema.tables
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = '${tbl}'
        """
        assertEquals(1, result.size())
        assertTrue(result[0][0] != null)
        return result[0][0]
    }

    sql """ DROP TABLE IF EXISTS ${tbl} FORCE """
    sql """
        CREATE TABLE ${tbl} (
            k INT NOT NULL,
            v INT NOT NULL
        )
        DUPLICATE KEY(k)
        PARTITION BY RANGE(k) (
            PARTITION p1 VALUES LESS THAN ("10"),
            PARTITION p2 VALUES LESS THAN ("20")
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    try {
        sql """ INSERT INTO ${tbl} VALUES (1, 1) """
        def updateTime1 = getInfoSchemaUpdateTime()
        def updateTimeMs1 = toMillis(updateTime1)
        logger.info("table update time after first insert: ${updateTime1}")

        sleep(2000)
        sql """ INSERT INTO ${tbl} VALUES (11, 11) """
        def updateTime2 = getInfoSchemaUpdateTime()
        def updateTimeMs2 = toMillis(updateTime2)
        logger.info("table update time after second insert: ${updateTime2}")
        assertTrue(updateTimeMs2 > updateTimeMs1)

        sleep(2000)
        sql """ ALTER TABLE ${tbl} DROP PARTITION p2 FORCE """
        def updateTime3 = getInfoSchemaUpdateTime()
        def updateTimeMs3 = toMillis(updateTime3)
        logger.info("table update time after dropping latest partition: ${updateTime3}")
        assertTrue(updateTimeMs3 > updateTimeMs2)

        sleep(2000)
        sql """ BEGIN """
        sql """ INSERT INTO ${tbl} VALUES (2, 2) """
        sql """ INSERT INTO ${tbl} VALUES (3, 3) """
        sql """ COMMIT """
        def updateTime4 = getInfoSchemaUpdateTime()
        def updateTimeMs4 = toMillis(updateTime4)
        logger.info("table update time after sub transaction commit: ${updateTime4}")
        assertTrue(updateTimeMs4 > updateTimeMs3)

        def tableStatus = sql """ SHOW TABLE STATUS LIKE '${tbl}' """
        assertEquals(1, tableStatus.size())
        assertTrue(tableStatus[0][12] != null)
        logger.info("show table status update time: ${tableStatus[0][12]}")
        assertTrue(toMillis(tableStatus[0][12]) >= updateTimeMs4)
    } finally {
        sql """ DROP TABLE IF EXISTS ${tbl} FORCE """
    }
}
