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

import org.apache.doris.regression.suite.ClusterOptions

// Verify that INSERT job statistics (ScannedRows, LoadBytes, etc.) shown in
// SHOW LOAD are preserved after a FE restart. Before the fix, loadStatistic
// was not serialized to the edit log, so all counters reset to 0 after restart.
suite("test_insert_statistic_after_fe_restart", "docker") {
    def options = new ClusterOptions()
    options.setFeNum(1)
    docker(options) {
        def dbName = "test_insert_statistic_restart_db"
        def srcTbl = "src_tbl"
        def dstTbl = "dst_tbl"

        sql """DROP DATABASE IF EXISTS ${dbName}"""
        sql """CREATE DATABASE ${dbName}"""
        sql """USE ${dbName}"""

        sql """
            CREATE TABLE ${srcTbl} (
                k1 INT NULL,
                k2 VARCHAR(50) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 3
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            CREATE TABLE ${dstTbl} (
                k1 INT NULL,
                k2 VARCHAR(50) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 3
            PROPERTIES ("replication_num" = "1")
        """

        // Insert enough rows so ScannedRows and LoadBytes are clearly non-zero
        sql """INSERT INTO ${srcTbl} SELECT number, concat('value_', number)
               FROM numbers('number'='1000')"""

        // insert into select — this creates the INSERT load job tracked by show load
        sql """INSERT INTO ${dstTbl} SELECT * FROM ${srcTbl}"""

        def result = sql """SHOW LOAD FROM ${dbName}"""
        assertEquals(1, result.size())
        def jobDetailsBefore = parseJson(result[0][14])
        logger.info("JobDetails before restart: ${result[0][14]}")

        assertTrue(jobDetailsBefore.ScannedRows > 0,
                "ScannedRows should be > 0 before restart, got ${jobDetailsBefore.ScannedRows}")
        assertTrue(jobDetailsBefore.LoadBytes > 0,
                "LoadBytes should be > 0 before restart, got ${jobDetailsBefore.LoadBytes}")

        // Restart FE and reconnect
        cluster.restartFrontends()
        sleep(30000)
        context.reconnectFe()

        sql """USE ${dbName}"""

        result = sql """SHOW LOAD FROM ${dbName}"""
        assertEquals(1, result.size())
        def jobDetailsAfter = parseJson(result[0][14])
        logger.info("JobDetails after restart: ${result[0][14]}")

        assertEquals(jobDetailsBefore.ScannedRows, jobDetailsAfter.ScannedRows,
                "ScannedRows changed after FE restart: before=${jobDetailsBefore.ScannedRows}, after=${jobDetailsAfter.ScannedRows}")
        assertEquals(jobDetailsBefore.LoadBytes, jobDetailsAfter.LoadBytes,
                "LoadBytes changed after FE restart: before=${jobDetailsBefore.LoadBytes}, after=${jobDetailsAfter.LoadBytes}")
        assertEquals(jobDetailsBefore.FileNumber, jobDetailsAfter.FileNumber,
                "FileNumber changed after FE restart")
        assertEquals(jobDetailsBefore.FileSize, jobDetailsAfter.FileSize,
                "FileSize changed after FE restart")
    }
}
