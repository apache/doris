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

import java.io.RandomAccessFile
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

suite("test_tablet_stat_mgr_concurrent_partition_churn", "nonConcurrent") {
    String dbName = context.config.getDbNameByFile(context.file)
    sql "select 1"

    String tableName = "test_issue_59138"
    String oldInterval = null
    AtomicBoolean stopped = new AtomicBoolean(false)
    AtomicReference<Throwable> firstError = new AtomicReference<>()

    String dorisHome = System.getProperty("DORIS_HOME")
    if (dorisHome == null || dorisHome.isEmpty()) {
        dorisHome = context.config.suitePath.replace("/regression-test/suites", "")
    }

    File feLog = new File("${dorisHome}/fe/log/fe.log")
    File feWarnLog = new File("${dorisHome}/fe/log/fe.warn.log")
    long feLogOffset = feLog.exists() ? feLog.length() : 0L
    long feWarnLogOffset = feWarnLog.exists() ? feWarnLog.length() : 0L

    def readAppendedLog = { File file, long offset ->
        if (!file.exists()) {
            return ""
        }
        RandomAccessFile raf = new RandomAccessFile(file, "r")
        try {
            long safeOffset = Math.min(offset, raf.length())
            raf.seek(safeOffset)
            int size = (int) (raf.length() - safeOffset)
            if (size <= 0) {
                return ""
            }
            byte[] bytes = new byte[size]
            raf.readFully(bytes)
            return new String(bytes, "UTF-8")
        } finally {
            raf.close()
        }
    }

    def failIfNeeded = {
        if (firstError.get() != null) {
            throw firstError.get()
        }
    }

    def startWorker = { Closure body ->
        Thread.startDaemon {
            try {
                connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
                    sql "use ${dbName}"
                    body()
                }
            } catch (Throwable t) {
                logger.warn("tablet stat mgr concurrency worker failed", t)
                firstError.compareAndSet(null, t)
                stopped.set(true)
            }
        }
    }

    try {
        def configRow = sql """ ADMIN SHOW FRONTEND CONFIG LIKE 'tablet_stat_update_interval_second' """
        oldInterval = configRow[0][1]
        sql """ ADMIN SET FRONTEND CONFIG ("tablet_stat_update_interval_second" = "1") """

        sql """ DROP TABLE IF EXISTS ${tableName} FORCE """
        sql """
            CREATE TABLE ${tableName} (
                `k1` INT NOT NULL,
                `k2` INT NOT NULL
            )
            DUPLICATE KEY(`k1`)
            PARTITION BY RANGE(`k1`) (
                PARTITION p0 VALUES LESS THAN ("100"),
                PARTITION p1 VALUES LESS THAN ("200")
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """

        sql """ INSERT INTO ${tableName} VALUES (1, 1), (101, 1) """

        def insertWorker = startWorker {
            int i = 0
            while (!stopped.get()) {
                int left = i % 90
                int right = 100 + (i % 90)
                sql """ INSERT INTO ${tableName} VALUES (${left}, ${i}), (${right}, ${i}) """
                i++
            }
        }

        def partitionWorker = startWorker {
            int i = 0
            while (!stopped.get()) {
                String partitionName = "p_dyn_${i}"
                int upperBound = 300 + i * 100
                sql """ ALTER TABLE ${tableName} ADD PARTITION ${partitionName} VALUES LESS THAN ("${upperBound}") """
                sql """ ALTER TABLE ${tableName} DROP PARTITION IF EXISTS ${partitionName} FORCE """
                i++
            }
        }

        def tableWorker = startWorker {
            int i = 0
            while (!stopped.get()) {
                String tempTable = "tmp_issue_59138_${i}"
                sql """
                    CREATE TABLE ${tempTable} (
                        `k1` INT NOT NULL,
                        `k2` INT NOT NULL
                    )
                    DUPLICATE KEY(`k1`)
                    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                    PROPERTIES (
                        "replication_num" = "1"
                    )
                """
                sql """ DROP TABLE IF EXISTS ${tempTable} FORCE """
                i++
            }
        }

        sleep(12000)
        stopped.set(true)
        [insertWorker, partitionWorker, tableWorker].each { Thread thread ->
            thread.join(10000)
        }

        failIfNeeded()
        sql "sync"
        def rowCount = sql """ SELECT count(*) FROM ${tableName} """
        assertTrue(rowCount[0][0].toLong() > 0L)

        sleep(3000)
        String appendedLogs = readAppendedLog(feLog, feLogOffset) + readAppendedLog(feWarnLog, feWarnLogOffset)
        assertFalse(appendedLogs.contains("ConcurrentModificationException"))
        assertFalse(appendedLogs.contains("daemon thread got exception. name: tablet stat mgr"))
    } finally {
        stopped.set(true)
        if (oldInterval != null) {
            sql """ ADMIN SET FRONTEND CONFIG ("tablet_stat_update_interval_second" = "${oldInterval}") """
        }
        sql """ DROP TABLE IF EXISTS ${tableName} FORCE """
    }
}
