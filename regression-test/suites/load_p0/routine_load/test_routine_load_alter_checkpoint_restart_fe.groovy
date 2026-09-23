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

import org.apache.doris.regression.Config
import org.apache.doris.regression.suite.ClusterOptions
import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

suite("test_routine_load_alter_checkpoint_restart_fe", "docker") {
    def runSuffix = System.currentTimeMillis().toString()
    def persistedPropertyKeys = [
        "columnToColumnExpr",
        "column_separator",
        "precedingFilter",
        "whereExpr",
        "partitions",
        "delete",
        "sequence_col",
        "merge_type",
        "max_batch_interval"
    ]

    def readDefinition = { String jobName ->
        def showResult = sql "SHOW ROUTINE LOAD FOR ${jobName}"
        assertEquals(1, showResult.size(), "${jobName}: SHOW ROUTINE LOAD should return one row")
        assertEquals("PAUSED", showResult[0][8].toString(), "${jobName}: job should remain paused")
        def properties = parseJson(showResult[0][11].toString())
        def persistedProperties = persistedPropertyKeys.collectEntries { key ->
            [(key): properties[key].toString()]
        }

        def showCreateResult = sql "SHOW CREATE ROUTINE LOAD FOR ${jobName}"
        assertEquals(1, showCreateResult.size(), "${jobName}: SHOW CREATE should return one row")
        String createSql = showCreateResult[0][2].toString()
        int propertiesIndex = createSql.indexOf("PROPERTIES\n")
        assertTrue(propertiesIndex > 0, "${jobName}: SHOW CREATE should contain PROPERTIES")
        return [
            properties: persistedProperties,
            loadSql: createSql.substring(0, propertiesIndex)
        ]
    }

    def assertDefinitionEquals = { testCase, String phase, expected, actual ->
        persistedPropertyKeys.each { key ->
            assertEquals(expected.properties[key], actual.properties[key],
                    "${testCase.id}: ${key} changed after ${phase}")
        }
        assertEquals(expected.loadSql, actual.loadSql,
                "${testCase.id}: SHOW CREATE load definition changed after ${phase}")
    }

    def assertAlterApplied = { testCase, before, after ->
        String actualProperty = after.properties[testCase.targetKey]
        assertNotEquals(before.properties[testCase.targetKey], actualProperty,
                "${testCase.id}: ALTER did not change ${testCase.targetKey}")
        if (testCase.propertyExact != null) {
            assertEquals(testCase.propertyExact, actualProperty,
                    "${testCase.id}: unexpected ${testCase.targetKey}")
        }
        testCase.propertyContains.each { fragment ->
            assertTrue(actualProperty.contains(fragment),
                    "${testCase.id}: ${testCase.targetKey} should contain ${fragment}")
        }
        testCase.propertyExcludes.each { fragment ->
            assertFalse(actualProperty.contains(fragment),
                    "${testCase.id}: ${testCase.targetKey} should not contain ${fragment}")
        }
        if (testCase.rejectActualNewline) {
            assertFalse(actualProperty.contains("\n"),
                    "${testCase.id}: a literal backslash-n became a newline")
        }
        testCase.showContains.each { fragment ->
            assertTrue(after.loadSql.contains(fragment),
                    "${testCase.id}: SHOW CREATE should contain ${fragment}")
        }
        testCase.showExcludes.each { fragment ->
            assertFalse(after.loadSql.contains(fragment),
                    "${testCase.id}: SHOW CREATE should not contain ${fragment}")
        }
    }

    def createTable = { String tableName, String tableKind ->
        sql "DROP TABLE IF EXISTS ${tableName}"
        String keyType = tableKind == "duplicate"
                ? "DUPLICATE KEY(`id`)" : "UNIQUE KEY(`id`)"
        def tableProperties = ['"replication_num" = "1"']
        if (tableKind != "duplicate") {
            tableProperties.add('"enable_unique_key_merge_on_write" = "true"')
        }
        if (tableKind == "sequence") {
            tableProperties.add('"function_column.sequence_type" = "BIGINT"')
        }
        sql """
            CREATE TABLE ${tableName} (
                `id` INT NOT NULL,
                `city` STRING NULL,
                `op` STRING NULL,
                `tag` STRING NULL,
                `stage` STRING NULL,
                `mapped_col` BIGINT NULL,
                `text1` STRING NULL,
                `seq_old` BIGINT NULL,
                `seq_new` BIGINT NULL
            ) ENGINE=OLAP
            ${keyType}
            PARTITION BY RANGE(`id`) (
                PARTITION `p_old` VALUES LESS THAN ("100"),
                PARTITION `p_new` VALUES LESS THAN ("200"),
                PARTITION `p_max` VALUES LESS THAN (MAXVALUE)
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                ${tableProperties.join(",\n                ")}
            )
        """
    }

    def newTestCases = { String lifecycle ->
        def prefix = "rlp_${lifecycle}_${runSuffix}"
        return [
            [
                id: "columns_mapping",
                tableName: "${prefix}_columns_tbl",
                jobName: "${prefix}_columns_job",
                tableKind: "duplicate",
                createLoadClause: "COLUMNS(id, city, op, tag, stage, mapped_col = id + 1)",
                alterLoadClause: "COLUMNS(id, op, city, tag, stage, mapped_col = id + 2)",
                targetKey: "columnToColumnExpr",
                propertyExact: null,
                propertyContains: ["mapped_col", "2"],
                propertyExcludes: ["+ 1"],
                showContains: ["COLUMNS(", "mapped_col", "+ 2"],
                showExcludes: ["+ 1"],
                rejectActualNewline: false
            ],
            [
                id: "where",
                tableName: "${prefix}_where_tbl",
                jobName: "${prefix}_where_job",
                tableKind: "duplicate",
                createLoadClause: "WHERE mapped_col < 100",
                alterLoadClause: "WHERE mapped_col < 50",
                targetKey: "whereExpr",
                propertyExact: null,
                propertyContains: ["mapped_col", "50"],
                propertyExcludes: ["100"],
                showContains: ["WHERE", "mapped_col", "50"],
                showExcludes: ["100"],
                rejectActualNewline: false
            ],
            [
                id: "column_separator",
                tableName: "${prefix}_separator_tbl",
                jobName: "${prefix}_separator_job",
                tableKind: "duplicate",
                createLoadClause: "COLUMNS TERMINATED BY ','",
                alterLoadClause: "COLUMNS TERMINATED BY '|'",
                targetKey: "column_separator",
                propertyExact: "'|'",
                propertyContains: [],
                propertyExcludes: [],
                showContains: ["COLUMNS TERMINATED BY \"|\""],
                showExcludes: ["COLUMNS TERMINATED BY \",\""],
                rejectActualNewline: false
            ],
            [
                id: "escaped_column_separator",
                tableName: "${prefix}_escaped_separator_tbl",
                jobName: "${prefix}_escaped_separator_job",
                tableKind: "duplicate",
                createLoadClause: "COLUMNS TERMINATED BY ','",
                alterLoadClause: /COLUMNS TERMINATED BY '\x01'/,
                targetKey: "column_separator",
                propertyExact: "'\\x01'",
                propertyContains: [],
                propertyExcludes: [],
                showContains: ["COLUMNS TERMINATED BY \"\\x01\""],
                showExcludes: ["COLUMNS TERMINATED BY \",\""],
                rejectActualNewline: false
            ],
            [
                id: "preceding_filter",
                tableName: "${prefix}_preceding_tbl",
                jobName: "${prefix}_preceding_job",
                tableKind: "duplicate",
                createLoadClause: "PRECEDING FILTER id > 111",
                alterLoadClause: "PRECEDING FILTER id > 888",
                targetKey: "precedingFilter",
                propertyExact: null,
                propertyContains: ["id", "888"],
                propertyExcludes: ["111"],
                showContains: ["PRECEDING FILTER", "888"],
                showExcludes: ["111"],
                rejectActualNewline: false
            ],
            [
                id: "partition",
                tableName: "${prefix}_partition_tbl",
                jobName: "${prefix}_partition_job",
                tableKind: "duplicate",
                createLoadClause: "PARTITION(p_old)",
                alterLoadClause: "PARTITION(p_new)",
                targetKey: "partitions",
                propertyExact: "p_new",
                propertyContains: [],
                propertyExcludes: [],
                showContains: ["PARTITION(p_new)"],
                showExcludes: ["PARTITION(p_old)"],
                rejectActualNewline: false
            ],
            [
                id: "delete_on",
                tableName: "${prefix}_delete_tbl",
                jobName: "${prefix}_delete_job",
                tableKind: "delete",
                mergeClause: "WITH MERGE",
                createLoadClause: "COLUMNS(id, op), DELETE ON op = 'DELETE_OLD'",
                alterLoadClause: "DELETE ON op = 'DELETE_NEW'",
                targetKey: "delete",
                propertyExact: null,
                propertyContains: ["DELETE_NEW"],
                propertyExcludes: ["DELETE_OLD"],
                showContains: ["WITH MERGE", "DELETE ON", "DELETE_NEW"],
                showExcludes: ["DELETE_OLD"],
                rejectActualNewline: false
            ],
            [
                id: "sequence_column",
                tableName: "${prefix}_sequence_tbl",
                jobName: "${prefix}_sequence_job",
                tableKind: "sequence",
                createLoadClause: "COLUMNS(id, seq_old, seq_new), ORDER BY seq_old",
                alterLoadClause: "ORDER BY seq_new",
                targetKey: "sequence_col",
                propertyExact: "seq_new",
                propertyContains: [],
                propertyExcludes: [],
                showContains: ["ORDER BY seq_new"],
                showExcludes: ["ORDER BY seq_old"],
                rejectActualNewline: false
            ],
            [
                id: "default_mode_backslash_literal",
                tableName: "${prefix}_default_backslash_tbl",
                jobName: "${prefix}_default_backslash_job",
                tableKind: "duplicate",
                createLoadClause: "WHERE text1 = 'old_default'",
                alterLoadClause: /WHERE text1 = 'A\\nB'/,
                targetKey: "whereExpr",
                propertyExact: null,
                propertyContains: ["A\\nB"],
                propertyExcludes: ["old_default"],
                showContains: ["WHERE", "A\\nB"],
                showExcludes: ["old_default"],
                rejectActualNewline: true
            ],
            [
                id: "no_backslash_escapes_literal",
                tableName: "${prefix}_no_backslash_tbl",
                jobName: "${prefix}_no_backslash_job",
                tableKind: "duplicate",
                createLoadClause: "WHERE text1 = 'old_no_backslash'",
                alterLoadClause: /WHERE text1 = 'A\nB'/,
                sqlMode: "NO_BACKSLASH_ESCAPES",
                targetKey: "whereExpr",
                propertyExact: null,
                propertyContains: ["A\\nB"],
                propertyExcludes: ["old_no_backslash"],
                showContains: ["WHERE", "A\\nB"],
                showExcludes: ["old_no_backslash"],
                rejectActualNewline: true
            ],
            [
                id: "json_function_backslash_literal",
                tableName: "${prefix}_json_function_tbl",
                jobName: "${prefix}_json_function_job",
                tableKind: "duplicate",
                createLoadClause: "WHERE text1 = 'old_json_function'",
                alterLoadClause: /WHERE json_object('key', 'A\\nB') IS NOT NULL/,
                targetKey: "whereExpr",
                propertyExact: null,
                propertyContains: ["json_object", "nB"],
                propertyExcludes: ["old_json_function"],
                showContains: ["WHERE", "json_object", "nB"],
                showExcludes: ["old_json_function"],
                rejectActualNewline: true
            ],
            [
                id: "property_only",
                tableName: "${prefix}_property_tbl",
                jobName: "${prefix}_property_job",
                tableKind: "duplicate",
                createLoadClause: "COLUMNS(id, city), WHERE city != 'keep_definition'",
                alterLoadClause: "PROPERTIES (\"max_batch_interval\" = \"10\")",
                propertyOnly: true,
                targetKey: "max_batch_interval",
                propertyExact: "10",
                propertyContains: [],
                propertyExcludes: [],
                showContains: ["COLUMNS(", "WHERE", "keep_definition"],
                showExcludes: [],
                rejectActualNewline: false
            ]
        ]
    }

    def prepareCases = { String lifecycle, List<String> createdJobs ->
        def testCases = newTestCases(lifecycle)
        testCases.each { testCase ->
            createTable(testCase.tableName, testCase.tableKind)
            String mergeClause = testCase.mergeClause ?: ""
            sql """
                CREATE ROUTINE LOAD ${testCase.jobName} ON ${testCase.tableName}
                ${mergeClause}
                ${testCase.createLoadClause}
                PROPERTIES (
                    "max_batch_interval" = "5"
                )
                FROM KAFKA (
                    "kafka_broker_list" = "127.0.0.1:9092",
                    "kafka_topic" = "${testCase.jobName}_topic"
                )
            """
            createdJobs.add(testCase.jobName)
            sql "PAUSE ROUTINE LOAD FOR ${testCase.jobName}"

            def before = readDefinition(testCase.jobName)
            if (testCase.sqlMode != null) {
                sql "SET sql_mode = '${testCase.sqlMode}'"
            }
            try {
                sql "ALTER ROUTINE LOAD FOR ${testCase.jobName} ${testCase.alterLoadClause}"
            } finally {
                if (testCase.sqlMode != null) {
                    sql "SET sql_mode = 'DEFAULT'"
                }
            }
            def after = readDefinition(testCase.jobName)
            assertAlterApplied(testCase, before, after)
            if (testCase.id == "delete_on") {
                assertEquals("MERGE", after.properties.merge_type,
                        "delete_on: merge type should remain MERGE")
            }
            if (testCase.propertyOnly) {
                assertEquals(before.loadSql, after.loadSql,
                        "property_only: a typed property ALTER must not rewrite the load definition")
            }
            testCase.expected = after
        }
        return testCases
    }

    def reconnectToCurrentMaster = {
        def master = cluster.getMasterFe()
        if (master == null) {
            throw new IllegalStateException("No master FE is available")
        }
        def jdbcUrl = Config.buildUrlWithDb(master.host, master.queryPort, context.dbName)
        context.connectTo(jdbcUrl, context.config.jdbcUser, context.config.jdbcPassword)
    }

    def currentJournalId = {
        def result = sql """
            SELECT ReplayedJournalId FROM frontends() WHERE IsMaster = 'true'
        """
        return result[0][0].toString().toLong()
    }

    def waitForAllFeReplay = { long targetJournalId, int expectedFeCount ->
        Awaitility.await().atMost(90, SECONDS).pollInterval(1, SECONDS).until {
            def replayedIds = sql """
                SELECT ReplayedJournalId FROM frontends() WHERE Alive = 'true'
            """
            return replayedIds.size() == expectedFeCount && replayedIds.every {
                it[0].toString().toLong() >= targetJournalId
            }
        }
    }

    def latestImageSequence = { frontend ->
        def imageDir = new File(frontend.getBasePath(), "doris-meta/image")
        if (!imageDir.isDirectory()) {
            throw new IllegalStateException("Image directory does not exist: ${imageDir}")
        }
        def imageFiles = imageDir.listFiles()
        if (imageFiles == null) {
            throw new IllegalStateException("Cannot list image directory: ${imageDir}")
        }
        long latest = -1L
        imageFiles.each { file ->
            if (file.name ==~ /^image\.\d+$/) {
                latest = Math.max(latest, file.name.substring("image.".length()).toLong())
            }
        }
        return latest
    }

    def stopJobs = { List<String> createdJobs ->
        createdJobs.each { jobName ->
            try {
                sql "STOP ROUTINE LOAD FOR ${jobName}"
            } catch (Exception e) {
                logger.warn("Failed to stop routine load job {}: {}", jobName, e.message)
            }
        }
    }

    def journalOptions = new ClusterOptions()
    journalOptions.setFeNum(3)
    journalOptions.setBeNum(1)
    journalOptions.cloudMode = false
    journalOptions.feConfigs += ["edit_log_roll_num=50000"]

    docker(journalOptions) {
        def createdJobs = []
        Integer stoppedMasterIndex = null
        try {
            def testCases = prepareCases("journal", createdJobs)
            long targetJournalId = currentJournalId()
            waitForAllFeReplay(targetJournalId, 3)

            cluster.getAllFrontends(true).each { frontend ->
                assertTrue(latestImageSequence(frontend) < targetJournalId,
                        "journal: FE ${frontend.index} checkpoint already contains tested ALTERs")
            }

            def oldMaster = cluster.getMasterFe()
            assertNotNull(oldMaster)
            stoppedMasterIndex = oldMaster.index
            cluster.stopFrontends(stoppedMasterIndex)
            Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until {
                def newMaster = cluster.getMasterFe()
                return newMaster != null && newMaster.index != stoppedMasterIndex
            }
            reconnectToCurrentMaster()

            testCases.each { testCase ->
                assertDefinitionEquals(testCase, "journal replay and master failover",
                        testCase.expected, readDefinition(testCase.jobName))
            }
        } finally {
            if (stoppedMasterIndex != null) {
                try {
                    cluster.startFrontends(stoppedMasterIndex)
                } catch (Exception e) {
                    logger.warn("Failed to restart old master FE {}: {}", stoppedMasterIndex, e.message)
                }
            }
            try {
                reconnectToCurrentMaster()
                stopJobs(createdJobs)
            } catch (Exception e) {
                logger.warn("Failed to clean journal replay cases: {}", e.message)
            }
        }
    }

    def imageOptions = new ClusterOptions()
    imageOptions.setFeNum(1)
    imageOptions.setBeNum(1)
    imageOptions.cloudMode = false
    imageOptions.feConfigs += ["edit_log_roll_num=1"]

    docker(imageOptions) {
        def createdJobs = []
        try {
            def testCases = prepareCases("image", createdJobs)
            long targetJournalId = currentJournalId()
            def checkpointMaster = cluster.getMasterFe()
            Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until {
                return latestImageSequence(checkpointMaster) >= targetJournalId
            }

            cluster.restartFrontends()
            boolean reconnected = false
            for (int retry = 0; retry < 60 && !reconnected; retry++) {
                try {
                    reconnectToCurrentMaster()
                    sql "SELECT 1"
                    reconnected = true
                } catch (Exception ignored) {
                    sleep(1000)
                }
            }
            assertTrue(reconnected, "image: failed to reconnect after restarting FE")

            testCases.each { testCase ->
                assertDefinitionEquals(testCase, "checkpoint and image recovery",
                        testCase.expected, readDefinition(testCase.jobName))
            }
        } finally {
            try {
                reconnectToCurrentMaster()
                stopJobs(createdJobs)
            } catch (Exception e) {
                logger.warn("Failed to clean image recovery cases: {}", e.message)
            }
        }
    }
}
