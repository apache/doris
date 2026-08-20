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
import org.apache.doris.regression.util.RoutineLoadTestUtils
import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

suite("test_routine_load_alter_checkpoint_restart_fe", "docker") {
    if (!RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        return
    }

    def topicSuffix = System.currentTimeMillis()
    def jobName = "test_routine_load_persistence_job_${topicSuffix}"
    def topic = "test_routine_load_persistence_topic_${topicSuffix}"
    def kafkaBroker = RoutineLoadTestUtils.getKafkaBroker(context)

    def options = new ClusterOptions()
    options.setFeNum(3)
    options.setBeNum(1)
    options.cloudMode = false
    options.feConfigs += [
        "edit_log_roll_num=50000"
    ]

    def persistedKeys = [
        "columnToColumnExpr",
        "column_separator",
        "precedingFilter",
        "whereExpr",
        "merge_type"
    ]

    docker(options) {
        def producer = RoutineLoadTestUtils.createKafkaProducer(kafkaBroker)
        Integer stoppedMasterIndex = null

        def runSql = { String query -> sql query }
        def readDefinition = {
            def showResult = sql "SHOW ROUTINE LOAD FOR ${jobName}"
            assertEquals("PAUSED", showResult[0][8].toString())
            def properties = parseJson(showResult[0][11].toString())
            return persistedKeys.collectEntries { key ->
                [(key): properties[key].toString()]
            }
        }
        def assertPropertiesEqual = { expected, actual ->
            persistedKeys.each { key ->
                assertEquals(expected[key], actual[key])
            }
        }
        def currentJournalId = {
            def result = sql """
                SELECT ReplayedJournalId FROM frontends() WHERE IsMaster = 'true'
            """
            return result[0][0].toString().toLong()
        }
        def waitForAllFeReplay = { long targetJournalId ->
            Awaitility.await().atMost(90, SECONDS).pollInterval(1, SECONDS).until {
                def replayedIds = sql """
                    SELECT ReplayedJournalId FROM frontends() WHERE Alive = 'true'
                """
                return replayedIds.size() == 3 && replayedIds.every {
                    it[0].toString().toLong() >= targetJournalId
                }
            }
        }
        def latestImageSequence = { frontend ->
            def imageDir = new File(frontend.getBasePath(), "doris-meta/image")
            long latest = -1L
            imageDir.listFiles()?.each { file ->
                if (file.name ==~ /^image\.\d+$/) {
                    latest = Math.max(latest, file.name.substring("image.".length()).toLong())
                }
            }
            return latest
        }
        def waitForCheckpoint = { frontend, long targetJournalId ->
            Awaitility.await().atMost(150, SECONDS).pollInterval(1, SECONDS).until {
                return latestImageSequence(frontend) >= targetJournalId
            }
        }
        def reconnectToCurrentMaster = {
            def master = cluster.getMasterFe()
            assertNotNull(master)
            def jdbcUrl = Config.buildUrlWithDb(master.host, master.queryPort, context.dbName)
            context.connectTo(jdbcUrl, context.config.jdbcUser, context.config.jdbcPassword)
        }

        sql "DROP TABLE IF EXISTS test_routine_load_alter_checkpoint_restart_tbl"
        sql """
            CREATE TABLE test_routine_load_alter_checkpoint_restart_tbl (
                `id` INT NULL,
                `source_col` STRING NULL,
                `event_date` DATE NULL,
                `text1` STRING NULL,
                `event_dt` DATETIME NULL,
                `text2` STRING NULL,
                `mapped_col` BIGINT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """

        try {
            RoutineLoadTestUtils.sendTestDataToKafka(producer, [topic])
            producer.flush()

            sql """
                CREATE ROUTINE LOAD ${jobName} ON test_routine_load_alter_checkpoint_restart_tbl
                COLUMNS TERMINATED BY ",",
                COLUMNS(id, source_col, event_date, text1, event_dt, text2, mapped_col = id + 1),
                PRECEDING FILTER id > 0,
                WHERE mapped_col < 100
                PROPERTIES (
                    "max_batch_interval" = "5",
                    "exec_mem_limit" = "268435456"
                )
                FROM KAFKA (
                    "kafka_broker_list" = "${kafkaBroker}",
                    "kafka_topic" = "${topic}",
                    "property.group.id" = "${jobName}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                )
            """

            RoutineLoadTestUtils.waitForTaskFinish(
                    runSql, jobName, "test_routine_load_alter_checkpoint_restart_tbl", 0)
            sql "PAUSE ROUTINE LOAD FOR ${jobName}"

            def originalDefinition = readDefinition()
            def oldMaster = cluster.getMasterFe()
            assertNotNull(oldMaster)
            stoppedMasterIndex = oldMaster.index

            sql "ALTER ROUTINE LOAD FOR ${jobName} COLUMNS TERMINATED BY '|'"
            sql """
                ALTER ROUTINE LOAD FOR ${jobName}
                COLUMNS(id, source_col, event_date, text1, event_dt, text2, mapped_col = id + 2)
            """
            sql "ALTER ROUTINE LOAD FOR ${jobName} PRECEDING FILTER id > 8"
            sql "ALTER ROUTINE LOAD FOR ${jobName} WHERE mapped_col < 50"

            def alteredDefinition = readDefinition()
            assertNotEquals(originalDefinition.columnToColumnExpr, alteredDefinition.columnToColumnExpr)
            assertEquals("'|'", alteredDefinition.column_separator)
            assertNotEquals(originalDefinition.precedingFilter, alteredDefinition.precedingFilter)
            assertNotEquals(originalDefinition.whereExpr, alteredDefinition.whereExpr)

            long alterJournalId = currentJournalId()
            waitForAllFeReplay(alterJournalId)
            assertTrue("ALTER must be tested through journal replay before checkpoint",
                    latestImageSequence(oldMaster) < alterJournalId)

            cluster.stopFrontends(stoppedMasterIndex)
            Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until {
                def newMaster = cluster.getMasterFe()
                return newMaster != null && newMaster.index != stoppedMasterIndex
            }
            reconnectToCurrentMaster()

            def failoverDefinition = readDefinition()
            assertPropertiesEqual(alteredDefinition, failoverDefinition)

            // Force the new leader to roll the next ALTER journal so that checkpoint recovery is also covered.
            sql "ADMIN SET FRONTEND CONFIG ('edit_log_roll_num' = '1')"
            sql "ALTER ROUTINE LOAD FOR ${jobName} WHERE mapped_col < 40"
            def finalDefinition = readDefinition()
            assertNotEquals(failoverDefinition.whereExpr, finalDefinition.whereExpr)

            long finalJournalId = currentJournalId()
            cluster.startFrontends(stoppedMasterIndex)
            stoppedMasterIndex = null
            waitForAllFeReplay(finalJournalId)

            def checkpointMaster = cluster.getMasterFe()
            waitForCheckpoint(checkpointMaster, finalJournalId)
            cluster.restartFrontends()
            sleep(30000)
            reconnectToCurrentMaster()

            def restartedDefinition = readDefinition()
            assertPropertiesEqual(finalDefinition, restartedDefinition)
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
                sql "STOP ROUTINE LOAD FOR ${jobName}"
            } catch (Exception e) {
                logger.warn("Failed to stop routine load job {}: {}", jobName, e.message)
            }
            producer.close()
        }
    }
}
