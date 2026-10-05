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
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.Assert

suite("test_routine_load_master_switch_exactly_once", "docker") {
    def topicSuffix = System.currentTimeMillis()
    def kafkaCsvTopic = "test_rl_master_switch_${topicSuffix}"

    if (!RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        return
    }

    def kafkaBroker = RoutineLoadTestUtils.getKafkaBroker(context)

    def options = new ClusterOptions()
    options.setFeNum(3)
    options.setBeNum(1)
    options.cloudMode = false

    docker(options) {
        def producer = RoutineLoadTestUtils.createKafkaProducer(kafkaBroker)

        def tableName = "test_rl_master_switch_exactly_once"
        def job = "test_rl_master_switch_job_${topicSuffix}"
        Integer oldMasterIndex = null

        def firstBatch = (1..30).collect { i ->
            "${i},payload_${i},2026-04-${String.format('%02d', (i % 28) + 1)}".toString()
        }
        def secondBatch = (31..40).collect { i ->
            "${i},payload_${i},2026-04-${String.format('%02d', (i % 28) + 1)}".toString()
        }

        def waitForRowCountAtLeast = { long expectedMinRows, int maxWait = 120 ->
            def waited = 0
            while (waited < maxWait) {
                def rowCount = sql "SELECT count(*) FROM ${tableName}"
                long count = (rowCount[0][0] as Number).longValue()
                if (count >= expectedMinRows) {
                    return count
                }
                sleep(1000)
                waited++
            }
            Assert.fail("Row count did not reach ${expectedMinRows} within timeout")
        }

        def waitForExactRowCount = { long expected, int maxWait = 180 ->
            def waited = 0
            while (waited < maxWait) {
                def state = sql "SHOW ROUTINE LOAD FOR ${job}"
                def rowCount = sql "SELECT count(*) FROM ${tableName}"
                long count = (rowCount[0][0] as Number).longValue()
                logger.info("waitForExactRowCount: state={}, rows={}", state[0][8].toString(), count)
                if (state[0][8].toString() == "RUNNING" && count == expected) {
                    return
                }
                sleep(1000)
                waited++
            }
            Assert.fail("Row count did not reach ${expected} within timeout")
        }

        def reconnectToCurrentMaster = {
            def currentMaster = cluster.getMasterFe()
            Assert.assertNotNull("Current master FE should exist", currentMaster)
            def masterJdbcUrl = Config.buildUrlWithDb(currentMaster.host, currentMaster.queryPort, context.dbName)
            context.connectTo(masterJdbcUrl, context.config.jdbcUser, context.config.jdbcPassword)
        }

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `id` int NOT NULL,
                `payload` string NULL,
                `ts` string NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        sql "sync"

        try {
            firstBatch.each { line ->
                producer.send(new ProducerRecord<>(kafkaCsvTopic, null, line)).get()
            }
            producer.flush()

            sql """
                CREATE ROUTINE LOAD ${job} ON ${tableName}
                COLUMNS TERMINATED BY ","
                PROPERTIES
                (
                    "max_batch_interval" = "5"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafkaBroker}",
                    "kafka_topic" = "${kafkaCsvTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            waitForRowCountAtLeast(1L)

            def oldMaster = cluster.getMasterFe()
            Assert.assertNotNull("Master FE should exist before switch", oldMaster)
            oldMasterIndex = oldMaster.index
            cluster.stopFrontends(oldMasterIndex)

            def waited = 0
            while (waited < 180) {
                def newMaster = cluster.getMasterFe()
                if (newMaster != null && newMaster.index != oldMasterIndex) {
                    logger.info("Master switched from {} to {}", oldMasterIndex, newMaster.index)
                    break
                }
                sleep(1000)
                waited++
            }
            Assert.assertTrue("A new master FE should be elected after old master stops",
                cluster.getMasterFe() != null && cluster.getMasterFe().index != oldMasterIndex)

            reconnectToCurrentMaster()

            secondBatch.each { line ->
                producer.send(new ProducerRecord<>(kafkaCsvTopic, null, line)).get()
            }
            producer.flush()

            waitForExactRowCount(40L)

            def finalCount = sql "SELECT count(*), count(distinct id), sum(id) FROM ${tableName}"
            Assert.assertEquals(40L, (finalCount[0][0] as Number).longValue())
            Assert.assertEquals(40L, (finalCount[0][1] as Number).longValue())
            Assert.assertEquals(820L, (finalCount[0][2] as Number).longValue())

            def boundaryRows = sql "SELECT id, payload FROM ${tableName} WHERE id IN (1, 30, 31, 40) ORDER BY id"
            Assert.assertEquals(4, boundaryRows.size())
            Assert.assertEquals(1, boundaryRows[0][0])
            Assert.assertEquals("payload_1", boundaryRows[0][1])
            Assert.assertEquals(30, boundaryRows[1][0])
            Assert.assertEquals("payload_30", boundaryRows[1][1])
            Assert.assertEquals(31, boundaryRows[2][0])
            Assert.assertEquals("payload_31", boundaryRows[2][1])
            Assert.assertEquals(40, boundaryRows[3][0])
            Assert.assertEquals("payload_40", boundaryRows[3][1])

            def finalState = sql "SHOW ROUTINE LOAD FOR ${job}"
            Assert.assertEquals("RUNNING", finalState[0][8].toString())
        } finally {
            if (oldMasterIndex != null) {
                try {
                    cluster.startFrontends(oldMasterIndex)
                    sleep(5000)
                } catch (Exception e) {
                    logger.warn("Failed to restart old master FE {}: {}", oldMasterIndex, e.message)
                }
            }
            try {
                reconnectToCurrentMaster()
            } catch (Exception e) {
                logger.warn("Failed to reconnect FE during cleanup: {}", e.message)
            }
            try {
                sql "STOP ROUTINE LOAD FOR ${job}"
            } catch (Exception e) {
                logger.warn("Failed to stop routine load: {}", e.message)
            }
            producer.close()
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }
}
