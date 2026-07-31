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
import org.apache.doris.regression.util.RoutineLoadTestUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.Assert

suite("test_routine_load_too_many_tasks_recovery_exactly_once", "docker") {
    def topicSuffix = System.currentTimeMillis()
    def kafkaCsvTopic = "test_rl_too_many_tasks_recovery_${topicSuffix}"

    if (!RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        return
    }

    def kafkaPort = context.config.otherConfigs.get("kafka_port")
    Assert.assertNotNull("kafka_port must be configured in regression-conf.groovy", kafkaPort)
    def kafka_broker = "127.0.0.1:${kafkaPort}"

    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = false
    options.enableDebugPoints()

    docker(options) {
        def producer = RoutineLoadTestUtils.createKafkaProducer(kafka_broker)

        def tableName = "test_rl_too_many_tasks_recovery"
        def job = "test_too_many_tasks_recovery_job_${topicSuffix}"
        def debugPoint = "FE.ROUTINE_LOAD_TASK_SUBMIT_FAILED.TOO_MANY_TASKS"

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

        List<String> testData = []
        for (int i = 1; i <= 20; i++) {
            testData.add("${i},payload_${i},2026-02-${String.format('%02d', i)}".toString())
        }

        testData.each { line ->
            logger.info("Sending to Kafka: ${line}")
            def record = new ProducerRecord<>(kafkaCsvTopic, null, line.toString())
            producer.send(record).get()
        }
        producer.flush()
        logger.info("All 20 records sent to Kafka")

        try {
            GetDebugPoint().enableDebugPointForAllFEs(debugPoint)
            logger.info("Debug point enabled: ${debugPoint}")

            sql """
                CREATE ROUTINE LOAD ${job} ON ${tableName}
                COLUMNS TERMINATED BY ","
                PROPERTIES
                (
                    "max_batch_interval" = "5"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaCsvTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            def waitCount = 0
            def sawFailure = false
            while (waitCount < 30) {
                sleep(1000)
                def state = sql "show routine load for ${job}"
                def routineLoadState = state[0][8].toString()
                def statistic = state[0][14].toString()
                def otherMsg = state[0][19].toString()
                logger.info("State: ${routineLoadState}, stats: ${statistic}, msg: ${otherMsg}")

                def statJson = new groovy.json.JsonSlurper().parseText(statistic)
                long abortedCount = statJson.abortedTaskNum as long
                if (abortedCount > 0 || otherMsg.contains("TOO_MANY_TASKS")) {
                    sawFailure = true
                    logger.info("Detected TOO_MANY_TASKS failure: abortedTaskNum=${abortedCount}, msg=${otherMsg}")
                    break
                }
                waitCount++
            }
            Assert.assertTrue("应观察到 TOO_MANY_TASKS 失败（abortedTaskNum>0 或 otherMsg 含相关信息）", sawFailure)

            def failPhaseCount = sql "select count(*) from ${tableName}"
            logger.info("Row count during failure phase: ${failPhaseCount[0][0]}")

            GetDebugPoint().disableDebugPointForAllFEs(debugPoint)
            logger.info("Debug point disabled, waiting for recovery...")

            def maxWaitCount = 120
            def count = 0
            while (true) {
                def state = sql "show routine load for ${job}"
                def routineLoadState = state[0][8].toString()
                def statistic = state[0][14].toString()
                logger.info("Recovery phase - State: ${routineLoadState}, stats: ${statistic}")

                def rowCount = sql "select count(*) from ${tableName}"
                if (routineLoadState == "RUNNING" && rowCount[0][0] == 20) {
                    logger.info("Job recovered and all 20 rows consumed")
                    break
                }
                if (count > maxWaitCount) {
                    Assert.fail("Routine load did not recover within timeout. "
                        + "State: ${routineLoadState}, rows: ${rowCount[0][0]}")
                }
                sleep(1000)
                count++
            }

            def finalCount = sql "select count(*) from ${tableName}"
            Assert.assertEquals("应精确消费 20 行", 20L, finalCount[0][0])

            def distinctCount = sql "select count(distinct id) from ${tableName}"
            Assert.assertEquals("不应有重复 id", 20L, distinctCount[0][0])

            def sumId = sql "select sum(id) from ${tableName}"
            Assert.assertEquals("sum(id) 应为 1+2+...+20=210", 210L, sumId[0][0])

            def minId = sql "select min(id) from ${tableName}"
            Assert.assertEquals("min(id) 应为 1", 1, minId[0][0])

            def maxId = sql "select max(id) from ${tableName}"
            Assert.assertEquals("max(id) 应为 20", 20, maxId[0][0])

            def finalState = sql "show routine load for ${job}"
            Assert.assertEquals("最终状态应为 RUNNING", "RUNNING", finalState[0][8].toString())
            logger.info("All exactly-once assertions passed after TOO_MANY_TASKS recovery")
        } catch (Exception e) {
            logger.error("Test failed: ${e.getMessage()}")
            throw e
        } finally {
            try {
                GetDebugPoint().disableDebugPointForAllFEs(debugPoint)
            } catch (Exception e) {
                logger.warn("Failed to disable debug point in cleanup: ${e.message}")
            }
            try {
                sql "stop routine load for ${job}"
            } catch (Exception e) {
                logger.warn("Failed to stop routine load: ${e.message}")
            }
            producer.close()
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }
}
