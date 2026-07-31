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

suite("test_routine_load_commit_failure_recovery_exactly_once", "docker") {
    if (!RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        return
    }

    def topicSuffix = System.currentTimeMillis()
    def kafkaTopic = "test_rl_commit_failure_${topicSuffix}"
    def kafkaPort = context.config.otherConfigs.get("kafka_port")
    Assert.assertNotNull("kafka_port must be configured in regression-conf.groovy", kafkaPort)
    def kafkaBroker = "127.0.0.1:${kafkaPort}"

    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = false
    options.enableDebugPoints()

    docker(options) {
        def producer = RoutineLoadTestUtils.createKafkaProducer(kafkaBroker)
        def tableName = "test_rl_commit_failure"
        def jobName = "test_rl_commit_failure_job_${topicSuffix}"
        def debugPoint = "StreamLoadExecutor.commit_txn.failed"

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                `id` int NOT NULL,
                `payload` string NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        sql "sync"

        for (int i = 1; i <= 20; i++) {
            producer.send(new ProducerRecord<>(kafkaTopic, null, "${i},payload_${i}".toString())).get()
        }
        producer.flush()

        try {
            GetDebugPoint().enableDebugPointForAllBEs(debugPoint)

            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS TERMINATED BY ","
                PROPERTIES ("max_batch_interval" = "5")
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafkaBroker}",
                    "kafka_topic" = "${kafkaTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                )
            """

            def sawPaused = false
            for (int i = 0; i < 60; i++) {
                sleep(1000)
                def job = sql "SHOW ROUTINE LOAD FOR ${jobName}"
                def state = job[0][8].toString()
                def reason = job[0][17].toString()
                logger.info("Commit failure phase - state: ${state}, reason: ${reason}")
                if (state == "PAUSED"
                        && reason.contains("TASKS_ABORT_ERR")
                        && reason.contains("COMMIT_FAILED")) {
                    sawPaused = true
                    break
                }
            }
            Assert.assertTrue("Routine load job should pause after transaction commit failure", sawPaused)
            Assert.assertEquals("Failed transaction must not publish rows",
                    0L, (sql "SELECT count(*) FROM ${tableName}")[0][0])

            GetDebugPoint().disableDebugPointForAllBEs(debugPoint)

            def recovered = false
            for (int i = 0; i < 120; i++) {
                def job = sql "SHOW ROUTINE LOAD FOR ${jobName}"
                def state = job[0][8].toString()
                def rowCount = (sql "SELECT count(*) FROM ${tableName}")[0][0]
                logger.info("Recovery phase - state: ${state}, rows: ${rowCount}")
                if (state == "RUNNING" && rowCount == 20L) {
                    recovered = true
                    break
                }
                sleep(1000)
            }
            Assert.assertTrue("Routine load job should auto-resume and consume all rows", recovered)
            Assert.assertEquals(20L, (sql "SELECT count(DISTINCT id) FROM ${tableName}")[0][0])
            Assert.assertEquals(210L, (sql "SELECT sum(id) FROM ${tableName}")[0][0])
        } finally {
            try {
                GetDebugPoint().disableDebugPointForAllBEs(debugPoint)
            } catch (Exception e) {
                logger.warn("Failed to disable debug point in cleanup: ${e.message}")
            }
            try {
                sql "STOP ROUTINE LOAD FOR ${jobName}"
            } catch (Exception e) {
                logger.warn("Failed to stop routine load in cleanup: ${e.message}")
            }
            producer.close()
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }
}
