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
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.Assert

suite("test_routine_load_same_job_offset_recovery_exactly_once", "docker") {
    def topicSuffix = System.currentTimeMillis()
    def kafkaCsvTopic = "test_rl_same_job_offset_recovery_${topicSuffix}"

    if (!RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        return
    }

    def kafkaBroker = RoutineLoadTestUtils.getKafkaBroker(context)

    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = false

    docker(options) {
        def producer = RoutineLoadTestUtils.createKafkaProducer(kafkaBroker)

        def adminProps = new Properties()
        adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker)
        def adminClient = AdminClient.create(adminProps)

        def tableName = "test_rl_same_job_offset_recovery"
        def job = "test_rl_same_job_offset_recovery_job_${topicSuffix}"

        def firstBatch = [
            "1,payload_1,2026-03-01",
            "2,payload_2,2026-03-02"
        ]
        def secondBatch = [
            "3,payload_3,2026-03-03",
            "4,payload_4,2026-03-04",
            "5,payload_5,2026-03-05"
        ]

        def waitForExactRowCount = { long expected, int maxWait = 120 ->
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

        def waitForPausedByOffsetError = { int maxWait = 120 ->
            def waited = 0
            while (waited < maxWait) {
                def res = sql "SHOW ROUTINE LOAD FOR ${job}"
                def state = res[0][8].toString()
                def reason = res[0][17].toString()
                def otherMsg = res[0][19].toString()
                def combinedMsg = "${reason} ${otherMsg}".toLowerCase()
                logger.info("waitForPausedByOffsetError: state={}, reason={}, msg={}", state, reason, otherMsg)
                if (state == "PAUSED" && (
                    combinedMsg.contains("offset out of range")
                    || combinedMsg.contains("greater than kafka latest offset")
                )) {
                    return
                }
                sleep(1000)
                waited++
            }
            Assert.fail("Routine load job did not pause on bad offset within timeout")
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
            adminClient.createTopics([new NewTopic(kafkaCsvTopic, 1, (short) 1)]).all().get()

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
                    "kafka_partitions" = "0",
                    "kafka_offsets" = "OFFSET_BEGINNING"
                );
            """

            firstBatch.each { line ->
                producer.send(new ProducerRecord<>(kafkaCsvTopic, 0, null, line)).get()
            }
            producer.flush()
            waitForExactRowCount(2L)

            sql "PAUSE ROUTINE LOAD FOR ${job}"
            sql "ALTER ROUTINE LOAD FOR ${job} FROM KAFKA(\"kafka_partitions\" = \"0\", \"kafka_offsets\" = \"999999\");"
            sql "RESUME ROUTINE LOAD FOR ${job}"

            waitForPausedByOffsetError()

            def pausedCount = sql "SELECT count(*) FROM ${tableName}"
            Assert.assertEquals(2L, (pausedCount[0][0] as Number).longValue())

            sql "ALTER ROUTINE LOAD FOR ${job} FROM KAFKA(\"kafka_partitions\" = \"0\", \"kafka_offsets\" = \"2\");"
            secondBatch.each { line ->
                producer.send(new ProducerRecord<>(kafkaCsvTopic, 0, null, line)).get()
            }
            producer.flush()
            sql "RESUME ROUTINE LOAD FOR ${job}"

            waitForExactRowCount(5L)

            def finalRows = sql "SELECT id, payload FROM ${tableName} ORDER BY id"
            Assert.assertEquals(5, finalRows.size())
            for (int i = 0; i < 5; i++) {
                Assert.assertEquals(i + 1, finalRows[i][0])
                Assert.assertEquals("payload_${i + 1}".toString(), finalRows[i][1])
            }

            def distinctCount = sql "SELECT count(distinct id) FROM ${tableName}"
            Assert.assertEquals(5L, (distinctCount[0][0] as Number).longValue())

            def sumId = sql "SELECT sum(id) FROM ${tableName}"
            Assert.assertEquals(15L, (sumId[0][0] as Number).longValue())

            def finalState = sql "SHOW ROUTINE LOAD FOR ${job}"
            Assert.assertEquals("RUNNING", finalState[0][8].toString())
        } finally {
            try {
                sql "STOP ROUTINE LOAD FOR ${job}"
            } catch (Exception e) {
                logger.warn("Failed to stop routine load: {}", e.message)
            }
            producer.close()
            adminClient.close()
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }
}
