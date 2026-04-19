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

// 语义说明：
// Routine Load 因 strict_mode/schema mismatch 暂停后，RESUME 不会回放历史坏数据，
// 只会继续消费恢复后追加的新数据。
suite("test_routine_load_resume_without_replay_after_schema_pause", "docker") {
    def topicSuffix = System.currentTimeMillis()
    def kafkaCsvTopic = "test_rl_resume_no_replay_${topicSuffix}"

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

        def tableName = "test_rl_resume_no_replay"
        def job = "test_rl_resume_no_replay_job_${topicSuffix}"

        def schemaRejectedData = [
            "1,aaa,2023-01-01,val1,2023-01-01 10:00:00,abcdef",
            "2,bbb,2023-01-02,val2,2023-01-02 11:00:00,ghijkl"
        ]
        def postResumeData = [
            "3,ccc,2023-01-03,val3,2023-01-03 12:00:00,hello1",
            "4,ddd,2023-01-04,val4,2023-01-04 13:00:00,world2"
        ]

        def waitForPaused = { int maxWait = 60 ->
            def waited = 0
            while (waited < maxWait) {
                def res = sql "SHOW ROUTINE LOAD FOR ${job}"
                def state = res[0][8].toString()
                if (state == "PAUSED") {
                    logger.info("Job paused as expected. reason={}", res[0][17].toString())
                    return
                }
                sleep(1000)
                waited++
            }
            Assert.fail("Routine load job did not enter PAUSED within timeout")
        }

        def waitForSchemaChangeFinished = { int maxWait = 120 ->
            def waited = 0
            while (waited < maxWait) {
                def alterRes = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1"
                if (alterRes.size() > 0 && alterRes[0][9].toString() == "FINISHED") {
                    return
                }
                sleep(1000)
                waited++
            }
            Assert.fail("Schema change did not finish within timeout")
        }

        def waitForExactRowCount = { long expected, int maxWait = 120 ->
            def waited = 0
            while (waited < maxWait) {
                def jobRes = sql "SHOW ROUTINE LOAD FOR ${job}"
                def state = jobRes[0][8].toString()
                def countRes = sql "SELECT count(*) FROM ${tableName}"
                long rowCount = (countRes[0][0] as Number).longValue()
                logger.info("waitForExactRowCount: state={}, rows={}", state, rowCount)
                if (state == "RUNNING" && rowCount == expected) {
                    return
                }
                sleep(1000)
                waited++
            }
            Assert.fail("Row count did not reach ${expected} within timeout")
        }

        def waitForRunning = { int maxWait = 60 ->
            def waited = 0
            while (waited < maxWait) {
                def jobRes = sql "SHOW ROUTINE LOAD FOR ${job}"
                def state = jobRes[0][8].toString()
                if (state == "RUNNING") {
                    return
                }
                sleep(1000)
                waited++
            }
            Assert.fail("Routine load job did not enter RUNNING within timeout")
        }

        def assertRowCountKeeps = { long expected, int observeSeconds = 5 ->
            for (int i = 0; i < observeSeconds; i++) {
                def countRes = sql "SELECT count(*) FROM ${tableName}"
                long rowCount = (countRes[0][0] as Number).longValue()
                logger.info("assertRowCountKeeps: second={}, rows={}", i, rowCount)
                Assert.assertEquals("Observed row count should stay at ${expected}", expected, rowCount)
                sleep(1000)
            }
        }

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` int(20) NULL,
                `k2` string NULL,
                `v1` date NULL,
                `v2` string NULL,
                `v3` datetime NULL,
                `v4` varchar(5) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        sql "sync"

        schemaRejectedData.each { line ->
            logger.info("Sending schema-blocked row to Kafka: {}", line)
            producer.send(new ProducerRecord<>(kafkaCsvTopic, null, line)).get()
        }
        producer.flush()

        try {
            sql """
                CREATE ROUTINE LOAD ${job} ON ${tableName}
                COLUMNS TERMINATED BY ","
                PROPERTIES
                (
                    "strict_mode" = "true",
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafkaBroker}",
                    "kafka_topic" = "${kafkaCsvTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"

            waitForPaused()

            def pausedCount = sql "SELECT count(*) FROM ${tableName}"
            Assert.assertEquals("PAUSED 期间不应写入任何行", 0L, (pausedCount[0][0] as Number).longValue())

            sql "ALTER TABLE ${tableName} MODIFY COLUMN v4 VARCHAR(10)"
            waitForSchemaChangeFinished()

            sql "RESUME ROUTINE LOAD FOR ${job}"
            waitForRunning()
            assertRowCountKeeps(0L)

            // schema 放宽并 RESUME 后，同一个 job 不会补消费旧坏数据，
            // 但应继续消费恢复后追加的新数据。
            postResumeData.each { line ->
                logger.info("Sending post-resume row to Kafka: {}", line)
                producer.send(new ProducerRecord<>(kafkaCsvTopic, null, line)).get()
            }
            producer.flush()
            waitForExactRowCount(2L)

            def finalRows = sql "SELECT k1, k2, v4 FROM ${tableName} ORDER BY k1"
            Assert.assertEquals("resume 后应只消费 schema change 之后追加的 2 行", 2, finalRows.size())
            Assert.assertEquals(3, finalRows[0][0])
            Assert.assertEquals("ccc", finalRows[0][1])
            Assert.assertEquals("hello1", finalRows[0][2])
            Assert.assertEquals(4, finalRows[1][0])
            Assert.assertEquals("ddd", finalRows[1][1])
            Assert.assertEquals("world2", finalRows[1][2])

            def oldRows = sql "SELECT count(*) FROM ${tableName} WHERE k1 IN (1, 2)"
            Assert.assertEquals("resume 后不应回放历史 schema-failed 数据", 0L, (oldRows[0][0] as Number).longValue())

            def sumKey = sql "SELECT sum(k1) FROM ${tableName}"
            Assert.assertEquals(7L, (sumKey[0][0] as Number).longValue())

            def finalState = sql "SHOW ROUTINE LOAD FOR ${job}"
            Assert.assertEquals("RUNNING", finalState[0][8].toString())
        } finally {
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
