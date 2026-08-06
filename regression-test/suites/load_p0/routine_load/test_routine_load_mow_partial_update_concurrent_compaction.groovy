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

import groovy.json.JsonSlurper
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.RoutineLoadTestUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.Assert

suite("test_routine_load_mow_partial_update_concurrent_compaction", "docker") {
    def topicSuffix = System.currentTimeMillis()
    def kafkaCsvTopic = "test_rl_mow_pu_concurrent_compaction_${topicSuffix}"

    if (!RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        return
    }

    def kafkaBroker = RoutineLoadTestUtils.getKafkaBroker(context)

    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = false
    options.enableDebugPoints()

    docker(options) {
        def producer = RoutineLoadTestUtils.createKafkaProducer(kafkaBroker)

        def tableName = "test_rl_mow_pu_concurrent_compaction"
        def job = "test_mow_pu_concurrent_compaction_job_${topicSuffix}"
        def compactionDebugPoint = "FullCompaction.modify_rowsets.sleep"

        def waitForLoadedRows = { long expectedLoadedRows, int maxWait = 120 ->
            def waited = 0
            while (waited < maxWait) {
                def res = sql "SHOW ROUTINE LOAD FOR ${job}"
                def state = res[0][8].toString()
                def statJson = new JsonSlurper().parseText(res[0][14].toString())
                long loaded = statJson.loadedRows as long
                logger.info("waitForLoadedRows: state={}, loadedRows={}", state, loaded)
                if (state == "RUNNING" && loaded >= expectedLoadedRows) {
                    return
                }
                sleep(1000)
                waited++
            }
            Assert.fail("loadedRows did not reach ${expectedLoadedRows} within timeout")
        }

        sql """ DROP TABLE IF EXISTS ${tableName} force """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `k` int NOT NULL,
                `c1` int,
                `c2` int,
                `c3` int
            ) ENGINE=OLAP
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true"
            );
        """

        sql """
            INSERT INTO ${tableName} VALUES
            (1, 1, 11, 111),
            (2, 2, 22, 222),
            (3, 3, 33, 333)
        """
        sql "sync"

        def batch1 = [
            "1,10,110",
            "2,20,220"
        ]
        def batch2 = [
            "1,11,1110",
            "3,30,330"
        ]
        def batch3 = [
            "2,21,2210",
            "3,31,3310"
        ]

        def compactionError = null
        Thread compactionThread = null

        try {
            sql """
                CREATE ROUTINE LOAD ${job} ON ${tableName}
                COLUMNS TERMINATED BY ",",
                COLUMNS (k, c1, c2)
                PROPERTIES
                (
                    "max_batch_interval" = "1",
                    "partial_columns" = "true"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafkaBroker}",
                    "kafka_topic" = "${kafkaCsvTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            batch1.each { line ->
                producer.send(new ProducerRecord<>(kafkaCsvTopic, null, line)).get()
            }
            producer.flush()
            waitForLoadedRows(2L)

            GetDebugPoint().enableDebugPointForAllBEs(compactionDebugPoint)
            compactionThread = Thread.start {
                try {
                    trigger_and_wait_compaction(tableName, "full")
                } catch (Throwable t) {
                    compactionError = t
                }
            }

            sleep(2000)

            batch2.each { line ->
                producer.send(new ProducerRecord<>(kafkaCsvTopic, null, line)).get()
            }
            producer.flush()

            sleep(1500)

            batch3.each { line ->
                producer.send(new ProducerRecord<>(kafkaCsvTopic, null, line)).get()
            }
            producer.flush()

            waitForLoadedRows(6L, 180)

            compactionThread.join(120000)
            Assert.assertFalse("Compaction thread should finish within timeout", compactionThread.isAlive())
            if (compactionError != null) {
                throw compactionError
            }

            def finalRows = sql "SELECT k, c1, c2, c3 FROM ${tableName} ORDER BY k"
            Assert.assertEquals(3, finalRows.size())
            Assert.assertEquals(1, finalRows[0][0])
            Assert.assertEquals(11, finalRows[0][1])
            Assert.assertEquals(1110, finalRows[0][2])
            Assert.assertEquals(111, finalRows[0][3])

            Assert.assertEquals(2, finalRows[1][0])
            Assert.assertEquals(21, finalRows[1][1])
            Assert.assertEquals(2210, finalRows[1][2])
            Assert.assertEquals(222, finalRows[1][3])

            Assert.assertEquals(3, finalRows[2][0])
            Assert.assertEquals(31, finalRows[2][1])
            Assert.assertEquals(3310, finalRows[2][2])
            Assert.assertEquals(333, finalRows[2][3])

            def finalCount = sql "SELECT count(*), count(distinct k), sum(k) FROM ${tableName}"
            Assert.assertEquals(3L, (finalCount[0][0] as Number).longValue())
            Assert.assertEquals(3L, (finalCount[0][1] as Number).longValue())
            Assert.assertEquals(6L, (finalCount[0][2] as Number).longValue())
        } finally {
            try {
                GetDebugPoint().disableDebugPointForAllBEs(compactionDebugPoint)
            } catch (Exception e) {
                logger.warn("Failed to disable compaction debug point: {}", e.message)
            }
            try {
                sql "STOP ROUTINE LOAD FOR ${job}"
            } catch (Exception e) {
                logger.warn("Failed to stop routine load: {}", e.message)
            }
            producer.close()
            sql "DROP TABLE IF EXISTS ${tableName} force"
        }
    }
}
