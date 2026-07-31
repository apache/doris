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
// KIND, either express or implied.  See the License for
// the specific language governing permissions and limitations
// under the License.

import org.apache.doris.regression.util.RoutineLoadTestUtils
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig

suite("test_routine_load_be_restart","nonConcurrent") {
    def kafkaCsvTopics = [
                  "test_routine_load_be_restart",
                ]

    if (RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        def runSql = { String q -> sql q }
        def kafka_broker = RoutineLoadTestUtils.getKafkaBroker(context)
        def producer = RoutineLoadTestUtils.createKafkaProducer(kafka_broker)

        def tableName = "test_routine_load_be_restart"
        def job = "test_routine_load_be_restart"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` int(20) NULL,
                `k2` string NULL,
                `v1` date  NULL,
                `v2` string  NULL,
                `v3` datetime  NULL,
                `v4` string  NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """

        try {
            sql """
                CREATE ROUTINE LOAD ${job} ON ${tableName}
                COLUMNS TERMINATED BY ","
                PROPERTIES
                (
                    "max_batch_interval" = "10"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaCsvTopics[0]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            def injection_load_hang = "load.commit_timeout"
            def injection_abort_txn = "FE.abortTxnWhenCoordinateBeRestart"
            // test coordinator be restart
            try {
                GetDebugPoint().enableDebugPointForAllFEs(injection_load_hang)
                RoutineLoadTestUtils.sendTestDataToKafka(producer, kafkaCsvTopics)
                GetDebugPoint().enableDebugPointForAllFEs(injection_abort_txn)
                RoutineLoadTestUtils.waitForTaskAbort(runSql, job, 60)
                def pausedJob = sql "show routine load for ${job}"
                assertEquals("PAUSED", pausedJob[0][8].toString())
                assertTrue(pausedJob[0][17].toString().contains("TASKS_ABORT_ERR"))
            } finally {
                GetDebugPoint().disableDebugPointForAllFEs(injection_abort_txn)
                GetDebugPoint().disableDebugPointForAllFEs(injection_load_hang)
            }

            // After the simulated BE restart, auto resume must reschedule the paused job and load data.
            def count = RoutineLoadTestUtils.waitForTaskFinish(runSql, job, tableName, 0)
            logger.info("rows loaded after be-restart abort, wait iterations: " + count)

            // Verify the job recovered without a manual resume.
            def res = sql "show routine load for ${job}"
            assertEquals("RUNNING", res[0][8].toString())

            // Send additional data and confirm it is loaded to prove ongoing task scheduling
            // works correctly after the abort — this would time-out if the replacement task
            // was never enqueued due to the missing beforeAborted() call.
            def rowsAfterFirstLoad = sql("select count(*) from ${tableName}")[0][0] as int
            RoutineLoadTestUtils.sendTestDataToKafka(producer, kafkaCsvTopics)
            RoutineLoadTestUtils.waitForTaskFinish(runSql, job, tableName, rowsAfterFirstLoad)

            // test coordinator be down
            def injection_abort_txn_be_down = "HeartbeatMgr.abortTxnWhenCoordinateBeDown"
            try {
                GetDebugPoint().enableDebugPointForAllFEs(injection_load_hang)
                RoutineLoadTestUtils.sendTestDataToKafka(producer, kafkaCsvTopics)
                GetDebugPoint().enableDebugPointForAllFEs(injection_abort_txn_be_down)
                RoutineLoadTestUtils.waitForTaskAbort(runSql, job, 60, 2)
            } finally {
                GetDebugPoint().disableDebugPointForAllFEs(injection_abort_txn_be_down)
                GetDebugPoint().disableDebugPointForAllFEs(injection_load_hang)
            }
            def finalCount = RoutineLoadTestUtils.waitForTaskFinish(runSql, job, tableName, 0)
            logger.info("wait count: " + finalCount)
        } finally {
            sql "stop routine load for ${job}"
        }
    }
}
