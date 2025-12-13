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

import org.apache.doris.regression.util.RoutineLoadTestUtils
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig

suite("test_routine_load_adaptive_param","nonConcurrent") {
    def kafkaCsvTpoics = [
                  "test_routine_load_adaptive_param",
                ]

    if (RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        def runSql = { String q -> sql q }
        def kafka_broker = RoutineLoadTestUtils.getKafkaBroker(context)
        def producer = RoutineLoadTestUtils.createKafkaProducer(kafka_broker)

        def tableName = "test_routine_load_adaptive_param"
        def job = "test_adaptive_param"
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
                    "kafka_topic" = "${kafkaCsvTpoics[0]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            def injection = "RoutineLoadTaskInfo.judgeEof"
            try {
                GetDebugPoint().enableDebugPointForAllFEs(injection)
                RoutineLoadTestUtils.sendTestDataToKafka(producer, kafkaCsvTpoics)
                RoutineLoadTestUtils.waitForTaskFinish(runSql, job, tableName, 0)
            } finally {
                GetDebugPoint().disableDebugPointForAllFEs(injection)
            }
            // test adaptively increase
            logger.info("---test adaptively increase---")
            RoutineLoadTestUtils.sendTestDataToKafka(producer, kafkaCsvTpoics)
            RoutineLoadTestUtils.checkTaskTimeout(runSql, job, "3600")
            RoutineLoadTestUtils.waitForTaskFinish(runSql, job, tableName, 2)

            // test restore adaptively
            logger.info("---test restore adaptively---")
            RoutineLoadTestUtils.sendTestDataToKafka(producer, kafkaCsvTpoics)
            RoutineLoadTestUtils.waitForTaskFinish(runSql, job, tableName, 4)
            RoutineLoadTestUtils.checkTaskTimeout(runSql, job, "100")
        } finally {
            sql "stop routine load for ${job}"
        }
    }
}
