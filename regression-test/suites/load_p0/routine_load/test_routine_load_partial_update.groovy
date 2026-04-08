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
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

suite("test_routine_load_partial_update", "nonConcurrent") {
    def kafkaCsvTopic = "test_routine_load_partial_update"

    if (RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        def runSql = { String q -> sql q }
        def kafka_broker = RoutineLoadTestUtils.getKafkaBroker(context)
        def producer = RoutineLoadTestUtils.createKafkaProducer(kafka_broker)

        def tableName = "test_routine_load_partial_update"
        def job = "test_partial_update_job"

        sql """ DROP TABLE IF EXISTS ${tableName} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `id` int NULL,
                `name` varchar(65533) NULL,
                `score` int NULL,
                `age` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            COMMENT 'test partial update'
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true"
            );
        """

        // insert initial data
        sql """
            INSERT INTO ${tableName} VALUES
            (1, 'alice', 100, 20),
            (2, 'bob', 90, 21),
            (3, 'charlie', 80, 22)
        """

        qt_select_initial "SELECT * FROM ${tableName} ORDER BY id"

        try {
            // create routine load with partial_columns=true
            // only update id and score columns
            sql """
                CREATE ROUTINE LOAD ${job} ON ${tableName}
                COLUMNS TERMINATED BY ",",
                COLUMNS (id, score)
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "partial_columns" = "true"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaCsvTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            // send partial update data to kafka
            // update score for id=1 from 100 to 150
            // update score for id=2 from 90 to 95
            def data = [
                "1,150",
                "2,95",
                "100,100"
            ]

            data.each { line ->
                logger.info("Sending to Kafka: ${line}")
                def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
                producer.send(record).get()
            }
            producer.flush()

            // wait for routine load task to finish
            RoutineLoadTestUtils.waitForTaskFinish(runSql, job, tableName, 3)

            // verify partial update: score should be updated, name and age should remain unchanged
            qt_select_after_partial_update "SELECT * FROM ${tableName} ORDER BY id"
        } catch (Exception e) {
            logger.error("Error during test: " + e.getMessage())
            throw e
        } finally {
            sql "STOP ROUTINE LOAD FOR ${job}"
        }
    }
}
