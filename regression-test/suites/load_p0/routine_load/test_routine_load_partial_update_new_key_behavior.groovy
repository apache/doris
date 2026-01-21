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

suite("test_routine_load_partial_update_new_key_behavior", "nonConcurrent") {
    def kafkaCsvTopic = "test_routine_load_partial_update_new_key_behavior"

    if (RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        def runSql = { String q -> sql q }
        def kafka_broker = RoutineLoadTestUtils.getKafkaBroker(context)
        def producer = RoutineLoadTestUtils.createKafkaProducer(kafka_broker)

        // Test 1: partial_update_new_key_behavior=APPEND (default)
        def tableName1 = "test_routine_load_pu_new_key_append"
        def job1 = "test_new_key_behavior_append"

        sql """ DROP TABLE IF EXISTS ${tableName1} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName1} (
                `k` int NOT NULL,
                `c1` int,
                `c2` int,
                `c3` int
            ) ENGINE=OLAP
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true"
            );
        """

        sql """
            INSERT INTO ${tableName1} VALUES
            (1, 1, 1, 1),
            (2, 2, 2, 2),
            (3, 3, 3, 3)
        """

        qt_select_initial "SELECT * FROM ${tableName1} ORDER BY k"
        try {
            sql """
                CREATE ROUTINE LOAD ${job1} ON ${tableName1}
                COLUMNS TERMINATED BY ",",
                COLUMNS (k, c1)
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "partial_columns" = "true",
                    "partial_update_new_key_behavior" = "append"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaCsvTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            // send data with existing keys and new keys
            def data1 = [
                "1,10",  // update existing key
                "2,20",  // update existing key
                "4,40",  // new key - should be appended with default values for c2 and c3
                "5,50"   // new key - should be appended with default values for c2 and c3
            ]

            data1.each { line ->
                logger.info("Sending to Kafka: ${line}")
                def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
                producer.send(record).get()
            }
            producer.flush()

            // wait for routine load task to finish
            sql "set skip_delete_bitmap=true;"
            sql "sync;"
            RoutineLoadTestUtils.waitForTaskFinish(runSql, job1, tableName1, 6)
            sql "set skip_delete_bitmap=false;"
            sql "sync;"

            // verify: new keys should be appended
            qt_select_after_append "SELECT * FROM ${tableName1} ORDER BY k"

        } catch (Exception e) {
            logger.info("Caught expected exception: ${e.getMessage()}")
            throw e
        } finally {
            sql "STOP ROUTINE LOAD FOR ${job1}"
        }

        // Test 2: partial_update_new_key_behavior=ERROR
        def tableName2 = "test_routine_load_pu_new_key_error"
        def job2 = "test_new_key_behavior_error"

        sql """ DROP TABLE IF EXISTS ${tableName2} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName2} (
                `k` int NOT NULL,
                `c1` int,
                `c2` int,
                `c3` int
            ) ENGINE=OLAP
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true"
            );
        """

        sql """
            INSERT INTO ${tableName2} VALUES
            (1, 1, 1, 1),
            (2, 2, 2, 2),
            (3, 3, 3, 3),
            (4, 4, 4, 4),
            (5, 5, 5, 5)
        """
        try {
            sql """
                CREATE ROUTINE LOAD ${job2} ON ${tableName2}
                COLUMNS TERMINATED BY ",",
                COLUMNS (k, c2)
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "partial_columns" = "true",
                    "partial_update_new_key_behavior" = "error"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaCsvTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            // send data with only existing keys (should succeed)
            def data2 = [
                "1,100",
                "2,200"
            ]

            data2.each { line ->
                logger.info("Sending to Kafka: ${line}")
                def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
                producer.send(record).get()
            }
            producer.flush()

            // wait for routine load task to finish
            sql "set skip_delete_bitmap=true;"
            sql "sync;"
            RoutineLoadTestUtils.waitForTaskFinish(runSql, job2, tableName2, 6)
            sql "set skip_delete_bitmap=false;"
            sql "sync;"

            // verify: existing keys should be updated
            qt_select_after_error_mode "SELECT * FROM ${tableName2} ORDER BY k"

            // Now send data with new keys - this should fail the task
            def data3 = [
                "10,1000",  // new key - should cause error
                "11,1100"   // new key - should cause error
            ]

            data3.each { line ->
                logger.info("Sending to Kafka with new keys: ${line}")
                def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
                producer.send(record).get()
            }
            producer.flush()

            RoutineLoadTestUtils.waitForTaskAbort(runSql, job2)
            def state = sql "SHOW ROUTINE LOAD FOR ${job2}"
            logger.info("routine load state after new keys: ${state[0][8].toString()}")
            logger.info("routine load error rows: ${state[0][15].toString()}")

            // the data should not be loaded due to error
            qt_select_after_error_rejected "SELECT * FROM ${tableName2} ORDER BY k"

        } catch (Exception e) {
            logger.info("Caught expected exception: ${e.getMessage()}")
            throw e
        } finally {
            sql "STOP ROUTINE LOAD FOR ${job2}"
        }

        // Test 3: Test invalid property value
        def tableName3 = "test_routine_load_pu_invalid_prop"
        sql """ DROP TABLE IF EXISTS ${tableName3} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName3} (
                `k` int NOT NULL,
                `c1` int,
                `c2` int,
                `c3` int
            ) ENGINE=OLAP
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true"
            );
        """

        test {
            sql """
                CREATE ROUTINE LOAD test_invalid_property ON ${tableName3}
                COLUMNS TERMINATED BY ",",
                COLUMNS (k, c3)
                PROPERTIES
                (
                    "partial_columns" = "true",
                    "partial_update_new_key_behavior" = "invalid"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaCsvTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            exception "partial_update_new_key_behavior should be one of {'APPEND', 'ERROR'}"
        }

        // Test 4: Test setting property without partial_columns
        def tableName4 = "test_routine_load_pu_without_partial"
        sql """ DROP TABLE IF EXISTS ${tableName4} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName4} (
                `k` int NOT NULL,
                `c1` int,
                `c2` int,
                `c3` int
            ) ENGINE=OLAP
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true"
            );
        """

        test {
            sql """
                CREATE ROUTINE LOAD test_without_partial_columns ON ${tableName4}
                COLUMNS TERMINATED BY ",",
                COLUMNS (k, c3)
                PROPERTIES
                (
                    "partial_update_new_key_behavior" = "append"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaCsvTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            exception "partial_update_new_key_behavior can only be set when partial update is enabled"
        }
    }
}
