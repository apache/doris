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

suite("test_routine_load_flexible_partial_update", "nonConcurrent") {

    if (RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        def runSql = { String q -> sql q }
        def kafka_broker = RoutineLoadTestUtils.getKafkaBroker(context)
        def producer = RoutineLoadTestUtils.createKafkaProducer(kafka_broker)

        // Test 1: Basic flexible partial update
        def kafkaJsonTopic1 = "test_routine_load_flexible_partial_update_basic"
        def tableName1 = "test_routine_load_flex_update_basic"
        def job1 = "test_flex_partial_update_job_basic"

        sql """ DROP TABLE IF EXISTS ${tableName1} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName1} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL,
                `age` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            COMMENT 'test flexible partial update'
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"
            );
        """

        // verify skip bitmap column is enabled
        def show_res = sql "show create table ${tableName1}"
        assertTrue(show_res.toString().contains('"enable_unique_key_skip_bitmap_column" = "true"'))

        // insert initial data
        sql """
            INSERT INTO ${tableName1} VALUES
            (1, 'alice', 100, 20),
            (2, 'bob', 90, 21),
            (3, 'charlie', 80, 22),
            (4, 'david', 70, 23),
            (5, 'eve', 60, 24)
        """

        qt_select_initial1 "SELECT id, name, score, age FROM ${tableName1} ORDER BY id"

        try {
            // create routine load with flexible partial update
            sql """
                CREATE ROUTINE LOAD ${job1} ON ${tableName1}
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json",
                    "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic1}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            // send JSON data with different columns per row
            // Row 1: update only score for id=1
            // Row 2: update only age for id=2
            // Row 3: update both name and score for id=3
            // Row 4: insert new row with only id and name
            def data = [
                '{"id": 1, "score": 150}',
                '{"id": 2, "age": 30}',
                '{"id": 3, "name": "chuck", "score": 95}',
                '{"id": 6, "name": "frank"}'
            ]

            data.each { line ->
                logger.info("Sending to Kafka: ${line}")
                def record = new ProducerRecord<>(kafkaJsonTopic1, null, line)
                producer.send(record).get()
            }
            producer.flush()

            // wait for routine load task to finish
            // With skip_delete_bitmap=true, count = initial + kafka_messages = 5 + 4 = 9
            RoutineLoadTestUtils.waitForTaskFinishMoW(runSql, job1, tableName1, 8)

            // verify flexible partial update results
            qt_select_after_flex_update1 "SELECT id, name, score, age FROM ${tableName1} ORDER BY id"
        } catch (Exception e) {
            logger.error("Error during test: " + e.getMessage())
            throw e
        } finally {
            sql "STOP ROUTINE LOAD FOR ${job1}"
        }

        // Test 2: Flexible partial update with default values
        def kafkaJsonTopic2 = "test_routine_load_flexible_partial_update_default"
        def tableName2 = "test_routine_load_flex_update_default"
        def job2 = "test_flex_partial_update_job_default"

        sql """ DROP TABLE IF EXISTS ${tableName2} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName2} (
                `id` int NOT NULL,
                `v1` bigint NULL,
                `v2` bigint NULL DEFAULT "9876",
                `v3` bigint NOT NULL,
                `v4` bigint NOT NULL DEFAULT "1234"
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            COMMENT 'test flexible partial update with default values'
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"
            );
        """

        // insert initial data
        sql """
            INSERT INTO ${tableName2} VALUES
            (1, 10, 20, 30, 40),
            (2, 100, 200, 300, 400),
            (3, 1000, 2000, 3000, 4000)
        """

        qt_select_initial2 "SELECT id, v1, v2, v3, v4 FROM ${tableName2} ORDER BY id"

        try {
            sql """
                CREATE ROUTINE LOAD ${job2} ON ${tableName2}
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json",
                    "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic2}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            // send JSON data with different columns per row
            def data2 = [
                '{"id": 1, "v1": 11}',
                '{"id": 2, "v2": 222, "v3": 333}',
                '{"id": 4, "v3": 4444}'
            ]

            data2.each { line ->
                logger.info("Sending to Kafka: ${line}")
                def record = new ProducerRecord<>(kafkaJsonTopic2, null, line)
                producer.send(record).get()
            }
            producer.flush()

            // With skip_delete_bitmap=true, count = initial + kafka_messages = 3 + 3 = 6
            RoutineLoadTestUtils.waitForTaskFinishMoW(runSql, job2, tableName2, 5)

            qt_select_after_flex_update2 "SELECT id, v1, v2, v3, v4 FROM ${tableName2} ORDER BY id"
        } catch (Exception e) {
            logger.error("Error during test: " + e.getMessage())
            throw e
        } finally {
            sql "STOP ROUTINE LOAD FOR ${job2}"
        }

        // Test 3: Error case - CSV format not supported
        def kafkaCsvTopic3 = "test_routine_load_flexible_partial_update_csv_error"
        def tableName3 = "test_routine_load_flex_update_csv_error"
        def job3 = "test_flex_partial_update_job_csv_error"

        sql """ DROP TABLE IF EXISTS ${tableName3} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName3} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"
            );
        """

        test {
            sql """
                CREATE ROUTINE LOAD ${job3} ON ${tableName3}
                COLUMNS TERMINATED BY ","
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaCsvTopic3}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            exception "Flexible partial update only supports JSON format"
        }

        // Test 4: Error case - jsonpaths not supported with flexible partial update
        def tableName4 = "test_routine_load_flex_update_jsonpaths_error"
        def job4 = "test_flex_partial_update_job_jsonpaths_error"

        sql """ DROP TABLE IF EXISTS ${tableName4} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName4} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"
            );
        """

        test {
            sql """
                CREATE ROUTINE LOAD ${job4} ON ${tableName4}
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json",
                    "jsonpaths" = '[\"\$.id\", \"\$.name\", \"\$.score\"]',
                    "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "test_topic",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            exception "Flexible partial update does not support jsonpaths"
        }

        // Test 5: Error case - fuzzy_parse not supported
        def kafkaJsonTopic5 = "test_routine_load_flexible_partial_update_fuzzy_error"
        def tableName5 = "test_routine_load_flex_update_fuzzy_error"
        def job5 = "test_flex_partial_update_job_fuzzy_error"

        sql """ DROP TABLE IF EXISTS ${tableName5} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName5} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"
            );
        """

        test {
            sql """
                CREATE ROUTINE LOAD ${job5} ON ${tableName5}
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json",
                    "fuzzy_parse" = "true",
                    "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic5}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            exception "Flexible partial update does not support fuzzy_parse"
        }

        // Test 6: Error case - COLUMNS clause not supported
        def kafkaJsonTopic6 = "test_routine_load_flexible_partial_update_columns_error"
        def tableName6 = "test_routine_load_flex_update_columns_error"
        def job6 = "test_flex_partial_update_job_columns_error"

        sql """ DROP TABLE IF EXISTS ${tableName6} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName6} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"
            );
        """

        test {
            sql """
                CREATE ROUTINE LOAD ${job6} ON ${tableName6}
                COLUMNS (id, name, score)
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json",
                    "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic6}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            exception "Flexible partial update does not support COLUMNS specification"
        }

        // Test 7: Success case - WHERE clause works with flexible partial update
        def kafkaJsonTopic7 = "test_routine_load_flexible_partial_update_where"
        def tableName7 = "test_routine_load_flex_update_where"
        def job7 = "test_flex_partial_update_job_where"

        sql """ DROP TABLE IF EXISTS ${tableName7} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName7} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL,
                `age` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"
            );
        """

        // insert initial data
        sql """
            INSERT INTO ${tableName7} VALUES
            (1, 'alice', 100, 20),
            (2, 'bob', 90, 21),
            (3, 'charlie', 80, 22)
        """

        qt_select_initial7 "SELECT id, name, score, age FROM ${tableName7} ORDER BY id"

        try {
            // create routine load with WHERE clause and flexible partial update
            sql """
                CREATE ROUTINE LOAD ${job7} ON ${tableName7}
                WHERE id > 1
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json",
                    "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic7}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            // send JSON data - WHERE clause filters id > 1, so id=1 row should NOT be processed
            def data7 = [
                '{"id": 1, "score": 999}',
                '{"id": 2, "score": 95}',
                '{"id": 3, "name": "chuck"}',
                '{"id": 4, "name": "diana", "score": 70}'
            ]

            data7.each { line ->
                logger.info("Sending to Kafka: ${line}")
                def record = new ProducerRecord<>(kafkaJsonTopic7, null, line)
                producer.send(record).get()
            }
            producer.flush()

            // With skip_delete_bitmap=true and WHERE id > 1:
            // - id=1: 1 version (not updated, filtered by WHERE)
            // - id=2: 2 versions (original + partial update)
            // - id=3: 2 versions (original + partial update)
            // - id=4: 1 version (new row)
            // Total: 6 rows, so expectedMinRows = 5 (waits for count > 5)
            RoutineLoadTestUtils.waitForTaskFinishMoW(runSql, job7, tableName7, 5)

            // verify: id=1 should NOT be updated (filtered by WHERE), id=2,3,4 should be updated
            qt_select_after_flex_where "SELECT id, name, score, age FROM ${tableName7} ORDER BY id"
        } catch (Exception e) {
            logger.error("Error during test: " + e.getMessage())
            throw e
        } finally {
            sql "STOP ROUTINE LOAD FOR ${job7}"
        }

        // Test 8: Error case - table without skip_bitmap column
        def kafkaJsonTopic8 = "test_routine_load_flexible_partial_update_no_skip_bitmap"
        def tableName8 = "test_routine_load_flex_update_no_skip_bitmap"
        def job8 = "test_flex_partial_update_job_no_skip_bitmap"

        sql """ DROP TABLE IF EXISTS ${tableName8} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName8} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "enable_unique_key_skip_bitmap_column" = "false"
            );
        """

        test {
            sql """
                CREATE ROUTINE LOAD ${job8} ON ${tableName8}
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json",
                    "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic8}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            exception "Flexible partial update can only support table with skip bitmap hidden column"
        }

        // Test 9: Error case - table with variant column
        def kafkaJsonTopic9 = "test_routine_load_flexible_partial_update_variant"
        def tableName9 = "test_routine_load_flex_update_variant"
        def job9 = "test_flex_partial_update_job_variant"

        sql """ DROP TABLE IF EXISTS ${tableName9} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName9} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `data` variant NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"
            );
        """

        test {
            sql """
                CREATE ROUTINE LOAD ${job9} ON ${tableName9}
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json",
                    "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic9}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            exception "Flexible partial update can only support table without variant columns"
        }

        // Test 10: Error case - invalid unique_key_update_mode value
        def kafkaJsonTopic10 = "test_routine_load_flexible_partial_update_invalid_mode"
        def tableName10 = "test_routine_load_flex_update_invalid_mode"
        def job10 = "test_flex_partial_update_job_invalid_mode"

        sql """ DROP TABLE IF EXISTS ${tableName10} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName10} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"
            );
        """

        test {
            sql """
                CREATE ROUTINE LOAD ${job10} ON ${tableName10}
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json",
                    "unique_key_update_mode" = "INVALID_MODE"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic10}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            exception "unique_key_update_mode should be one of"
        }

        // Test 11: UPDATE_FIXED_COLUMNS mode (backward compatibility)
        def kafkaJsonTopic11 = "test_routine_load_fixed_columns_mode"
        def tableName11 = "test_routine_load_fixed_columns_mode"
        def job11 = "test_fixed_columns_mode_job"

        sql """ DROP TABLE IF EXISTS ${tableName11} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName11} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL,
                `age` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true"
            );
        """

        // insert initial data
        sql """
            INSERT INTO ${tableName11} VALUES
            (1, 'alice', 100, 20),
            (2, 'bob', 90, 21),
            (3, 'charlie', 80, 22)
        """

        qt_select_initial11 "SELECT id, name, score, age FROM ${tableName11} ORDER BY id"

        try {
            // create routine load with UPDATE_FIXED_COLUMNS mode
            sql """
                CREATE ROUTINE LOAD ${job11} ON ${tableName11}
                COLUMNS (id, score)
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json",
                    "unique_key_update_mode" = "UPDATE_FIXED_COLUMNS"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic11}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            def data11 = [
                '{"id": 1, "score": 150}',
                '{"id": 2, "score": 95}',
                '{"id": 4, "score": 85}'
            ]

            data11.each { line ->
                logger.info("Sending to Kafka: ${line}")
                def record = new ProducerRecord<>(kafkaJsonTopic11, null, line)
                producer.send(record).get()
            }
            producer.flush()

            // With skip_delete_bitmap=true, count = initial + kafka_messages = 3 + 3 = 6
            RoutineLoadTestUtils.waitForTaskFinishMoW(runSql, job11, tableName11, 5)

            qt_select_after_fixed_update "SELECT id, name, score, age FROM ${tableName11} ORDER BY id"
        } catch (Exception e) {
            logger.error("Error during test: " + e.getMessage())
            throw e
        } finally {
            sql "STOP ROUTINE LOAD FOR ${job11}"
        }

        // Test 12: ALTER ROUTINE LOAD to change unique_key_update_mode
        def kafkaJsonTopic12 = "test_routine_load_alter_flex_mode"
        def tableName12 = "test_routine_load_alter_flex_mode"
        def job12 = "test_alter_flex_mode_job"

        sql """ DROP TABLE IF EXISTS ${tableName12} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName12} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL,
                `age` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"
            );
        """

        // insert initial data
        sql """
            INSERT INTO ${tableName12} VALUES
            (1, 'alice', 100, 20),
            (2, 'bob', 90, 21),
            (3, 'charlie', 80, 22)
        """

        qt_select_initial12 "SELECT id, name, score, age FROM ${tableName12} ORDER BY id"

        try {
            // create routine load with UPSERT mode (default)
            sql """
                CREATE ROUTINE LOAD ${job12} ON ${tableName12}
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic12}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            // pause the job before altering
            sql "PAUSE ROUTINE LOAD FOR ${job12}"

            // alter to UPDATE_FLEXIBLE_COLUMNS mode
            sql """
                ALTER ROUTINE LOAD FOR ${job12}
                PROPERTIES
                (
                    "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                );
            """

            // verify the property was changed
            def res = sql "SHOW ROUTINE LOAD FOR ${job12}"
            def jobProperties = res[0][11].toString()
            logger.info("Altered routine load job properties: ${jobProperties}")
            assertTrue(jobProperties.contains("UPDATE_FLEXIBLE_COLUMNS"))

            // resume the job
            sql "RESUME ROUTINE LOAD FOR ${job12}"

            // send JSON data with different columns per row
            def data12 = [
                '{"id": 1, "score": 200}',
                '{"id": 2, "age": 35}',
                '{"id": 4, "name": "diana"}'
            ]

            data12.each { line ->
                logger.info("Sending to Kafka: ${line}")
                def record = new ProducerRecord<>(kafkaJsonTopic12, null, line)
                producer.send(record).get()
            }
            producer.flush()

            // With skip_delete_bitmap=true, count = initial + kafka_messages = 3 + 3 = 6
            RoutineLoadTestUtils.waitForTaskFinishMoW(runSql, job12, tableName12, 5)

            // verify flexible partial update results after alter
            qt_select_after_alter_flex "SELECT id, name, score, age FROM ${tableName12} ORDER BY id"
        } catch (Exception e) {
            logger.error("Error during test: " + e.getMessage())
            throw e
        } finally {
            sql "STOP ROUTINE LOAD FOR ${job12}"
        }

        // Test 13: ALTER ROUTINE LOAD - error when trying to change to flex mode with invalid settings
        def kafkaJsonTopic13 = "test_routine_load_alter_flex_error"
        def tableName13 = "test_routine_load_alter_flex_error"
        def job13 = "test_alter_flex_error_job"

        sql """ DROP TABLE IF EXISTS ${tableName13} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName13} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "enable_unique_key_skip_bitmap_column" = "false"
            );
        """

        try {
            // create routine load with UPSERT mode (default)
            sql """
                CREATE ROUTINE LOAD ${job13} ON ${tableName13}
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic13}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            // pause the job before altering
            sql "PAUSE ROUTINE LOAD FOR ${job13}"

            // try to alter to UPDATE_FLEXIBLE_COLUMNS mode - should fail because table doesn't have skip_bitmap
            test {
                sql """
                    ALTER ROUTINE LOAD FOR ${job13}
                    PROPERTIES
                    (
                        "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                    );
                """
                exception "Flexible partial update can only support table with skip bitmap hidden column"
            }
        } catch (Exception e) {
            logger.error("Error during test: " + e.getMessage())
            throw e
        } finally {
            sql "STOP ROUTINE LOAD FOR ${job13}"
        }

        // Test 14: ALTER to flex mode fails when using CSV format
        def kafkaJsonTopic14 = "test_routine_load_alter_flex_csv_error"
        def tableName14 = "test_routine_load_alter_flex_csv_error"
        def job14 = "test_alter_flex_csv_error_job"

        sql """ DROP TABLE IF EXISTS ${tableName14} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName14} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"
            );
        """

        try {
            // create routine load with CSV format
            sql """
                CREATE ROUTINE LOAD ${job14} ON ${tableName14}
                COLUMNS TERMINATED BY ","
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "csv"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic14}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            sql "PAUSE ROUTINE LOAD FOR ${job14}"

            // try to alter to UPDATE_FLEXIBLE_COLUMNS mode - should fail because CSV format
            test {
                sql """
                    ALTER ROUTINE LOAD FOR ${job14}
                    PROPERTIES
                    (
                        "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                    );
                """
                exception "Flexible partial update only supports JSON format"
            }
        } catch (Exception e) {
            logger.error("Error during test: " + e.getMessage())
            throw e
        } finally {
            sql "STOP ROUTINE LOAD FOR ${job14}"
        }

        // Test 15: ALTER to flex mode fails when using fuzzy_parse
        def kafkaJsonTopic15 = "test_routine_load_alter_flex_fuzzy_error"
        def tableName15 = "test_routine_load_alter_flex_fuzzy_error"
        def job15 = "test_alter_flex_fuzzy_error_job"

        sql """ DROP TABLE IF EXISTS ${tableName15} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName15} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"
            );
        """

        try {
            // create routine load with fuzzy_parse enabled
            sql """
                CREATE ROUTINE LOAD ${job15} ON ${tableName15}
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json",
                    "fuzzy_parse" = "true"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic15}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            sql "PAUSE ROUTINE LOAD FOR ${job15}"

            // try to alter to UPDATE_FLEXIBLE_COLUMNS mode - should fail because fuzzy_parse
            test {
                sql """
                    ALTER ROUTINE LOAD FOR ${job15}
                    PROPERTIES
                    (
                        "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                    );
                """
                exception "Flexible partial update does not support fuzzy_parse"
            }
        } catch (Exception e) {
            logger.error("Error during test: " + e.getMessage())
            throw e
        } finally {
            sql "STOP ROUTINE LOAD FOR ${job15}"
        }

        // Test 16: ALTER to flex mode fails with jsonpaths
        def kafkaJsonTopic16 = "test_routine_load_alter_flex_jsonpaths_error"
        def tableName16 = "test_routine_load_alter_flex_jsonpaths_error"
        def job16 = "test_alter_flex_jsonpaths_error_job"

        sql """ DROP TABLE IF EXISTS ${tableName16} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName16} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"
            );
        """

        try {
            // create routine load with jsonpaths (UPSERT mode)
            sql """
                CREATE ROUTINE LOAD ${job16} ON ${tableName16}
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json",
                    "jsonpaths" = '[\"\$.id\", \"\$.name\", \"\$.score\"]'
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic16}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            sql "PAUSE ROUTINE LOAD FOR ${job16}"

            // alter to UPDATE_FLEXIBLE_COLUMNS mode - should fail because jsonpaths is set
            test {
                sql """
                    ALTER ROUTINE LOAD FOR ${job16}
                    PROPERTIES
                    (
                        "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                    );
                """
                exception "Flexible partial update does not support jsonpaths"
            }
        } catch (Exception e) {
            logger.error("Error during test: " + e.getMessage())
            throw e
        } finally {
            sql "STOP ROUTINE LOAD FOR ${job16}"
        }

        // Test 17: ALTER to flex mode fails when COLUMNS clause is specified
        def kafkaJsonTopic17 = "test_routine_load_alter_flex_columns_error"
        def tableName17 = "test_routine_load_alter_flex_columns_error"
        def job17 = "test_alter_flex_columns_error_job"

        sql """ DROP TABLE IF EXISTS ${tableName17} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName17} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"
            );
        """

        try {
            // create routine load with COLUMNS clause
            sql """
                CREATE ROUTINE LOAD ${job17} ON ${tableName17}
                COLUMNS (id, name, score)
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic17}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            sql "PAUSE ROUTINE LOAD FOR ${job17}"

            // try to alter to UPDATE_FLEXIBLE_COLUMNS mode - should fail because COLUMNS clause
            test {
                sql """
                    ALTER ROUTINE LOAD FOR ${job17}
                    PROPERTIES
                    (
                        "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                    );
                """
                exception "Flexible partial update does not support COLUMNS specification"
            }
        } catch (Exception e) {
            logger.error("Error during test: " + e.getMessage())
            throw e
        } finally {
            sql "STOP ROUTINE LOAD FOR ${job17}"
        }

        // Test 18: ALTER to flex mode succeeds with WHERE clause
        def kafkaJsonTopic18 = "test_routine_load_alter_flex_where"
        def tableName18 = "test_routine_load_alter_flex_where"
        def job18 = "test_alter_flex_where_job"

        sql """ DROP TABLE IF EXISTS ${tableName18} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName18} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL,
                `age` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"
            );
        """

        // insert initial data
        sql """
            INSERT INTO ${tableName18} VALUES
            (1, 'alice', 100, 20),
            (2, 'bob', 90, 21)
        """

        qt_select_initial18 "SELECT id, name, score, age FROM ${tableName18} ORDER BY id"

        try {
            // create routine load with WHERE clause (UPSERT mode)
            sql """
                CREATE ROUTINE LOAD ${job18} ON ${tableName18}
                WHERE id > 1
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic18}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            sql "PAUSE ROUTINE LOAD FOR ${job18}"

            // alter to UPDATE_FLEXIBLE_COLUMNS mode - should succeed
            sql """
                ALTER ROUTINE LOAD FOR ${job18}
                PROPERTIES
                (
                    "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                );
            """

            // verify the property was changed
            def res = sql "SHOW ROUTINE LOAD FOR ${job18}"
            def jobProperties = res[0][11].toString()
            logger.info("Altered routine load job properties: ${jobProperties}")
            assertTrue(jobProperties.contains("UPDATE_FLEXIBLE_COLUMNS"))

            sql "RESUME ROUTINE LOAD FOR ${job18}"

            // send JSON data - WHERE clause filters id > 1
            def data18 = [
                '{"id": 1, "score": 999}',
                '{"id": 2, "score": 95}',
                '{"id": 3, "name": "charlie", "score": 80}'
            ]

            data18.each { line ->
                logger.info("Sending to Kafka: ${line}")
                def record = new ProducerRecord<>(kafkaJsonTopic18, null, line)
                producer.send(record).get()
            }
            producer.flush()

            // With skip_delete_bitmap=true and WHERE id > 1:
            // - id=1: 1 version (not updated, filtered by WHERE)
            // - id=2: 2 versions (original + partial update)
            // - id=3: 1 version (new row)
            // Total: 4 rows, so expectedMinRows = 3 (waits for count > 3)
            RoutineLoadTestUtils.waitForTaskFinishMoW(runSql, job18, tableName18, 3)

            // verify: id=1 should NOT be updated (filtered by WHERE), id=2,3 should be updated
            qt_select_after_alter_flex_where "SELECT id, name, score, age FROM ${tableName18} ORDER BY id"
        } catch (Exception e) {
            logger.error("Error during test: " + e.getMessage())
            throw e
        } finally {
            sql "STOP ROUTINE LOAD FOR ${job18}"
        }

        // Test 19: ALTER to flex mode fails on non-MoW table
        def kafkaJsonTopic19 = "test_routine_load_alter_flex_non_mow_error"
        def tableName19 = "test_routine_load_alter_flex_non_mow_error"
        def job19 = "test_alter_flex_non_mow_error_job"

        sql """ DROP TABLE IF EXISTS ${tableName19} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName19} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "false"
            );
        """

        try {
            // create routine load
            sql """
                CREATE ROUTINE LOAD ${job19} ON ${tableName19}
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic19}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            sql "PAUSE ROUTINE LOAD FOR ${job19}"

            // try to alter to UPDATE_FLEXIBLE_COLUMNS mode - should fail because non-MoW
            test {
                sql """
                    ALTER ROUTINE LOAD FOR ${job19}
                    PROPERTIES
                    (
                        "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                    );
                """
                exception "Flexible partial update is only supported in unique table MoW"
            }
        } catch (Exception e) {
            logger.error("Error during test: " + e.getMessage())
            throw e
        } finally {
            sql "STOP ROUTINE LOAD FOR ${job19}"
        }

        // Test 20: ALTER from flex mode to UPSERT mode (success case)
        def kafkaJsonTopic20 = "test_routine_load_alter_flex_to_upsert"
        def tableName20 = "test_routine_load_alter_flex_to_upsert"
        def job20 = "test_alter_flex_to_upsert_job"

        sql """ DROP TABLE IF EXISTS ${tableName20} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName20} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL,
                `age` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"
            );
        """

        // insert initial data
        sql """
            INSERT INTO ${tableName20} VALUES
            (1, 'alice', 100, 20),
            (2, 'bob', 90, 21)
        """

        qt_select_initial20 "SELECT id, name, score, age FROM ${tableName20} ORDER BY id"

        try {
            // create routine load with flex mode
            sql """
                CREATE ROUTINE LOAD ${job20} ON ${tableName20}
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json",
                    "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic20}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            sql "PAUSE ROUTINE LOAD FOR ${job20}"

            // alter to UPSERT mode
            sql """
                ALTER ROUTINE LOAD FOR ${job20}
                PROPERTIES
                (
                    "unique_key_update_mode" = "UPSERT"
                );
            """

            // verify the property was changed
            def res = sql "SHOW ROUTINE LOAD FOR ${job20}"
            def jobProperties = res[0][11].toString()
            logger.info("Altered routine load job properties: ${jobProperties}")
            assertTrue(jobProperties.contains("UPSERT"))

            sql "RESUME ROUTINE LOAD FOR ${job20}"

            // send JSON data - with UPSERT mode, missing columns should be NULL
            def data20 = [
                '{"id": 1, "score": 200}',
                '{"id": 3, "name": "charlie", "score": 80, "age": 22}'
            ]

            data20.each { line ->
                logger.info("Sending to Kafka: ${line}")
                def record = new ProducerRecord<>(kafkaJsonTopic20, null, line)
                producer.send(record).get()
            }
            producer.flush()

            // With skip_delete_bitmap=true, count = initial + kafka_messages = 2 + 2 = 4
            RoutineLoadTestUtils.waitForTaskFinishMoW(runSql, job20, tableName20, 3)

            // with UPSERT, id=1 should have NULL for name and age (full row replacement)
            qt_select_after_alter_upsert "SELECT id, name, score, age FROM ${tableName20} ORDER BY id"
        } catch (Exception e) {
            logger.error("Error during test: " + e.getMessage())
            throw e
        } finally {
            sql "STOP ROUTINE LOAD FOR ${job20}"
        }

        // Test 21: ALTER from flex mode to UPDATE_FIXED_COLUMNS mode (success case)
        def kafkaJsonTopic21 = "test_routine_load_alter_flex_to_fixed"
        def tableName21 = "test_routine_load_alter_flex_to_fixed"
        def job21 = "test_alter_flex_to_fixed_job"

        sql """ DROP TABLE IF EXISTS ${tableName21} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName21} (
                `id` int NOT NULL,
                `name` varchar(65533) NULL,
                `score` int NULL,
                `age` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"
            );
        """

        // insert initial data
        sql """
            INSERT INTO ${tableName21} VALUES
            (1, 'alice', 100, 20),
            (2, 'bob', 90, 21)
        """

        qt_select_initial21 "SELECT id, name, score, age FROM ${tableName21} ORDER BY id"

        try {
            // create routine load with flex mode
            sql """
                CREATE ROUTINE LOAD ${job21} ON ${tableName21}
                PROPERTIES
                (
                    "max_batch_interval" = "10",
                    "format" = "json",
                    "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaJsonTopic21}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            sql "PAUSE ROUTINE LOAD FOR ${job21}"

            // alter to UPDATE_FIXED_COLUMNS mode - need to add COLUMNS clause via ALTER
            // Note: This changes from flexible to fixed partial update
            sql """
                ALTER ROUTINE LOAD FOR ${job21}
                PROPERTIES
                (
                    "unique_key_update_mode" = "UPDATE_FIXED_COLUMNS"
                );
            """

            // verify the property was changed
            def res = sql "SHOW ROUTINE LOAD FOR ${job21}"
            def jobProperties = res[0][11].toString()
            logger.info("Altered routine load job properties: ${jobProperties}")
            assertTrue(jobProperties.contains("UPDATE_FIXED_COLUMNS"))

        } catch (Exception e) {
            logger.error("Error during test: " + e.getMessage())
            throw e
        } finally {
            sql "STOP ROUTINE LOAD FOR ${job21}"
        }
    }
}
