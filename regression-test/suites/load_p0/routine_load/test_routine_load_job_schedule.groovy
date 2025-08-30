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

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.Collections

suite("test_routine_load_job_schedule","p0") {
    def kafkaCsvTpoics = [
                  "test_routine_load_job_schedule",
                ]
    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    String kafka_port = context.config.otherConfigs.get("kafka_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def kafka_broker = "${externalEnvIp}:${kafka_port}"

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${kafka_broker}".toString())
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        def producer = new KafkaProducer<>(props)
        def adminClient = AdminClient.create(props)
        def testData = [
            "1,test_data_1,2023-01-01,value1,2023-01-01 10:00:00,extra1",
            "2,test_data_2,2023-01-02,value2,2023-01-02 11:00:00,extra2",
            "3,test_data_3,2023-01-03,value3,2023-01-03 12:00:00,extra3",
            "4,test_data_4,2023-01-04,value4,2023-01-04 13:00:00,extra4",
            "5,test_data_5,2023-01-05,value5,2023-01-05 14:00:00,extra5"
        ]
        testData.each { line->
            logger.info("Sending data to kafka: ${line}")
            def record = new ProducerRecord<>(kafkaCsvTpoics[0], null, line)
            producer.send(record)
        }

        def tableName = "test_routine_load_job_schedule"
        def job = "test_routine_load_job_schedule"
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
            GetDebugPoint().enableDebugPointForAllBEs("FE.ROUTINE_LOAD_TASK_SUBMIT_FAILED")
            sql """
                CREATE ROUTINE LOAD ${job} ON ${tableName}
                COLUMNS TERMINATED BY ","
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${newTopic.name()}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            sleep(5000)
            GetDebugPoint().disableDebugPointForAllBEs("FE.ROUTINE_LOAD_TASK_SUBMIT_FAILED")
            def count = 0
            def maxWaitCount = 60
            while (true) {
                def state = sql "show routine load for ${job}"
                def routineLoadState = state[0][8].toString()
                def statistic = state[0][14].toString()
                logger.info("Routine load state: ${routineLoadState}")
                logger.info("Routine load statistic: ${statistic}")
                def rowCount = sql "select count(*) from ${tableName}"
                if (routineLoadState == "RUNNING" && rowCount[0][0] == 5) {
                    break
                }
                if (count > maxWaitCount) {
                    assertEquals(1, 2)
                }
                sleep(1000)
                count++
            }
        } catch (Exception e) {
            logger.error("Test failed with exception: ${e.message}")
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("FE.ROUTINE_LOAD_TASK_SUBMIT_FAILED")
            try {
                sql "stop routine load for ${job}"
            } catch (Exception e) {
                logger.warn("Failed to stop routine load job: ${e.message}")
            }
        }

        sql "truncate table ${tableName}"
        def memJob = "test_routine_load_job_schedule_mem_limit"
        try {
            GetDebugPoint().enableDebugPointForAllBEs("FE.ROUTINE_LOAD_TASK_SUBMIT_FAILED.MEM_LIMIT_EXCEEDED")
            testData.each { line->
                logger.info("Sending data to kafka: ${line}")
                def record = new ProducerRecord<>(kafkaCsvTpoics[0], null, line)
                producer.send(record)
            }

            sql """
                CREATE ROUTINE LOAD ${memJob} ON ${tableName}
                COLUMNS TERMINATED BY ","
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${newTopic.name()}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING",
                    "max_batch_interval" = "6"
                );
            """

            sleep(5000)

            GetDebugPoint().disableDebugPointForAllBEs("FE.ROUTINE_LOAD_TASK_SUBMIT_FAILED.MEM_LIMIT_EXCEEDED")

            def count = 0
            def maxWaitCount = 120 // > 60 = maxBatchIntervalS * Config.routine_load_task_timeout_multiplier
            while (true) {
                def state = sql "show routine load for ${memJob}"
                def routineLoadState = state[0][8].toString()
                def statistic = state[0][14].toString()
                logger.info("Routine load state: ${routineLoadState}")
                logger.info("Routine load statistic: ${statistic}")
                def rowCount = sql "select count(*) from ${memTableName}"
                if (routineLoadState == "RUNNING" && rowCount[0][0] == 5) {
                    break
                }
                if (count > maxWaitCount) {
                    assertEquals(1, 2)
                }
                sleep(1000)
                count++
            }
        } catch (Exception e) {
            logger.error("MEM_LIMIT_EXCEEDED test failed with exception: ${e.message}")
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("FE.ROUTINE_LOAD_TASK_SUBMIT_FAILED.MEM_LIMIT_EXCEEDED")
            try {
                sql "stop routine load for test_routine_load_job_schedule_mem_limit"
            } catch (Exception e) {
                logger.warn("Failed to stop routine load job: ${e.message}")
            }
        }
    }
}
