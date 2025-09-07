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
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig

suite("test_routine_load_empty_field_as_null","p0") {
    def kafkaCsvTpoics = [
                "test_routine_load_empty_field_as_null",
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
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000")  
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
        def verifyKafkaConnection = { prod ->
            try {
                logger.info("=====try to connect Kafka========")
                def partitions = prod.partitionsFor("__connection_verification_topic")
                return partitions != null
            } catch (Exception e) {
                throw new Exception("Kafka connect fail: ${e.message}".toString())
            }
        }
        def producer = new KafkaProducer<>(props)
        try {
            logger.info("Kafka connecting: ${kafka_broker}")
            if (!verifyKafkaConnection(producer)) {
                throw new Exception("can't get any kafka info")
            }
        } catch (Exception e) {
            logger.error("FATAL: " + e.getMessage())
            producer.close()
            throw e  
        }
        logger.info("Kafka connect success")
        for (String kafkaCsvTopic in kafkaCsvTpoics) {
            def testData = [
                "9,\\N,2023-07-15,def,2023-07-20T05:48:31,ghi",
                "10,,2023-07-15,def,2023-07-20T05:48:31,ghi"
            ]
            
            testData.each { line ->
                logger.info("Sending data to kafka: ${line}")
                def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
                producer.send(record)
            }
        }

        def tableName = "test_routine_load_empty_field_as_null"
        def job = "test_follower_routine_load"
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
                    "empty_field_as_null" = "false"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaCsvTpoics[0]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            def count = 0
            def maxWaitCount = 60
            while (count < maxWaitCount) {
                def state = sql "show routine load for ${job}"
                def routineLoadState = state[0][8].toString()
                def statistic = state[0][14].toString()
                logger.info("Routine load state: ${routineLoadState}")
                logger.info("Routine load statistic: ${statistic}")
                def rowCount = sql "select count(*) from ${tableName}"
                if (routineLoadState == "RUNNING" && rowCount[0][0] > 0) {
                    break
                }
                sleep(1000)
                count++
            }
            qt_select_1 """
                SELECT * FROM ${tableName} ORDER BY k1 
            """

            sql "pause routine load for ${job}"
            def res = sql "show routine load for ${job}"
            logger.info("routine load job properties: ${res[0][11].toString()}".toString())
            sql "ALTER ROUTINE LOAD FOR ${job} PROPERTIES(\"empty_field_as_null\" = \"true\");"
            sql "truncate table ${tableName}"
            sql "resume routine load for ${job}"
            for (String kafkaCsvTopic in kafkaCsvTpoics) {
                def testData = [
                    "9,\\N,2023-07-15,def,2023-07-20T05:48:31,ghi",
                    "10,,2023-07-15,def,2023-07-20T05:48:31,ghi"
                ]
                testData.each { line ->
                    logger.info("Sending data to kafka: ${line}")
                    def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
                    producer.send(record)
                }
            }

            count = 0
            while (count < maxWaitCount) {
                def state = sql "show routine load for ${job}"
                def routineLoadState = state[0][8].toString()
                def statistic = state[0][14].toString()
                logger.info("Routine load state: ${routineLoadState}")
                logger.info("Routine load statistic: ${statistic}")
                def rowCount = sql "select count(*) from ${tableName}"
                if (routineLoadState == "RUNNING" && rowCount[0][0] > 0) {
                    break
                }
                sleep(1000)
                count++
            }
            qt_select_2 """
                SELECT * FROM ${tableName} ORDER BY k1 
            """

        } catch (Exception e) {
            logger.error("Test failed with exception: ${e.message}")
        } finally {
            try {
                sql "stop routine load for ${job}"
            } catch (Exception e) {
                logger.warn("Failed to stop routine load job: ${e.message}")
            }
        }
    }
}