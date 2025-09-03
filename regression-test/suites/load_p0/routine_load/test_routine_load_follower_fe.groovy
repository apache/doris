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

suite("test_routine_load_follower_fe","docker") {
    def options = new ClusterOptions()
    // Configure 3 FE nodes cluster
    options.setFeNum(3)
    options.setBeNum(1)

    docker(options) {
        def kafkaCsvTpoics = [
                  "test_routine_load_follower_fe",
                ]
        String enabled = context.config.otherConfigs.get("enableKafkaTest")
        String kafka_port = context.config.otherConfigs.get("kafka_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        def kafka_broker = "${externalEnvIp}:${kafka_port}"
        
        if (enabled != null && enabled.equalsIgnoreCase("true")) {
            // 1. send data to kafka
            def props = new Properties()
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${kafka_broker}".toString())
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            // add timeout config
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000")  
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")

            // check conenction
            def verifyKafkaConnection = { prod ->
                try {
                    logger.info("=====try to connect Kafka========")
                    def partitions = prod.partitionsFor("__connection_verification_topic")
                    return partitions != null
                } catch (Exception e) {
                    throw new Exception("Kafka connect fail: ${e.message}".toString())
                }
            }
            // Create kafka producer
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
            
            // Send test data to kafka topic
            for (String kafkaCsvTopic in kafkaCsvTpoics) {
                // Create simple test data
                def testData = [
                    "1,test_data_1,2023-01-01,value1,2023-01-01 10:00:00,extra1",
                    "2,test_data_2,2023-01-02,value2,2023-01-02 11:00:00,extra2",
                    "3,test_data_3,2023-01-03,value3,2023-01-03 12:00:00,extra3",
                    "4,test_data_4,2023-01-04,value4,2023-01-04 13:00:00,extra4",
                    "5,test_data_5,2023-01-05,value5,2023-01-05 14:00:00,extra5"
                ]
                
                testData.each { line ->
                    logger.info("Sending data to kafka: ${line}")
                    def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
                    producer.send(record)
                }
            }
            
            // 3. Connect to a follower FE and create table
            def masterFe = cluster.getMasterFe()
            def allFes = cluster.getAllFrontends()
            def followerFes = allFes.findAll { fe -> fe.index != masterFe.index }
            def followerFe = followerFes[0]
            logger.info("Master FE: ${masterFe.host}")
            logger.info("Using follower FE: ${followerFe.host}")
            // Connect to follower FE
            def url = String.format(
                    "jdbc:mysql://%s:%s/?useLocalSessionState=true&allowLoadLocalInfile=false",
                    followerFe.host, followerFe.queryPort)
            logger.info("Connecting to follower FE: ${url}")
            context.connectTo(url, context.config.jdbcUser, context.config.jdbcPassword)

            sql "drop database if exists test_routine_load_follower_fe"
            sql "create database test_routine_load_follower_fe"
            sql "use test_routine_load_follower_fe"
            def tableName = "test_routine_load_follower_fe"
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
                // 4. Create routine load job on follower FE
                sql """
                    CREATE ROUTINE LOAD ${job} ON ${tableName}
                    COLUMNS TERMINATED BY ","
                    PROPERTIES
                    (
                        "max_batch_interval" = "20",
                        "max_batch_rows" = "300000",
                        "max_batch_size" = "209715200"
                    )
                    FROM KAFKA
                    (
                        "kafka_broker_list" = "${kafka_broker}",
                        "kafka_topic" = "${kafkaCsvTpoics[0]}",
                        "property.group.id" = "test-follower-consumer-group",
                        "property.client.id" = "test-follower-client-id",
                        "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                    );
                """

                // 5. Wait for routine load to process data
                def count = 0
                def maxWaitCount = 60 // Wait up to 60 seconds
                while (count < maxWaitCount) {
                    def state = sql "show routine load for ${job}"
                    def routineLoadState = state[0][8].toString()
                    def statistic = state[0][14].toString()
                    logger.info("Routine load state: ${routineLoadState}")
                    logger.info("Routine load statistic: ${statistic}")
                    
                    def rowCount = sql "select count(*) from ${tableName}"
                    // Check if routine load is running and has processed some data
                    if (routineLoadState == "RUNNING" && rowCount[0][0] > 0) {
                        break
                    }
                    
                    sleep(1000)
                    count++
                }
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
} 