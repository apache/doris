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

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig

suite("test_load_single_tablet", "p0") {
    def tableName = "test_load_single_tablet_table"
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def ak = getS3AK()
    def sk = getS3SK()

    sql """ DROP TABLE IF EXISTS ${tableName} """
    
    sql """
        CREATE TABLE ${tableName} (
            user_id            BIGINT       NOT NULL COMMENT "user id",
            name               VARCHAR(20)           COMMENT "name",
            age                INT                   COMMENT "age"
        )
        UNIQUE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // Test Broker Load with load_to_single_tablet on HASH distribution table (should fail)
    try {
        def label1 = "test_load_single_tablet_" + System.currentTimeMillis()
        sql """
            LOAD LABEL ${label1} (
                DATA INFILE("s3://${s3BucketName}/load/jira/load22282.csv")
                INTO TABLE ${tableName}
                COLUMNS TERMINATED BY ","
                FORMAT AS "CSV"
                (user_id, name, age)
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "${ak}",
                "AWS_SECRET_KEY" = "${sk}",
                "AWS_ENDPOINT" = "${s3Endpoint}",
                "AWS_REGION" = "${s3Region}"
            )
            PROPERTIES (
                "timeout" = "3600",
                "load_to_single_tablet" = "true"
            );
            """

        // Wait for the load job to be processed and check if it fails
        def max_try_milli_secs = 60000
        def foundExpectedError = false
        while (max_try_milli_secs > 0) {
            def result = sql "SHOW LOAD WHERE LABEL = '${label1}'"
            if (result.size() > 0) {
                def state = result[0][2]
                def errorMsg = result[0][7]
                logger.info("Load label: ${label1}, state: ${state}, errorMsg: ${errorMsg}")
                
                if (state == "CANCELLED") {
                    logger.info("Broker load failed as expected with error: ${errorMsg}")
                    foundExpectedError = true
                    break
                } else if (state == "FINISHED") {
                    assertTrue(false, "Broker load should fail but succeeded")
                }
            }
            
            Thread.sleep(1000)
            max_try_milli_secs -= 1000
            if (max_try_milli_secs <= 0) {
                assertTrue(false, "test_load_single_tablet broker load timeout: ${label1}")
            }
        }
        
        assertTrue(foundExpectedError, "Broker load should have failed with expected error")
    } catch (Exception e) {
        // It's also acceptable if the load fails at creation time
        logger.info("Broker load failed at creation as expected: ${e.message}")
        assertTrue(e.message.contains("if load_to_single_tablet set to true") || 
                  e.message.contains("the olap table must be with random distribution"),
                  "Expected error message about random distribution, but got: ${e.message}")
    }

    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    String kafka_port = context.config.otherConfigs.get("kafka_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def kafka_broker = "${externalEnvIp}:${kafka_port}"
    
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def kafkaCsvTopic = "test_load_single_tablet_topic"
        
        def props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${kafka_broker}".toString())
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000")
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")

        def producer = new KafkaProducer<>(props)
        
        try {
            def testData = "1,ykiko,20"
            producer.send(new ProducerRecord<>(kafkaCsvTopic, null, testData))
            producer.flush()
            logger.info("Sent test data to Kafka topic ${kafkaCsvTopic}: ${testData}")
            
            def label2 = "test_load_single_tablet2_" + System.currentTimeMillis()
            
            sql """
                CREATE ROUTINE LOAD ${label2} ON ${tableName}
                COLUMNS TERMINATED BY ",",
                COLUMNS(user_id, name, age)
                PROPERTIES (
                    "load_to_single_tablet" = "true"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafka_broker}",
                    "kafka_topic" = "${kafkaCsvTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            
            assertTrue(false, "Routine load should fail when load_to_single_tablet=true with HASH distribution table")
        } catch (Exception e) {
            logger.info("Routine load failed as expected: ${e.message}")
            assertTrue(e.message.contains("if load_to_single_tablet set to true, the olap table must be with random distribution"),
                      "Expected error message: 'if load_to_single_tablet set to true, the olap table must be with random distribution', but got: ${e.message}")
        } finally {
            if (producer != null) {
                producer.close()
            }
        }
    } else {
        logger.info("Skip routine load test because enableKafkaTest is not enabled")
    }
    
    sql """ DROP TABLE IF EXISTS ${tableName} """
}