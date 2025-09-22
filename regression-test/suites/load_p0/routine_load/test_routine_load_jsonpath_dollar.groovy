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

suite("test_routine_load_jsonpath_dollar", "p0") {
    def tableName = "test_routine_load_jsonpath_dollar"
    def jobName = "test_routine_load_jsonpath_dollar_job"

    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    String kafka_port = context.config.otherConfigs.get("kafka_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // Send test data to Kafka
        def props = new Properties()
        props.put("bootstrap.servers", "${externalEnvIp}:${kafka_port}".toString())
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        def producer = new KafkaProducer<>(props)

        def kafkaJson = new File("""${context.file.parent}/data/${jobName}.json""").text
        def lines = kafkaJson.readLines()
        lines.each { line ->
            logger.info("=====${line}========")
            def record = new ProducerRecord<>(jobName, null, line)
            producer.send(record)
        }

        try {
            sql """
                DROP TABLE IF EXISTS ${tableName}
            """

            sql """
                CREATE TABLE ${tableName} (
                    time DATETIME,
                    id INT,
                    name VARCHAR(50),
                    content TEXT
                )
                UNIQUE KEY(time, id)
                DISTRIBUTED BY HASH(time, id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1"
                );
            """

            // Create routine load job with $. in jsonpaths
            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS(ot,time=from_unixtime(`ot`), id, name, content),
                PRECEDING FILTER ((`ot` > 0) AND (`id` != ''))
                PROPERTIES
                (
                    "format" = "json",
                    "jsonpaths" = '[\"\$.time\", \"\$.id\", \"\$.name\", \"\$.\"]',
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200",
                    "max_error_number" = "0",
                    "strip_outer_array" = "false",
                    "strict_mode" = "false"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${jobName}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            sql "sync"

            // Wait for routine load to be in RUNNING state
            def count = 0
            while (true) {
                sleep(1000)
                def res = sql "show routine load for ${jobName}"
                def state = res[0][8].toString()
                log.info("routine load state: ${state}")
                if (state == "RUNNING") {
                    break
                }
                if (count >= 60) {
                    log.error("routine load failed to start after 60 seconds")
                    assertEquals("RUNNING", state)
                    break
                }
                count++
            }

            // Wait for data to be loaded
            count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}")
                log.info("routine load statistic: ${state[0][14].toString()}")
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 60) {
                    log.error("routine load can not load data for long time")
                    break
                }
                sleep(5000)
                count++
            }

            sql "sync"
            def result = sql "select * from ${tableName} order by time, id"
            log.info("Loaded data: ${result}")

            def rowCount = sql "select count(*) from ${tableName}"
            assertTrue(rowCount[0][0] > 0, "No data was loaded")

            def contentCheck = sql "select content from ${tableName} where id = 1"
            assertTrue(contentCheck.size() > 0, "No data found for id = 1")
            def jsonContent = contentCheck[0][0].toString()
            assertTrue(jsonContent.contains("test1"), "Content should contain the full JSON with 'test1'")
            assertTrue(jsonContent.contains("field1"), "Content should contain the full JSON with 'field1'")
            assertTrue(jsonContent.contains("time"), "Content should contain the full JSON with 'time' field")

            def specificData = sql "select date_format(time, '%Y-%m-%dT%H:%i:%s'), id, name from ${tableName} where id = 1"
            assertEquals("2025-07-16T01:31:13", specificData[0][0])
            assertEquals(1, specificData[0][1])
            assertEquals("test1", specificData[0][2])
        } finally {
            try {
                sql "stop routine load for ${jobName}"
            } catch (Exception e) {
                log.info("Stop routine load failed: ${e.getMessage()}")
            }

            try {
                sql "DROP TABLE IF EXISTS ${tableName}"
            } catch (Exception e) {
                log.info("Drop table failed: ${e.getMessage()}")
            }
        }
    }
}