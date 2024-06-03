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
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig

suite("test_routine_load_error","p0") {
    def kafkaCsvTpoics = [
                  "multi_table_load_invalid_table",
                ]

    def kafkaJsonTopics = [
                  "invalid_json_path",
                ]

    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    String kafka_port = context.config.otherConfigs.get("kafka_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def kafka_broker = "${externalEnvIp}:${kafka_port}"

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // define kafka 
        def props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${kafka_broker}".toString())
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        // Create kafka producer
        def producer = new KafkaProducer<>(props)

        for (String kafkaCsvTopic in kafkaCsvTpoics) {
            def txt = new File("""${context.file.parent}/data/${kafkaCsvTopic}.csv""").text
            def lines = txt.readLines()
            lines.each { line ->
                logger.info("=====${line}========")
                def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
                producer.send(record)
            }
        }
        for (String kafkaJsonTopic in kafkaJsonTopics) {
            def kafkaJson = new File("""${context.file.parent}/data/${kafkaJsonTopic}.json""").text
            def lines = kafkaJson.readLines()
            lines.each { line ->
                logger.info("=====${line}========")
                def record = new ProducerRecord<>(kafkaJsonTopic, null, line)
                producer.send(record)
            }
        }
    }

    def i = 0
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            sql """
                CREATE ROUTINE LOAD testTableNoExist
                COLUMNS TERMINATED BY "|"
                PROPERTIES
                (
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "multi_table_load_invalid_table",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"

            def count = 0
            while (true) {
                sleep(1000)
                def res = sql "show routine load for testTableNoExist"
                def state = res[0][8].toString()
                if (state != "PAUSED") {
                    count++
                    if (count > 60) {
                        assertEquals(1, 2)
                    } 
                    continue;
                }
                log.info("reason of state changed: ${res[0][17].toString()}".toString())
                assertTrue(res[0][17].toString().contains("table not found"))
                break;
            }
        } finally {
            sql "stop routine load for testTableNoExist"
        }
    }

    // test out of range
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def jobName = "testOutOfRange"
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName}
                COLUMNS TERMINATED BY "|"
                PROPERTIES
                (
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_partitions" = "0",
                    "kafka_topic" = "multi_table_load_invalid_table",
                    "kafka_offsets" = "100"
                );
            """
            sql "sync"

            def count = 0
            while (true) {
                sleep(1000)
                def res = sql "show routine load for ${jobName}"
                def state = res[0][8].toString()
                log.info("routine load state: ${res[0][8].toString()}".toString())
                log.info("routine load statistic: ${res[0][14].toString()}".toString())
                log.info("reason of state changed: ${res[0][17].toString()}".toString())
                if (state != "PAUSED") {
                    count++
                    if (count > 60) {
                        assertEquals(1, 2)
                    } 
                    continue;
                }
                log.info("reason of state changed: ${res[0][17].toString()}".toString())
                assertTrue(res[0][17].toString().contains("is greater than kafka latest offset"))
                break;
            }
        } finally {
            sql "stop routine load for ${jobName}"
        }
    }
    
    // test json path is invalid
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def tableName = "test_invalid_json_path"
        def jobName = "invalid_json_path"
        try {
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """
            CREATE TABLE IF NOT EXISTS ${tableName}
            (
                k00 INT                    NOT NULL,
                k01 array<BOOLEAN>         NULL,
                k02 array<TINYINT>         NULL,
                k03 array<SMALLINT>        NULL,
                k04 array<INT>             NULL,
                k05 array<BIGINT>          NULL,
                k06 array<LARGEINT>        NULL,
                k07 array<FLOAT>           NULL,
                k08 array<DOUBLE>          NULL,
                k09 array<DECIMAL>         NULL,
                k10 array<DECIMALV3>       NULL,
                k11 array<DATE>            NULL,
                k12 array<DATETIME>        NULL,
                k13 array<DATEV2>          NULL,
                k14 array<DATETIMEV2>      NULL,
                k15 array<CHAR>            NULL,
                k16 array<VARCHAR>         NULL,
                k17 array<STRING>          NULL,
                kd01 array<BOOLEAN>         NOT NULL DEFAULT "[]",
                kd02 array<TINYINT>         NOT NULL DEFAULT "[]",
                kd03 array<SMALLINT>        NOT NULL DEFAULT "[]",
                kd04 array<INT>             NOT NULL DEFAULT "[]",
                kd05 array<BIGINT>          NOT NULL DEFAULT "[]",
                kd06 array<LARGEINT>        NOT NULL DEFAULT "[]",
                kd07 array<FLOAT>           NOT NULL DEFAULT "[]",
                kd08 array<DOUBLE>          NOT NULL DEFAULT "[]",
                kd09 array<DECIMAL>         NOT NULL DEFAULT "[]",
                kd10 array<DECIMALV3>       NOT NULL DEFAULT "[]",
                kd11 array<DATE>            NOT NULL DEFAULT "[]",
                kd12 array<DATETIME>        NOT NULL DEFAULT "[]",
                kd13 array<DATEV2>          NOT NULL DEFAULT "[]",
                kd14 array<DATETIMEV2>      NOT NULL DEFAULT "[]",
                kd15 array<CHAR>            NOT NULL DEFAULT "[]",
                kd16 array<VARCHAR>         NOT NULL DEFAULT "[]",
                kd17 array<STRING>          NOT NULL DEFAULT "[]"
            )
            UNIQUE KEY(k00)
            DISTRIBUTED BY HASH(k00) BUCKETS 32
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true"
            );
            """

            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17)
                PROPERTIES
                (
                    "format" = "json",
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200",
                    "jsonpaths" = "[\'t\',\'a\']"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${jobName}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            def count = 0
            while (true) {
                sleep(1000)
                def res = sql "show routine load for ${jobName}"
                def state = res[0][8].toString()
                log.info("routine load state: ${res[0][8].toString()}".toString())
                log.info("routine load statistic: ${res[0][14].toString()}".toString())
                log.info("reason of state changed: ${res[0][17].toString()}".toString())
                if (state != "PAUSED") {
                    count++
                    if (count > 60) {
                        assertEquals(1, 2)
                    } 
                    continue;
                }
                log.info("reason of state changed: ${res[0][17].toString()}".toString())
                assertTrue(res[0][17].toString().contains("Invalid json path"))
                break;
            }
        } finally {
            sql "stop routine load for ${jobName}"
            sql """ DROP TABLE IF EXISTS ${tableName} """
        }
    }

    // test failed to fetch all current partition
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def jobName = "invalid_topic"
        try {
            sql """
                CREATE ROUTINE LOAD ${jobName}
                COLUMNS TERMINATED BY "|"
                PROPERTIES
                (
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "invalid_topic",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"

            def count = 0
            while (true) {
                sleep(1000)
                def res = sql "show routine load for ${jobName}"
                def state = res[0][8].toString()
                if (state != "PAUSED") {
                    count++
                    if (count > 60) {
                        assertEquals(1, 2)
                    } 
                    continue;
                }
                log.info("reason of state changed: ${res[0][17].toString()}".toString())
                assertTrue(res[0][17].toString().contains("may be Kafka properties set in job is error or no partition in this topic that should check Kafka"))
                break;
            }
        } finally {
            sql "stop routine load for ${jobName}"
        }
    }
}