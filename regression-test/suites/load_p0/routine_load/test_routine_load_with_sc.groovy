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

suite("test_routine_load_with_sc","p0") {
    def kafkaCsvTpoics = [
                  "test_routine_load_with_sc",
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
    }

    def jobName = "test_routine_load_with_sc_job"
    def tableName = "test_routine_load_with_sc"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            sql """ DROP TABLE IF EXISTS ${tableName}"""
            sql """ CREATE TABLE IF NOT EXISTS ${tableName}
                    (
                        `k1` int(20) NULL,
                        `k2` string NULL,
                        `v1` date  NULL,
                        `v2` string  NULL,
                        `v3` datetime  NULL,
                        `v4` varchar(5)  NULL
                    ) ENGINE=OLAP
                    DUPLICATE KEY(`k1`)
                    COMMENT 'OLAP'
                    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
                    PROPERTIES ("replication_allocation" = "tag.location.default: 1");
                """
            sql "sync"

            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS TERMINATED BY ","
                PROPERTIES
                (
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTpoics[0]}",
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
                break;
            }

            sql "ALTER TABLE ${tableName} MODIFY COLUMN v4 VARCHAR(10)"
            sql "resume routine load for ${jobName}"

            def props = new Properties()
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${kafka_broker}".toString())
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
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

            while (true) {
                def res = sql "select count(*) from ${tableName}"
                def state = sql "show routine load for ${jobName}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 120) {
                    log.error("routine load can not visible for long time")
                    assertEquals(20, res[0][0])
                    break
                }
                sleep(5000)
                count++
            }
            qt_sql_with_sc "select * from ${tableName} order by k1"
        } finally {
            sql "stop routine load for ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }
}
