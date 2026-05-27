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
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

suite("test_routine_load_preceding_filter_keyword", "p0") {
    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    String kafkaPort = context.config.otherConfigs.get("kafka_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def topicName = "test_preceding_filter_keyword"
        def tableName = "test_routine_load_preceding_filter_keyword_tbl"
        def jobName = "test_preceding_filter_keyword_job"

        def props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${externalEnvIp}:${kafkaPort}".toString())
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000")
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")

        def producer = new KafkaProducer<>(props)
        try {
            def txt = new File("""${context.file.parent}/data/${topicName}.csv""").text
            def lines = txt.readLines()
            lines.each { line ->
                def record = new ProducerRecord<>(topicName, null, line)
                producer.send(record)
            }
        } finally {
            producer.close()
        }

        try {
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """
                CREATE TABLE ${tableName} (
                    app STRING,
                    `group` STRING,
                    msg STRING
                )
                DUPLICATE KEY(app)
                DISTRIBUTED BY HASH(app) BUCKETS 1
                PROPERTIES ("replication_num" = "1");
            """

            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS(app, `group`, msg),
                COLUMNS TERMINATED BY "|",
                PRECEDING FILTER app IS NOT NULL AND `group` IS NOT NULL
                PROPERTIES
                (
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafkaPort}",
                    "kafka_topic" = "${topicName}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"

            int retry = 0
            while (true) {
                sleep(1000)
                def res = sql "show routine load for ${jobName}"
                def state = res[0][8].toString()
                def reason = res[0][17].toString()
                if (state == "RUNNING") {
                    break
                }
                if (state == "PAUSED") {
                    assertTrue(false, "routine load should not be paused, reason: ${reason}")
                }
                retry++
                if (retry > 60) {
                    assertTrue(false, "routine load should become RUNNING, current state: ${state}, reason: ${reason}")
                }
            }

            retry = 0
            while (true) {
                sleep(1000)
                def state = sql "show routine load for ${jobName}"
                if (state[0][8].toString() == "PAUSED") {
                    assertTrue(false, "routine load should keep running, reason: ${state[0][17].toString()}")
                }
                def cnt = sql "select count(*) from ${tableName}"
                if (cnt[0][0] > 0) {
                    break
                }
                retry++
                if (retry > 60) {
                    assertTrue(false, "routine load did not ingest data in time")
                }
            }
        } finally {
            try {
                sql "stop routine load for ${jobName}"
            } catch (Exception e) {
                logger.info("stop routine load failed: ${e.message}".toString())
            }
            sql """ DROP TABLE IF EXISTS ${tableName} """
        }
    }
}
