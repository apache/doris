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

suite("test_black_list","nonConcurrent,p0") {
    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // 1. send data
        def kafkaCsvTpoics = [
                  "test_black_list",
                ]
        String kafka_port = context.config.otherConfigs.get("kafka_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        def kafka_broker = "${externalEnvIp}:${kafka_port}"
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

        // 2. create table and routine load job
        def tableName = "test_black_list"
        def job = "test_black_list_job"
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

        def inject = "KafkaDataConsumer.get_latest_offsets_for_partitions.timeout"
        try {
            GetDebugPoint().enableDebugPointForAllBEs(inject)
            sql """
                CREATE ROUTINE LOAD ${job} ON ${tableName}
                COLUMNS TERMINATED BY ","
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${kafkaCsvTpoics[0]}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            def count = 0
            while (true) {
                def res = sql "select count(*) from ${tableName}"
                log.info("res: ${res}")
                def state = sql "show routine load for ${job}"
                log.info("routine load state: ${state[0][8].toString()}".toString())
                log.info("reason of state changed: ${state[0][17].toString()}".toString())
                log.info("other msg: ${state[0][19].toString()}".toString())
                if (state[0][17].toString().contains("failed to get latest partition offset") || state[0][19].toString().contains("failed to get latest partition offset")) {
                    break
                }
                if (count >= 90) {
                    log.error("routine load test fail")
                    assertEquals(1, 2)
                    break
                }
                sleep(1000)
                count++
            }

            count = 0
            GetDebugPoint().disableDebugPointForAllBEs(inject)
            while (true) {
                sleep(1000)
                def res = sql "show routine load for ${job}"
                log.info("routine load statistic: ${res[0][14].toString()}".toString())
                log.info("progress: ${res[0][15].toString()}".toString())
                log.info("lag: ${res[0][16].toString()}".toString())
                res = sql "select count(*) from ${tableName}"
                if (res[0][0] > 0) {
                    break;
                }
                count++
                if (count > 60) {
                    assertEquals(1, 2)
                } 
                continue;
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(inject)
            sql "stop routine load for ${job}"
        }
    }
}