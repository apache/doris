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

import groovy.json.JsonSlurper

suite("test_routine_load_abnormal_job_monitor","p0") {
    def kafkaCsvTpoics = [
                  "test_abnormal_job_monitor",
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

    def jobName1 = "test_abnormal_job_monitor1"
    def jobName2 = "test_abnormal_job_monitor2"
    def tableName = "test_abnormal_job_monitor"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            sql """
            CREATE TABLE IF NOT EXISTS ${tableName}
            (
                k00 INT             NOT NULL,
                k01 DATE            NOT NULL,
                k02 BOOLEAN         NULL,
                k03 TINYINT         NULL,
                k04 SMALLINT        NULL,
                k05 INT             NULL,
                k06 BIGINT          NULL,
                k07 LARGEINT        NULL,
                k08 FLOAT           NULL,
                k09 DOUBLE          NULL,
                k10 DECIMAL(9,1)    NULL,
                k11 DECIMALV3(9,1)  NULL,
                k12 DATETIME        NULL,
                k13 DATEV2          NULL,
                k14 DATETIMEV2      NULL,
                k15 CHAR            NULL,
                k16 VARCHAR         NULL,
                k17 STRING          NULL,
                k18 JSON            NULL,
                kd01 BOOLEAN         NOT NULL DEFAULT "TRUE",
                kd02 TINYINT         NOT NULL DEFAULT "1",
                kd03 SMALLINT        NOT NULL DEFAULT "2",
                kd04 INT             NOT NULL DEFAULT "3",
                kd05 BIGINT          NOT NULL DEFAULT "4",
                kd06 LARGEINT        NOT NULL DEFAULT "5",
                kd07 FLOAT           NOT NULL DEFAULT "6.0",
                kd08 DOUBLE          NOT NULL DEFAULT "7.0",
                kd09 DECIMAL         NOT NULL DEFAULT "888888888",
                kd10 DECIMALV3       NOT NULL DEFAULT "999999999",
                kd11 DATE            NOT NULL DEFAULT "2023-08-24",
                kd12 DATETIME        NOT NULL DEFAULT "2023-08-24 12:00:00",
                kd13 DATEV2          NOT NULL DEFAULT "2023-08-24",
                kd14 DATETIMEV2      NOT NULL DEFAULT "2023-08-24 12:00:00",
                kd15 CHAR(255)       NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd16 VARCHAR(300)    NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd17 STRING          NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd18 JSON            NULL,
                
                INDEX idx_inverted_k104 (`k05`) USING INVERTED,
                INDEX idx_inverted_k110 (`k11`) USING INVERTED,
                INDEX idx_inverted_k113 (`k13`) USING INVERTED,
                INDEX idx_inverted_k114 (`k14`) USING INVERTED,
                INDEX idx_inverted_k117 (`k17`) USING INVERTED PROPERTIES("parser" = "english"),
                INDEX idx_ngrambf_k115 (`k15`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),
                INDEX idx_ngrambf_k116 (`k16`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),
                INDEX idx_ngrambf_k117 (`k17`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),

                INDEX idx_bitmap_k104 (`k02`) USING BITMAP,
                INDEX idx_bitmap_k110 (`kd01`) USING BITMAP
                
            )
            DUPLICATE KEY(k00)
            PARTITION BY RANGE(k01)
            (
                PARTITION p1 VALUES [('2023-08-01'), ('2023-08-11')),
                PARTITION p2 VALUES [('2023-08-11'), ('2023-08-21')),
                PARTITION p3 VALUES [('2023-08-21'), ('2023-09-01'))
            )
            DISTRIBUTED BY HASH(k00) BUCKETS 32
            PROPERTIES (
                "bloom_filter_columns"="k05",
                "replication_num" = "1"
            );
            """
            sql """
                CREATE ROUTINE LOAD ${jobName1} on ${tableName}
                COLUMNS(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18),
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
                    "kafka_topic" = "test_abnormal_job_monitor_invaild",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql """
                CREATE ROUTINE LOAD ${jobName2} on ${tableName}
                COLUMNS(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18),
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
                    "kafka_topic" = "test_abnormal_job_monitor",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"

            sql "pause routine load for ${jobName2}"

            def count = 0
            def metricCount = 0
            String masterHttpAddress = getMasterIp() + ":" + getMasterPort()
            log.info("master host: ${masterHttpAddress}".toString())
            while (true) {
                metricCount = 0
                httpTest {
                    endpoint masterHttpAddress
                    uri "/metrics?type=json"
                    op "get"
                    check { code, body ->
                        def jsonSlurper = new JsonSlurper()
                        def result = jsonSlurper.parseText(body)

                        def entry = result.find { it.tags?.metric == "doris_fe_job" && it.tags?.state == "ABNORMAL_PAUSED"}
                        def value = entry ? entry.value : null
                        log.info("Contains ABNORMAL_PAUSE: ${entry != null}".toString())
                        log.info("Value of ABNORMAL_PAUSE: ${value}".toString())
                        if (value > 0) {
                            metricCount++
                        }

                        entry = result.find { it.tags?.metric == "doris_fe_job" && it.tags?.state == "USER_PAUSED"}
                        value = entry ? entry.value : null
                        log.info("Contains USER_PAUSE: ${entry != null}".toString())
                        log.info("Value of USER_PAUSE: ${value}".toString())
                        if (value > 0) {
                            metricCount++
                        }

                        if (body.contains("doris_fe_routine_load_progress")){
                            log.info("contain doris_fe_routine_load_progress")
                            metricCount++
                        }

                        if (body.contains("doris_fe_routine_load_lag")){
                            log.info("contain doris_fe_routine_load_lag")
                            metricCount++
                        }

                        if (body.contains("doris_fe_routine_load_abort_task_num")){
                            log.info("contain doris_fe_routine_load_abort_task_num")
                            metricCount++
                        }
                    }
                }
                if (metricCount == 5) {
                    break
                }
                count++
                sleep(1000)
                if (count > 60) {
                    assertEquals(1, 2)
                }
            }
        } finally {
            sql "stop routine load for ${jobName1}"
            sql "stop routine load for ${jobName2}"
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }
}