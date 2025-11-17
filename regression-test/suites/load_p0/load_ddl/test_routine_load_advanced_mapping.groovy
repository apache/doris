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

suite("test_routine_load_advanced_mapping","p0") {
    def kafkaCsvTpoics = [
                  "load_ddl_basic_data_json_by_line",
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
            def txt = new File("""${context.file.parent}/data/${kafkaCsvTopic}.json""").text
            def lines = txt.readLines()
            lines.each { line ->
                logger.info("=====${line}========")
                def record = new ProducerRecord<>(kafkaCsvTopic, null, line)
                producer.send(record)
            }
        }
    }

    def jobName = "whereConditionJob"
    def tableName = "test_routine_load_advanced_mapping"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            sql """
            CREATE TABLE IF NOT EXISTS ${tableName}
            (
                k00 INT             NOT NULL,
                k01 DATE            NOT NULL,
                k02 BOOLEAN         REPLACE NULL,
                k03 TINYINT         SUM NULL,
                k04 SMALLINT        SUM NULL,
                k05 INT             SUM NULL,
                k06 BIGINT          SUM NULL,
                k07 LARGEINT        SUM NULL,
                k08 FLOAT           SUM NULL,
                k09 DOUBLE          SUM NULL,
                k10 DECIMAL(9,1)    SUM NULL,
                k11 DECIMALV3(9,1)  SUM NULL,
                k12 DATETIME        REPLACE NULL,
                k13 DATEV2          REPLACE NULL,
                k14 DATETIMEV2      REPLACE NULL,
                k15 CHAR(255)          REPLACE NULL,
                k16 VARCHAR(300)         REPLACE NULL,
                k17 STRING          REPLACE NULL,
                k18 JSON            REPLACE NULL,
                k19 BITMAP          BITMAP_UNION ,
                k20 HLL             HLL_UNION ,
                k21 QUANTILE_STATE  QUANTILE_UNION ,
                kd01 BOOLEAN         REPLACE NOT NULL DEFAULT "TRUE",
                kd02 TINYINT         SUM NOT NULL DEFAULT "1",
                kd03 SMALLINT        SUM NOT NULL DEFAULT "2",
                kd04 INT             SUM NOT NULL DEFAULT "3",
                kd05 BIGINT          SUM NOT NULL DEFAULT "4",
                kd06 LARGEINT        SUM NOT NULL DEFAULT "5",
                kd07 FLOAT           SUM NOT NULL DEFAULT "6.0",
                kd08 DOUBLE          SUM NOT NULL DEFAULT "7.0",
                kd09 DECIMAL         SUM NOT NULL DEFAULT "888888888",
                kd10 DECIMALV3       SUM NOT NULL DEFAULT "999999999",
                kd11 DATE            REPLACE NOT NULL DEFAULT "2023-08-24",
                kd12 DATETIME        REPLACE NOT NULL DEFAULT "2023-08-24 12:00:00",
                kd13 DATEV2          REPLACE NOT NULL DEFAULT "2023-08-24",
                kd14 DATETIMEV2      REPLACE NOT NULL DEFAULT "2023-08-24 12:00:00",
                kd15 CHAR(255)       REPLACE NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd16 VARCHAR(300)    REPLACE NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd17 STRING          REPLACE NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd18 JSON            REPLACE NULL,
                kd19 BITMAP          BITMAP_UNION ,
                kd20 HLL             HLL_UNION ,
                kd21 QUANTILE_STATE  QUANTILE_UNION ,
                INDEX idx_bitmap_k104 (`k01`) USING BITMAP
            )
            AGGREGATE KEY(k00,k01)
            PARTITION BY RANGE(k01)
            (
                PARTITION p1 VALUES [('2023-08-01'), ('2023-08-11')),
                PARTITION p2 VALUES [('2023-08-11'), ('2023-08-21')),
                PARTITION p3 VALUES [('2023-08-21'), ('2023-09-01'))
            )
            DISTRIBUTED BY HASH(k00) BUCKETS 32
            PROPERTIES (
                "replication_num" = "1"
            );
            """
            sql "sync"

            sql """
                CREATE ROUTINE LOAD ${jobName} on ${tableName}
                COLUMNS(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19=to_bitmap(k04),k20=HLL_HASH(k04),k21=TO_QUANTILE_STATE(k04,1.0),kd19=to_bitmap(k05),kd20=HLL_HASH(k05),kd21=TO_QUANTILE_STATE(k05,1.0)),
                COLUMNS TERMINATED BY "|",
                where k00 = 8
                PROPERTIES
                (
                    "format" = "json",
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "basic_data_json_by_line",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """
            sql "sync"

            def count = 0
            while (true) {
                sleep(1000)
                def res = sql "show routine load for ${jobName}"
                def state = res[0][8].toString()
                if (state != "RUNNING") {
                    count++
                    if (count > 60) {
                        assertEquals(1, 2)
                    } 
                    continue;
                }
                log.info("reason of state changed: ${res[0][11].toString()}".toString())
                def json = parseJson(res[0][11])
                assertEquals("(k00 = 8)", json.whereExpr.toString())
                break;
            }
            while (true) {
                sleep(1000)
                def res = sql "show routine load for ${jobName}"
                log.info("routine load statistic: ${res[0][14].toString()}".toString())
                def json = parseJson(res[0][14])
                if (json.unselectedRows.toString() != "19") {
                    count++
                    if (count > 60) {
                        assertEquals(1, 2)
                    } 
                    continue;
                }
                break;
            }
            sleep(2000)
        } finally {
            sql "stop routine load for ${jobName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }
}
