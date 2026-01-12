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

suite("test_routine_load_progress","docker") {
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true
    docker(options) {
        def kafkaCsvTpoics = [
                  "test_routine_load_progress",
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
            def tableName = "test_routine_load_progress"
            def job = "test_progress"
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
                    FROM KAFKA
                    (
                        "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                        "kafka_topic" = "${kafkaCsvTpoics[0]}",
                        "kafka_partitions" = "0",
                        "kafka_offsets" = "2"
                    );
                """
                def count = 0
                def beforeRes = 0
                def afterRes = 0
                while (true) {
                    beforeRes = sql "select count(*) from ${tableName}"
                    log.info("beforeRes: ${beforeRes}")
                    def state = sql "show routine load for ${job}"
                    log.info("routine load state: ${state[0][8].toString()}".toString())
                    log.info("routine load statistic: ${state[0][14].toString()}".toString())
                    log.info("reason of state changed: ${state[0][17].toString()}".toString())
                    def lagJson = parseJson(state[0][16].toString())
                    log.info("lag raw json: ${state[0][16].toString()}")
                    if (beforeRes[0][0] > 0 && lagJson["0"] == 0) {
                        break
                    }
                    if (count >= 30) {
                        log.error("routine load can not visible for long time")
                        assertEquals(1, 2)
                        break
                    }
                    sleep(1000)
                    count++
                }

                // 3. restart fe master
                def masterFeIndex = cluster.getMasterFe().index
                cluster.restartFrontends(masterFeIndex)
                sleep(30 * 1000)
                context.reconnectFe()

                // 4. check count of table
                def state = sql "show routine load for ${job}"
                log.info("routine load statistic: ${state[0][14].toString()}".toString())
                log.info("progress: ${state[0][15].toString()}")
                log.info("lag: ${state[0][16].toString()}")
                afterRes = sql "select count(*) from ${tableName}"
                log.info("afterRes: ${afterRes}")
                if (beforeRes[0][0] != afterRes[0][0]) {
                    assertEquals(1, 2)
                }
            } finally {
                sql "stop routine load for ${job}"
            }
        }
    }
}