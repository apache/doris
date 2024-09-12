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
import org.apache.doris.regression.util.NodeType
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig

suite("test_routine_load_restart_fe", "docker") {
    def kafkaCsvTpoics = [
                  "test_out_of_range",
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

    def options = new ClusterOptions()
    options.setFeNum(1)
    docker(options) {
        def load_with_injection = { injection ->
            def jobName = "test_routine_load_restart"
            def tableName = "dup_tbl_basic_multi_table"
            if (enabled != null && enabled.equalsIgnoreCase("true")) {
                try {
                    GetDebugPoint().enableDebugPointForAllBEs(injection)
                    sql new File("""${context.file.parent}/ddl/${tableName}_drop.sql""").text
                    sql new File("""${context.file.parent}/ddl/${tableName}_create.sql""").text
                    sql "sync"

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
                            "kafka_topic" = "test_out_of_range",
                            "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                        );
                    """
                    sql "sync"

                    def count = 0
                    while (true) {
                        sleep(1000)
                        def res = sql "show routine load for ${jobName}"
                        def state = res[0][8].toString()
                        if (state == "PAUSED") {
                            log.info("reason of state changed: ${res[0][17].toString()}".toString())
                            assertTrue(res[0][17].toString().contains("Offset out of range"))
                            assertTrue(res[0][17].toString().contains("consume partition"))
                            assertTrue(res[0][17].toString().contains("consume offset"))
                            GetDebugPoint().disableDebugPointForAllBEs(injection)
                            break;
                        }
                        count++
                        if (count > 60) {
                            GetDebugPoint().disableDebugPointForAllBEs(injection)
                            assertEquals(1, 2)
                            break;
                        } else {
                            continue;
                        }
                    }
                } catch (Exception e) {
                    sql "stop routine load for ${jobName}"
                    sql "DROP TABLE IF EXISTS ${tableName}"
                }
            }
        }
        load_with_injection("KafkaDataConsumer.group_consume.out_of_range")

        cluster.restartFrontends()
        sleep(30000)
        context.reconnectFe()

        def res = sql "show routine load for ${jobName}"
        def state = res[0][8].toString()
        if (state == "PAUSED") {
            log.info("reason of state changed: ${res[0][17].toString()}".toString())
            assertTrue(res[0][17].toString().contains("Offset out of range"))
            assertTrue(res[0][17].toString().contains("consume partition"))
            assertTrue(res[0][17].toString().contains("consume offset"))
        } else {
            assertEquals(1, 2)
        }
        sql "stop routine load for ${jobName}"
        sql "DROP TABLE IF EXISTS ${tableName}"
    }
}

