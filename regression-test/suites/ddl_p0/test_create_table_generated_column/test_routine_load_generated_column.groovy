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

suite("test_routine_load_generated_column") {
    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    String kafka_port = context.config.otherConfigs.get("kafka_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def kafka_broker = "${externalEnvIp}:${kafka_port}"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // define kafka
        String topic = "testGenCol";
        def props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${kafka_broker}".toString())
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        def producer = new KafkaProducer<>(props)
        filepath = getLoalFilePath "gen_col_data.csv"
        def txt = new File("${filepath}").text
        def lines = txt.readLines();
        lines.each { line ->
            logger.info("=====${line}========")
            def record = new ProducerRecord<>(topic, null, line)
            producer.send(record)
        }
        def tableName = "gencol_refer_gencol_rload"
        sql "drop table if exists ${tableName}"
        sql """create table  ${tableName}(a int,c double generated always as (abs(a+b)) not null,b int, d int generated always as(c+1))
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        ;"""

        sql """
            CREATE ROUTINE LOAD gen_col_routine_load ON  ${tableName}
            COLUMNS(a,b),
            COLUMNS TERMINATED BY ","
            FROM KAFKA
            (
                "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                "kafka_topic" = "${topic}",
                "property.kafka_default_offsets" = "OFFSET_BEGINNING"
            );
        """
        while (true) {
            sleep(1000)
            def res = sql "show routine load for gen_col_routine_load"
            def state = res[0][8].toString()
            if (state == "NEED_SCHEDULE") {
                continue;
            }
            log.info("reason of state changed: ${res[0][17].toString()}".toString())
            assertEquals(res[0][8].toString(), "RUNNING")
            break;
        }
        def count = 0
        while (true) {
            def res = sql "select count(*) from  ${tableName}"
            def state = sql "show routine load for gen_col_routine_load"
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

        qt_common_default "select * from  ${tableName} order by 1,2,3"

        sql "stop routine load for gen_col_routine_load;"
    }

}