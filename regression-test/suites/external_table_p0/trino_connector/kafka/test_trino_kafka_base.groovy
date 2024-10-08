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

suite("test_trino_kafka_base", "external,kafka,external_docker,external_docker_kafka") {

    // Ensure that all types are parsed correctly
    def select_top50 = {
        qt_select_top50 """select * from orc_all_types order by int_col desc limit 50;"""
    }


    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    String enabled_trino_connector = context.config.otherConfigs.get("enableTrinoConnectorTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")
        && enabled_trino_connector!= null && enabled_trino_connector.equalsIgnoreCase("true")) {
        // set up trino-connector plugins
        def host_ips = new ArrayList()
        String[][] backends = sql """ show backends """
        for (def b in backends) {
            host_ips.add(b[1])
        }
        String [][] frontends = sql """ show frontends """
        for (def f in frontends) {
            host_ips.add(f[1])
        }
        dispatchTrinoConnectors(host_ips.unique())

        def kafkaCsvTpoics = [
                "trino_kafka_basic_data"
            ]
        String kafka_port = context.config.otherConfigs.get("kafka_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        def kafka_broker = "${externalEnvIp}:${kafka_port}"

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


        // create trino-connector catalog
        String catalog_name = "test_trino_kafka_base_catalog"
        String db_name = "test_trino_kafka_base_db"
        String basic_data_table = "trino_kafka_basic_data"

        sql """drop catalog if exists ${catalog_name}"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                "type"="trino-connector",
                "trino.connector.name"="kafka",
                "trino.kafka.table-names"="${db_name}.${basic_data_table}",
                "trino.kafka.nodes"="${externalEnvIp}:${kafka_port}",
                "trino.kafka.table-description-dir" = "${context.file.parent}/table_desc"
            );
        """
        sql """use `${catalog_name}`.`${db_name}`"""

        // desc and select
        order_qt_basic_desc """ desc ${basic_data_table} """
        order_qt_basic_data """ select * from ${basic_data_table} order by k00"""
    }
}