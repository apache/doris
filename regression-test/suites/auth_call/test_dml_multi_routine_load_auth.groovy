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

import org.junit.Assert;
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig

suite("test_dml_multi_routine_load_auth","p0,auth_call") {

    String user = 'test_dml_multi_routine_load_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_dml_multi_routine_load_auth_db'
    String tableName1 = 'test_dml_multi_routine_load_auth_tb1'
    String tableName2 = 'test_dml_multi_routine_load_auth_tb2'
    String labelName = 'test_dml_multi_routine_load_auth_label'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    sql """create database ${dbName}"""

    String enabled = context.config.otherConfigs.get("enableKafkaTest")
    String kafka_port = context.config.otherConfigs.get("kafka_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def kafka_broker = "${externalEnvIp}:${kafka_port}"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        // define kafka
        String topic = "zfr_test_dml_multi_routine_load_auth_topic"
        def props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${kafka_broker}".toString())
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        def producer = new KafkaProducer<>(props)
        def txt = new File("""${context.file.parent}/data/multi_table_csv.csv""").text
        def lines = txt.readLines()
        lines.each { line ->
            logger.info("=====${line}========")
            def record = new ProducerRecord<>(topic, null, line)
            producer.send(record)
        }

        sql """use ${dbName}"""
        sql "drop table if exists ${tableName1}"
        sql "drop table if exists ${tableName2}"
        sql new File("""${context.file.parent}/ddl/${tableName1}.sql""").text
        sql new File("""${context.file.parent}/ddl/${tableName2}.sql""").text

        connect(user = user, password = "${pwd}", url = context.config.jdbcUrl) {
            test {
                sql """
                CREATE ROUTINE LOAD ${dbName}.${labelName} 
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
                    "kafka_topic" = "${topic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
                """
                exception "denied"
            }
        }

        sql """grant load_priv on ${dbName}.${tableName1} to ${user}"""
        connect(user = user, password = "${pwd}", url = context.config.jdbcUrl) {
            test {
                sql """
                CREATE ROUTINE LOAD ${dbName}.${labelName} 
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
                    "kafka_topic" = "${topic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
                """
                exception "denied"
            }
        }
        sql """grant load_priv on ${dbName}.${tableName2} to ${user}"""
        connect(user = user, password = "${pwd}", url = context.config.jdbcUrl) {
            test {
                sql """
                CREATE ROUTINE LOAD ${dbName}.${labelName} 
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
                    "kafka_topic" = "${topic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
                """
                exception "denied"
            }
        }
        sql """grant load_priv on ${dbName}.* to ${user}"""
        connect(user = user, password = "${pwd}", url = context.config.jdbcUrl) {
            sql """
                CREATE ROUTINE LOAD ${dbName}.${labelName} 
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
                    "kafka_topic" = "${topic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );"""
        }
    }

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
