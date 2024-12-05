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

suite("test_dml_routine_load_auth","p0,auth_call") {

    String user = 'test_dml_routine_load_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_dml_routine_load_auth_db'
    String tableName = 'test_dml_routine_load_auth_tb'
    String labelName = 'test_dml_routine_load_auth_label'

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
        String topic = "zfr_test_dml_routine_load_auth_topic"
        def props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "${kafka_broker}".toString())
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        def producer = new KafkaProducer<>(props)
        def filepath = getLoalFilePath "routine_load_data.csv"
        def txt = new File("${filepath}").text
        def lines = txt.readLines()
        lines.each { line ->
            logger.info("=====${line}========")
            def record = new ProducerRecord<>(topic, null, line)
            producer.send(record)
        }
        sql "drop table if exists ${tableName}"
        sql """create table  ${dbName}.${tableName}(a int,c double generated always as (abs(a+b)) not null,b int, d int generated always as(c+1))
            DISTRIBUTED BY HASH(a)
            PROPERTIES("replication_num" = "1");
            ;"""

        connect(user = user, password = "${pwd}", url = context.config.jdbcUrl) {
            test {
                sql """
                CREATE ROUTINE LOAD ${dbName}.${labelName} ON ${tableName}
                COLUMNS(a,b),
                COLUMNS TERMINATED BY ","
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

        sql """grant load_priv on ${dbName}.${tableName} to ${user}"""
        connect(user = user, password = "${pwd}", url = context.config.jdbcUrl) {
            sql """
                CREATE ROUTINE LOAD ${dbName}.${labelName} ON ${tableName}
                COLUMNS(a,b),
                COLUMNS TERMINATED BY ","
                FROM KAFKA
                (
                    "kafka_broker_list" = "${externalEnvIp}:${kafka_port}",
                    "kafka_topic" = "${topic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
                """
        }
        sql """revoke load_priv on ${dbName}.${tableName} from ${user}"""

        connect(user = user, password = "${pwd}", url = context.config.jdbcUrl) {
            test {
                sql """
                ALTER ROUTINE LOAD FOR ${dbName}.${labelName}
                    PROPERTIES
                    (
                        "desired_concurrent_number" = "1"
                    );
                """
                exception "denied"
            }
            test {
                sql """PAUSE ROUTINE LOAD FOR ${dbName}.${labelName};"""
                exception "denied"
            }
            test {
                sql """RESUME ROUTINE LOAD FOR ${dbName}.${labelName};"""
                exception "denied"
            }
            test {
                sql """STOP ROUTINE LOAD FOR ${dbName}.${labelName};"""
                exception "denied"
            }
            test {
                sql """show routine load for ${dbName}.${labelName}"""
                exception "no job"
            }
        }
        sql """grant load_priv on ${dbName}.${tableName} to ${user}"""
        connect(user = user, password = "${pwd}", url = context.config.jdbcUrl) {
            sql """PAUSE ROUTINE LOAD FOR ${dbName}.${labelName};"""
            sql """
            ALTER ROUTINE LOAD FOR ${dbName}.${labelName}
                PROPERTIES
                (
                    "desired_concurrent_number" = "1"
                );
            """
            sql """RESUME ROUTINE LOAD FOR ${dbName}.${labelName};"""
            sql """STOP ROUTINE LOAD FOR ${dbName}.${labelName};"""
            sql """use ${dbName};"""
            def res = sql """show all routine load"""
            assertTrue(res.size() == 1)
        }
        sql """revoke load_priv on ${dbName}.${tableName} from ${user}"""
    }

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
