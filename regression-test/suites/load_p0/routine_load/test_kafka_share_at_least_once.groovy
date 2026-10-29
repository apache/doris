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

import groovy.json.JsonSlurper
import org.apache.doris.regression.util.RoutineLoadTestUtils
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import java.util.concurrent.TimeUnit

suite("test_kafka_share_at_least_once", "kafka_share,nonConcurrent") {
    def kafkaTrunkHome = context.config.otherConfigs.get("kafkaTrunkHome")
    if (!RoutineLoadTestUtils.isKafkaTestEnabled(context) || !kafkaTrunkHome) {
        logger.info("Skipping Kafka share regression: enableKafkaTest and kafkaTrunkHome are required")
        return
    }

    def kafkaBroker = RoutineLoadTestUtils.getKafkaBroker(context)
    def suffix = UUID.randomUUID().toString().replace("-", "_")
    def topic = "doris_share_alo_${suffix}".toString()
    def group = "doris_share_alo_${suffix}".toString()
    def job = "share_alo_${suffix}".toString()
    def injection = "DatabaseTransactionMgr.commitTransaction.failed"
    def clientProperties = new Properties()
    clientProperties.put("bootstrap.servers", kafkaBroker)
    def admin = AdminClient.create(clientProperties)
    boolean topicCreated = false
    boolean jobCreated = false
    try {
        admin.createTopics([new NewTopic(topic, 1, (short) 1)]).all().get(30, TimeUnit.SECONDS)
        topicCreated = true
        def configOutput = File.createTempFile("doris-share-config-", ".log")
        try {
            def command = [new File(kafkaTrunkHome.toString(), "bin/kafka-configs.sh").path,
                    "--bootstrap-server", kafkaBroker, "--entity-type", "groups",
                    "--entity-name", group, "--alter", "--add-config",
                    "share.auto.offset.reset=earliest,share.isolation.level=read_committed"]
            def process = new ProcessBuilder(command.collect { it.toString() })
                    .redirectErrorStream(true).redirectOutput(configOutput).start()
            if (!process.waitFor(60, TimeUnit.SECONDS)) {
                process.destroyForcibly().waitFor(5, TimeUnit.SECONDS)
                throw new IllegalStateException("Kafka trunk group configuration timed out")
            }
            assertEquals(0, process.exitValue(), configOutput.text)
        } finally {
            configOutput.delete()
        }

        sql "DROP TABLE IF EXISTS test_kafka_share_at_least_once"
        sql """
            CREATE TABLE test_kafka_share_at_least_once (
                id INT NOT NULL,
                payload STRING NOT NULL
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """

        def producerProperties = new Properties()
        producerProperties.putAll(clientProperties)
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        producerProperties.put("acks", "all")
        def producer = new KafkaProducer<String, String>(producerProperties)
        try {
            def sendBatch = { int firstId, int lastId ->
                for (int id = firstId; id <= lastId; ++id) {
                    producer.send(new ProducerRecord<String, String>(topic, "${id},value_${id}".toString()))
                            .get(30, TimeUnit.SECONDS)
                }
                producer.flush()
            }
            def waitForRows = { int expected ->
                awaitUntil(120) {
                    def status = sql_return_maparray "SHOW ROUTINE LOAD FOR ${job}"
                    assertTrue(status[0].State in ["NEED_SCHEDULE", "RUNNING"], "${status}")
                    def rows = sql """
                        SELECT id, min(payload), max(payload)
                        FROM test_kafka_share_at_least_once GROUP BY id ORDER BY id
                    """
                    rows == (1..expected).collect { [it, "value_${it}".toString(), "value_${it}".toString()] }
                }
            }

            try {
                GetDebugPoint().enableDebugPointForAllFEs(injection, [execute: 1])
                sendBatch(1, 20)
                sql """
                    CREATE ROUTINE LOAD ${job} ON test_kafka_share_at_least_once
                    COLUMNS TERMINATED BY ","
                    PROPERTIES (
                        "desired_concurrent_number" = "3",
                        "max_batch_interval" = "5",
                        "max_error_number" = "0",
                        "max_filter_ratio" = "0",
                        "strict_mode" = "true"
                    )
                    FROM KAFKA (
                        "kafka_broker_list" = "${kafkaBroker}",
                        "kafka_topic" = "${topic}",
                        "kafka_consumer_type" = "share",
                        "property.group.id" = "${group}"
                    )
                """
                jobCreated = true
                awaitUntil(60) {
                    def status = sql_return_maparray "SHOW ROUTINE LOAD FOR ${job}"
                    new JsonSlurper().parseText(status[0].Statistic).abortedTaskNum > 0 &&
                            status[0].OtherMsg.toString().contains(injection)
                }
            } finally {
                GetDebugPoint().disableDebugPointForAllFEs(injection)
            }
            waitForRows(20)
            awaitUntil(60) {
                def tasks = sql_return_maparray "SHOW ROUTINE LOAD TASK WHERE JobName = '${job}'"
                tasks.count { (it.BeId as long) > 0 } > 1
            }

            sql "PAUSE ROUTINE LOAD FOR ${job}"
            awaitUntil(30) {
                (sql_return_maparray "SHOW ROUTINE LOAD FOR ${job}")[0].State == "PAUSED"
            }
            sendBatch(21, 40)
            sql "RESUME ROUTINE LOAD FOR ${job}"
            waitForRows(40)
        } finally {
            producer.close()
        }
    } finally {
        try {
            if (jobCreated) {
                sql "STOP ROUTINE LOAD FOR ${job}"
            }
        } finally {
            try {
                if (topicCreated) {
                    admin.deleteTopics([topic]).all().get(30, TimeUnit.SECONDS)
                }
            } finally {
                admin.close()
            }
        }
    }
}
