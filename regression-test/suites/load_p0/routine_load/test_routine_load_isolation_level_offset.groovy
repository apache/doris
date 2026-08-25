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

import org.apache.doris.regression.util.RoutineLoadTestUtils
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

suite("test_routine_load_isolation_level_offset", "nonConcurrent") {
    if (!RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        return
    }

    def kafkaBroker = RoutineLoadTestUtils.getKafkaBroker(context)
    def kafkaTopic = "test_routine_load_isolation_level_offset_${System.currentTimeMillis()}"
    def readCommittedJob = "test_isolation_offset_read_committed"
    def readUncommittedJob = "test_isolation_offset_read_uncommitted"

    sql "DROP TABLE IF EXISTS test_routine_load_isolation_level_offset"
    sql """
        CREATE TABLE test_routine_load_isolation_level_offset (
            k1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    def adminProperties = new Properties()
    adminProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker)
    def adminClient = AdminClient.create(adminProperties)
    try {
        adminClient.createTopics([new NewTopic(kafkaTopic, 1, (short) 1)]).all().get()
    } finally {
        adminClient.close()
    }

    def producerProperties = new Properties()
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker)
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")
    producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
            "${kafkaTopic}_transaction")
    producerProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000")
    producerProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")

    def producer = new KafkaProducer<>(producerProperties)
    try {
        producer.initTransactions()
        producer.beginTransaction()
        producer.send(new ProducerRecord<>(kafkaTopic, null, "1")).get()
        producer.flush()

        def createJob = { jobName, isolationLevel ->
            sql """
                CREATE ROUTINE LOAD ${jobName}
                ON test_routine_load_isolation_level_offset
                COLUMNS TERMINATED BY ","
                PROPERTIES ("desired_concurrent_number" = "1")
                FROM KAFKA (
                    "kafka_broker_list" = "${kafkaBroker}",
                    "kafka_topic" = "${kafkaTopic}",
                    "property.kafka_default_offsets" = "OFFSET_END",
                    "property.isolation.level" = "${isolationLevel}"
                )
            """
        }

        createJob(readCommittedJob, "read_committed")
        createJob(readUncommittedJob, "read_uncommitted")
        sql "PAUSE ROUTINE LOAD FOR ${readCommittedJob}"
        sql "PAUSE ROUTINE LOAD FOR ${readUncommittedJob}"

        def readCommittedInfo = sql "SHOW ROUTINE LOAD FOR ${readCommittedJob}"
        def readUncommittedInfo = sql "SHOW ROUTINE LOAD FOR ${readUncommittedJob}"
        assertEquals("OFFSET_ZERO", parseJson(readCommittedInfo[0][15])["0"])
        assertEquals("0", parseJson(readUncommittedInfo[0][15])["0"])

        sql """
            ALTER ROUTINE LOAD FOR ${readCommittedJob}
            FROM KAFKA ("kafka_partitions" = "0", "kafka_offsets" = "0")
        """
        sql """
            ALTER ROUTINE LOAD FOR ${readUncommittedJob}
            FROM KAFKA ("kafka_partitions" = "0", "kafka_offsets" = "0")
        """

        def attempts = 0
        while (true) {
            readCommittedInfo = sql "SHOW ROUTINE LOAD FOR ${readCommittedJob}"
            readUncommittedInfo = sql "SHOW ROUTINE LOAD FOR ${readUncommittedJob}"
            def readCommittedLag = parseJson(readCommittedInfo[0][16])["0"]
            def readUncommittedLag = parseJson(readUncommittedInfo[0][16])["0"]
            if (readCommittedLag == 0 && readUncommittedLag == 1) {
                break
            }
            if (attempts++ >= 60) {
                assertEquals(0, readCommittedLag)
                assertEquals(1, readUncommittedLag)
            }
            sleep(1000)
        }
    } finally {
        try_sql "STOP ROUTINE LOAD FOR ${readCommittedJob}"
        try_sql "STOP ROUTINE LOAD FOR ${readUncommittedJob}"
        try {
            producer.abortTransaction()
        } finally {
            producer.close()
        }
    }
}
