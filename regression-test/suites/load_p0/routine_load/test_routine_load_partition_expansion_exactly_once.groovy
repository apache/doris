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
import org.apache.doris.regression.util.RoutineLoadTestUtils
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.Assert
import java.util.Collections

suite("test_routine_load_partition_expansion_exactly_once", "docker") {
    def topicSuffix = System.currentTimeMillis()
    String kafkaCsvTopic = "test_rl_partition_expansion_${topicSuffix}"

    if (!RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        return
    }

    def kafkaBroker = RoutineLoadTestUtils.getKafkaBroker(context)

    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = false

    docker(options) {
        def producer = RoutineLoadTestUtils.createKafkaProducer(kafkaBroker)

        def adminProps = new Properties()
        adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker)
        def adminClient = AdminClient.create(adminProps)

        def tableName = "test_rl_partition_expansion"
        def job = "test_rl_partition_expansion_job_${topicSuffix}"

        def waitForExactRowCount = { long expected, int maxWait = 180 ->
            def waited = 0
            while (waited < maxWait) {
                def state = sql "SHOW ROUTINE LOAD FOR ${job}"
                def rowCount = sql "SELECT count(*) FROM ${tableName}"
                long count = (rowCount[0][0] as Number).longValue()
                logger.info("waitForExactRowCount: state={}, rows={}", state[0][8].toString(), count)
                if (state[0][8].toString() == "RUNNING" && count == expected) {
                    return
                }
                sleep(1000)
                waited++
            }
            Assert.fail("Row count did not reach ${expected} within timeout")
        }

        def waitForPartitionCount = { int expected, int maxWait = 60 ->
            def waited = 0
            while (waited < maxWait) {
                def topicDesc = adminClient.describeTopics([kafkaCsvTopic]).values().get(kafkaCsvTopic).get()
                if (topicDesc.partitions().size() == expected) {
                    return
                }
                sleep(1000)
                waited++
            }
            Assert.fail("Kafka topic partition count did not reach ${expected}")
        }

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `id` int NOT NULL,
                `payload` string NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        sql "sync"

        try {
            adminClient.createTopics([new NewTopic(kafkaCsvTopic, 1, (short) 1)]).all().get()
            waitForPartitionCount(1)

            sql """
                CREATE ROUTINE LOAD ${job} ON ${tableName}
                COLUMNS TERMINATED BY ","
                PROPERTIES
                (
                    "max_batch_interval" = "5"
                )
                FROM KAFKA
                (
                    "kafka_broker_list" = "${kafkaBroker}",
                    "kafka_topic" = "${kafkaCsvTopic}",
                    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
                );
            """

            (1..4).each { i ->
                producer.send(new ProducerRecord<>(kafkaCsvTopic, 0, null, "${i},payload_${i}".toString())).get()
            }
            producer.flush()
            waitForExactRowCount(4L)

            adminClient.createPartitions(Collections.singletonMap(kafkaCsvTopic, NewPartitions.increaseTo(3))).all().get()
            waitForPartitionCount(3)

            producer.send(new ProducerRecord<>(kafkaCsvTopic, 1, null, "5,payload_5")).get()
            producer.send(new ProducerRecord<>(kafkaCsvTopic, 2, null, "6,payload_6")).get()
            producer.send(new ProducerRecord<>(kafkaCsvTopic, 1, null, "7,payload_7")).get()
            producer.send(new ProducerRecord<>(kafkaCsvTopic, 2, null, "8,payload_8")).get()
            producer.send(new ProducerRecord<>(kafkaCsvTopic, 0, null, "9,payload_9")).get()
            producer.flush()

            waitForExactRowCount(9L, 240)

            def finalRows = sql "SELECT id, payload FROM ${tableName} ORDER BY id"
            Assert.assertEquals(9, finalRows.size())
            for (int i = 0; i < 9; i++) {
                Assert.assertEquals(i + 1, finalRows[i][0])
                Assert.assertEquals("payload_${i + 1}".toString(), finalRows[i][1])
            }

            def aggregate = sql "SELECT count(*), count(distinct id), sum(id) FROM ${tableName}"
            Assert.assertEquals(9L, (aggregate[0][0] as Number).longValue())
            Assert.assertEquals(9L, (aggregate[0][1] as Number).longValue())
            Assert.assertEquals(45L, (aggregate[0][2] as Number).longValue())

            def finalState = sql "SHOW ROUTINE LOAD FOR ${job}"
            Assert.assertEquals("RUNNING", finalState[0][8].toString())
        } finally {
            try {
                sql "STOP ROUTINE LOAD FOR ${job}"
            } catch (Exception e) {
                logger.warn("Failed to stop routine load: {}", e.message)
            }
            producer.close()
            adminClient.close()
            sql "DROP TABLE IF EXISTS ${tableName}"
        }
    }
}
