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
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

suite("test_adaptive_random_bucket_routine_load", "docker") {
    if (!RoutineLoadTestUtils.isKafkaTestEnabled(context)) {
        return
    }

    def kafkaBroker = RoutineLoadTestUtils.getKafkaBroker(context)
    def topic = "test_adaptive_random_bucket_routine_load_${UUID.randomUUID()}".toString()
    def job = "test_adaptive_random_bucket_routine_load_job"
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(2)
    options.cloudMode = true
    options.feConfigs += [
        'enable_adaptive_random_bucket_load=true',
        'max_routine_load_task_num_per_be=1'
    ]

    docker(options) {
        sql "DROP TABLE IF EXISTS test_adaptive_random_bucket_routine_load"
        // Two concurrent Kafka tasks must use different BEs. With only one tablet,
        // one task has to route to a BE other than the one executing the task.
        sql """
            CREATE TABLE test_adaptive_random_bucket_routine_load (
                k INT NOT NULL,
                v STRING
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """

        def adminProps = new Properties()
        adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker)
        def adminClient = AdminClient.create(adminProps)
        try {
            adminClient.createTopics([new NewTopic(topic, 2, (short) 1)]).all().get()
            try {
                def producer = RoutineLoadTestUtils.createKafkaProducer(kafkaBroker)
                try {
                    def sendBatch = { int firstId ->
                        for (int partition = 0; partition < 2; partition++) {
                            for (int i = 0; i < 3; i++) {
                                int id = firstId + partition * 3 + i
                                producer.send(new ProducerRecord<>(topic, partition, null,
                                        "${id},value_${id}".toString())).get()
                            }
                        }
                        producer.flush()
                    }

                    sendBatch(1)
                    sql """
                        CREATE ROUTINE LOAD ${job} ON test_adaptive_random_bucket_routine_load
                        COLUMNS TERMINATED BY ","
                        PROPERTIES (
                            "desired_concurrent_number" = "2",
                            "max_batch_interval" = "5",
                            "load_to_single_tablet" = "false"
                        )
                        FROM KAFKA (
                            "kafka_broker_list" = "${kafkaBroker}",
                            "kafka_topic" = "${topic}",
                            "kafka_partitions" = "0,1",
                            "kafka_offsets" = "OFFSET_BEGINNING,OFFSET_BEGINNING",
                            "property.enable.partition.eof" = "false"
                        )
                    """
                    try {
                        def checkJob = {
                            def state = sql_return_maparray "SHOW ROUTINE LOAD FOR ${job}"
                            logger.info("Routine load status: ${state}")
                            assertTrue(state[0].State in ["NEED_SCHEDULE", "RUNNING"],
                                    "Routine load stopped unexpectedly: ${state}")
                            // Do not let a retry on the tablet owner hide a routing failure.
                            assertFalse(state[0].OtherMsg.toString().contains("unknown partition channel"),
                                    "Adaptive routing failed: ${state}")
                        }

                        // Disable early EOF above so both tasks stay active for the batch interval,
                        // allowing their BE assignments to be observed before they are renewed.
                        awaitUntil(60) {
                            checkJob()
                            def tasks = sql_return_maparray "SHOW ROUTINE LOAD TASK WHERE JobName = '${job}'"
                            def backendIds = tasks.collect { it.BeId as long }.findAll { it > 0 }.toSet()
                            backendIds.size() == 2
                        }

                        def waitForRows = { int expected ->
                            awaitUntil(120) {
                                checkJob()
                                def rows = sql "SELECT count(*) FROM test_adaptive_random_bucket_routine_load"
                                rows[0][0] == expected
                            }
                        }
                        waitForRows(6)
                        order_qt_first_batch "SELECT k, v FROM test_adaptive_random_bucket_routine_load"

                        // Renewed tasks must receive the adaptive assignments as well.
                        sendBatch(7)
                        waitForRows(12)
                        order_qt_second_batch "SELECT k, v FROM test_adaptive_random_bucket_routine_load"
                    } finally {
                        sql "STOP ROUTINE LOAD FOR ${job}"
                    }
                } finally {
                    producer.close()
                }
            } finally {
                adminClient.deleteTopics([topic]).all().get()
            }
        } finally {
            adminClient.close()
        }
    }
}
