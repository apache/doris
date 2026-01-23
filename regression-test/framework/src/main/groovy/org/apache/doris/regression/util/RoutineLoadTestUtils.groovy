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

package org.apache.doris.regression.util

import groovy.json.JsonSlurper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.junit.Assert
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class RoutineLoadTestUtils {
    private static final Logger logger = LoggerFactory.getLogger(RoutineLoadTestUtils.class)

    static boolean isKafkaTestEnabled(context) {
        String enabled = context.config.otherConfigs.get("enableKafkaTest")
        return enabled != null && enabled.equalsIgnoreCase("true")
    }

    static String getKafkaBroker(context) {
        String kafka_port = context.config.otherConfigs.get("kafka_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        return "${externalEnvIp}:${kafka_port}"
    }

    static KafkaProducer createKafkaProducer(String kafkaBroker) {
        def props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000")
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
        def producer = new KafkaProducer<>(props)
        def verifyKafkaConnection = { prod ->
            try {
                logger.info("=====try to connect Kafka========")
                def partitions = prod.partitionsFor("__connection_verification_topic")
                return partitions != null
            } catch (Exception e) {
                throw new Exception("Kafka connect fail: ${e.message}".toString())
            }
        }
        try {
            logger.info("Kafka connecting: ${kafkaBroker}")
            if (!verifyKafkaConnection(producer)) {
                throw new Exception("can't get any kafka info")
            }
        } catch (Exception e) {
            logger.error("FATAL: " + e.getMessage())
            producer.close()
            throw e
        }
        logger.info("Kafka connect success")
        return producer
    }

    static void sendTestDataToKafka(KafkaProducer producer, List<String> topics, List<String> testData = null) {
        if (testData == null) {
            testData = [
                "9,\\N,2023-07-15,def,2023-07-20T05:48:31,ghi",
                "10,,2023-07-15,def,2023-07-20T05:48:31,ghi"
            ]
        }
        for (String topic in topics) {
            testData.each { line ->
                logger.info("Sending data to kafka: ${line}")
                def record = new ProducerRecord<>(topic, null, line)
                producer.send(record)
            }
        }
    }

    static void checkTaskTimeout(Closure sqlRunner, String jobName, String expectedTimeout, int maxAttempts = 60) {
        def count = 0
        while (true) {
            def res = sqlRunner.call("SHOW ROUTINE LOAD TASK WHERE JobName = '${jobName}'")
            if (res.size() > 0) {
                logger.info("res: ${res[0].toString()}")
                logger.info("timeout: ${res[0][6].toString()}")
                Assert.assertEquals(res[0][6].toString(), expectedTimeout)
                break;
            }
            if (count > maxAttempts) {
                Assert.assertEquals(1, 2)
                break;
            } else {
                sleep(1000)
                count++
            }
        }
    }

    static int waitForTaskFinish(Closure sqlRunner, String job, String tableName, int expectedMinRows = 0, int maxAttempts = 60) {
        return waitForTaskFinishInternal(sqlRunner, job, tableName, expectedMinRows, maxAttempts, false)
    }

    /**
     * Wait for routine load task to finish for MOW (Merge-on-Write) unique key tables.
     * Uses skip_delete_bitmap=true to properly count rows during partial update operations.
     */
    static int waitForTaskFinishMoW(Closure sqlRunner, String job, String tableName, int expectedMinRows = 0, int maxAttempts = 60) {
        return waitForTaskFinishInternal(sqlRunner, job, tableName, expectedMinRows, maxAttempts, true)
    }

    private static int waitForTaskFinishInternal(Closure sqlRunner, String job, String tableName, int expectedMinRows, int maxAttempts, boolean isMoW) {
        def count = 0
        while (true) {
            def res = sqlRunner.call("show routine load for ${job}")
            def routineLoadState = res[0][8].toString()
            def statistic = res[0][14].toString()
            logger.info("Routine load state: ${routineLoadState}")
            logger.info("Routine load statistic: ${statistic}")
            def rowCount
            if (isMoW) {
                // For MOW tables, use skip_delete_bitmap to properly count rows
                sqlRunner.call("set skip_delete_bitmap=true")
                sqlRunner.call("set skip_delete_sign=true")
                sqlRunner.call("sync")
                rowCount = sqlRunner.call("select count(*) from ${tableName}")
                sqlRunner.call("set skip_delete_bitmap=false")
                sqlRunner.call("set skip_delete_sign=false")
            } else {
                rowCount = sqlRunner.call("select count(*) from ${tableName}")
            }
            if (routineLoadState == "RUNNING" && rowCount[0][0] > expectedMinRows) {
                break
            }
            if (count > maxAttempts) {
                Assert.assertEquals(1, 2)
                break;
            } else {
                sleep(1000)
                count++
            }
        }
        return count
    }

    static void waitForTaskAbort(Closure sqlRunner, String job, int maxAttempts = 60, int expectedAbortedTaskNum = 1) {
        def count = 0
        while (true) {
            def res = sqlRunner.call("show routine load for ${job}")
            def statistic = res[0][14].toString()
            logger.info("Routine load statistic: ${statistic}")
            def jsonSlurper = new JsonSlurper()
            def json = jsonSlurper.parseText(res[0][14])
            if (json.abortedTaskNum > expectedAbortedTaskNum) {
                break
            }
            if (count > maxAttempts) {
                Assert.assertEquals(1, 2)
                break;
            } else {
                sleep(1000)
                count++
            }
        }
    }

    static void checkTxnTimeoutMatchesTaskTimeout(Closure sqlRunner, String jobName, String expectedTimeoutMs, int maxAttempts = 60) {
        def count = 0
        while (true) {
            def taskRes = sqlRunner.call("SHOW ROUTINE LOAD TASK WHERE JobName = '${jobName}'")
            if (taskRes.size() > 0) {
                def txnId = taskRes[0][1].toString()
                logger.info("Task txnId: ${txnId}, task timeout: ${taskRes[0][6].toString()}")
                if (txnId != null && txnId != "null" && txnId != "-1") {
                    // Get transaction timeout from SHOW TRANSACTION
                    def txnRes = sqlRunner.call("SHOW TRANSACTION WHERE id = ${txnId}")
                    if (txnRes.size() > 0) {
                        def txnTimeoutMs = txnRes[0][13].toString()
                        logger.info("Transaction timeout (ms): ${txnTimeoutMs}, expected: ${expectedTimeoutMs}")
                        Assert.assertEquals(expectedTimeoutMs, txnTimeoutMs)
                        break
                    }
                }
            }
            if (count > maxAttempts) {
                Assert.fail("Timeout waiting for task and transaction to be created")
                break
            } else {
                sleep(1000)
                count++
            }
        }
    }
}
