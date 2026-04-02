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

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.model.*
import java.nio.ByteBuffer

suite("test_kinesis_routine_load_pause_resume", "nonConcurrent") {
    def region = context.config.awsRegion ?: context.config.otherConfigs.get("awsRegion")
    def ak = context.config.awsAccessKey ?: context.config.otherConfigs.get("awsAccessKey")
    def sk = context.config.awsSecretKey ?: context.config.otherConfigs.get("awsSecretKey")

    if (!region || !ak || !sk) {
        logger.info("Skip ${name} case, missing AWS config: region=${region}, ak=${ak != null}, sk=${sk != null}")
        return
    }

    def suffix = UUID.randomUUID().toString().substring(0, 8)
    def streamName = "doris-test-pause-${suffix}"
    def jobName = "test_kinesis_pause_resume_${suffix}"

    def credentials = new BasicAWSCredentials(ak, sk)
    def kinesisClient = AmazonKinesisClientBuilder.standard()
        .withRegion(region)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .build()

    def waitForStreamReady = { int timeoutSec ->
        logger.info("Waiting for stream ${streamName} to become active")
        def describeRequest = new DescribeStreamRequest().withStreamName(streamName)
        for (int i = 0; i < timeoutSec; i++) {
            try {
                def result = kinesisClient.describeStream(describeRequest)
                def description = result.getStreamDescription()
                if (description.getStreamStatus() == "ACTIVE" && !description.getShards().isEmpty()) {
                    return
                }
            } catch (ResourceNotFoundException e) {
                // Metadata may not be visible immediately after create.
            }
            Thread.sleep(1000)
        }
        assertTrue(false, "Stream ${streamName} failed to become active")
    }

    def getJobState = {
        def result = sql "SHOW ROUTINE LOAD FOR ${jobName}"
        assertTrue(result.size() > 0, "SHOW ROUTINE LOAD returned empty result for job ${jobName}")
        return result[0][8].toString()
    }

    def waitForJobStateIn = { Set<String> expectedStates, int timeoutSec ->
        def lastState = "UNKNOWN"
        for (int i = 0; i < timeoutSec; i++) {
            lastState = getJobState()
            if (expectedStates.contains(lastState)) {
                logger.info("Routine load job ${jobName} reached state ${lastState}")
                return lastState
            }
            Thread.sleep(1000)
        }
        assertTrue(false, "Timeout waiting job ${jobName} to reach states ${expectedStates}, last state=${lastState}")
    }

    def putRecordWithRetry = { String partitionKey, String data ->
        def putRequest = new PutRecordRequest()
            .withStreamName(streamName)
            .withPartitionKey(partitionKey)
            .withData(ByteBuffer.wrap(data.getBytes("UTF-8")))
        for (int retry = 0; retry < 20; retry++) {
            try {
                kinesisClient.putRecord(putRequest)
                return
            } catch (ResourceNotFoundException e) {
                if (retry == 19) {
                    throw e
                }
                Thread.sleep(500)
            }
        }
    }

    def writeRange = { int startId, int endId ->
        logger.info("Writing records ${startId}-${endId} to stream ${streamName}")
        for (int i = startId; i <= endId; i++) {
            def data = "{\"id\": ${i}, \"name\": \"user_${i}\", \"age\": ${20 + i}}"
            putRecordWithRetry("key_${i}", data)
        }
    }

    def queryCount = {
        def result = sql "SELECT COUNT(*) FROM test_kinesis_pause_resume"
        return ((Number) result[0][0]).longValue()
    }

    def waitForCountAtLeast = { long expectedCount, int timeoutSec ->
        long lastCount = -1
        for (int i = 0; i < timeoutSec; i++) {
            lastCount = queryCount()
            if (lastCount >= expectedCount) {
                logger.info("Table test_kinesis_pause_resume row count reached ${lastCount} (expected >= ${expectedCount})")
                return lastCount
            }
            Thread.sleep(1000)
        }
        assertTrue(false, "Timeout waiting row count >= ${expectedCount}, last count=${lastCount}")
    }

    def streamCreated = false
    def tableCreated = false
    def jobCreated = false
    def test1PreparedForReuse = false

    try {
        // test1 : 建流/建表/建作业，写入首批数据，PAUSE 后继续写入新数据并验证不消费
        try {
            logger.info("Creating Kinesis stream: ${streamName}")
            kinesisClient.createStream(new CreateStreamRequest()
                .withStreamName(streamName)
                .withShardCount(1))
            streamCreated = true
            waitForStreamReady(120)

            sql "DROP TABLE IF EXISTS test_kinesis_pause_resume"
            sql """
                CREATE TABLE test_kinesis_pause_resume (
                    id INT,
                    name VARCHAR(100),
                    age INT
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            tableCreated = true

            sql """
                CREATE ROUTINE LOAD ${jobName} ON test_kinesis_pause_resume
                PROPERTIES (
                    "format" = "json",
                    "desired_concurrent_number" = "1"
                )
                FROM KINESIS (
                    "aws.region" = "${region}",
                    "aws.access_key" = "${ak}",
                    "aws.secret_key" = "${sk}",
                    "kinesis_stream" = "${streamName}",
                    "property.kinesis_default_pos" = "TRIM_HORIZON"
                )
            """
            jobCreated = true

            waitForJobStateIn(["RUNNING", "NEED_SCHEDULE"] as Set<String>, 120)

            writeRange(1, 30)
            long loadedBeforePause = waitForCountAtLeast(30, 180)
            assertEquals(30L, loadedBeforePause)

            sql "PAUSE ROUTINE LOAD FOR ${jobName}"
            waitForJobStateIn(["PAUSED"] as Set<String>, 60)

            // PAUSE 期间继续向 stream 写入新数据
            writeRange(31, 60)

            long pausedCountBeforeWait = queryCount()
            Thread.sleep(10000)
            long pausedCountAfterWait = queryCount()
            logger.info("Row count while paused: before=${pausedCountBeforeWait}, after=${pausedCountAfterWait}")
            assertEquals(pausedCountBeforeWait, pausedCountAfterWait)

            test1PreparedForReuse = true
        } finally {
            if (!test1PreparedForReuse) {
                if (jobCreated) {
                    try {
                        sql "STOP ROUTINE LOAD FOR ${jobName}"
                    } catch (Exception e) {
                        logger.warn("Failed to stop routine load ${jobName}: ${e.message}")
                    }
                    jobCreated = false
                }
                if (streamCreated) {
                    try {
                        kinesisClient.deleteStream(new DeleteStreamRequest().withStreamName(streamName))
                        logger.info("Deleted stream: ${streamName}")
                    } catch (Exception e) {
                        logger.warn("Failed to delete stream ${streamName}: ${e.message}")
                    }
                    streamCreated = false
                }
                if (tableCreated) {
                    sql "DROP TABLE IF EXISTS test_kinesis_pause_resume"
                    tableCreated = false
                }
            }
        }

        // test2 : RESUME 后继续消费，校验最终完整性，再 STOP
        try {
            assertTrue(test1PreparedForReuse, "Test2 requires test1 to prepare paused stream/job")

            sql "RESUME ROUTINE LOAD FOR ${jobName}"
            waitForJobStateIn(["RUNNING", "NEED_SCHEDULE"] as Set<String>, 120)

            long finalCount = waitForCountAtLeast(60, 180)
            assertEquals(60L, finalCount)

            def result = sql "SELECT COUNT(*), COUNT(DISTINCT id), MIN(id), MAX(id) FROM test_kinesis_pause_resume"
            assertEquals(60L, ((Number) result[0][0]).longValue())
            assertEquals(60L, ((Number) result[0][1]).longValue())
            assertEquals(1, ((Number) result[0][2]).intValue())
            assertEquals(60, ((Number) result[0][3]).intValue())

            sql "STOP ROUTINE LOAD FOR ${jobName}"
            waitForJobStateIn(["STOPPED", "CANCELLED"] as Set<String>, 60)
            jobCreated = false
        } finally {
            if (jobCreated) {
                try {
                    sql "STOP ROUTINE LOAD FOR ${jobName}"
                } catch (Exception e) {
                    logger.warn("Failed to stop routine load ${jobName}: ${e.message}")
                }
                jobCreated = false
            }
            if (streamCreated) {
                try {
                    kinesisClient.deleteStream(new DeleteStreamRequest().withStreamName(streamName))
                    logger.info("Deleted stream: ${streamName}")
                } catch (Exception e) {
                    logger.warn("Failed to delete stream ${streamName}: ${e.message}")
                }
                streamCreated = false
            }
            if (tableCreated) {
                sql "DROP TABLE IF EXISTS test_kinesis_pause_resume"
                tableCreated = false
            }
        }
    } finally {
        kinesisClient.shutdown()
    }
}
