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
    String enabled = context.config.otherConfigs.get("enableKinesisTest")
    def region = context.config.awsRegion ?: context.config.otherConfigs.get("awsRegion")
    def ak = context.config.awsAccessKey ?: context.config.otherConfigs.get("awsAccessKey")
    def sk = context.config.awsSecretKey ?: context.config.otherConfigs.get("awsSecretKey")

    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Skip ${name} case, enableKinesisTest is not true")
        return
    }

    if (!region || !ak || !sk) {
        logger.info("Skip ${name} case, missing AWS config: region=${region}, ak=${ak != null}, sk=${sk != null}")
        return
    }

    def suffix = UUID.randomUUID().toString().substring(0, 8)
    def streamName = "doris-test-pause-${suffix}"
    def tableName = "test_kinesis_pause_resume"
    def jobName = "test_kinesis_pause_resume_${suffix}"

    def credentials = new BasicAWSCredentials(ak, sk)
    def kinesisClient = AmazonKinesisClientBuilder.standard()
        .withRegion(region)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .build()

    def getJobState = {
        def result = sql "SHOW ROUTINE LOAD FOR ${jobName}"
        assertTrue(result.size() > 0, "SHOW ROUTINE LOAD returned empty result for job ${jobName}")
        return result[0][8].toString()
    }

    def waitForJobState = { String expectedState, int timeoutSec ->
        def lastState = "UNKNOWN"
        for (int i = 0; i < timeoutSec; i++) {
            lastState = getJobState()
            if (lastState == expectedState) {
                logger.info("Routine load job ${jobName} reached state ${expectedState}")
                return
            }
            Thread.sleep(1000)
        }
        assertTrue(false, "Timeout waiting job ${jobName} to reach state ${expectedState}, last state=${lastState}")
    }

    def writeRange = { int startId, int endId ->
        logger.info("Writing records ${startId}-${endId} to stream ${streamName}")
        for (int i = startId; i <= endId; i++) {
            def data = "{\"id\": ${i}, \"name\": \"user_${i}\", \"age\": ${20 + i}}"
            def putRequest = new PutRecordRequest()
                .withStreamName(streamName)
                .withPartitionKey("key_${i}")
                .withData(ByteBuffer.wrap(data.getBytes("UTF-8")))
            for (int retry = 0; retry < 20; retry++) {
                try {
                    kinesisClient.putRecord(putRequest)
                    break
                } catch (ResourceNotFoundException e) {
                    if (retry == 19) {
                        throw e
                    }
                    Thread.sleep(500)
                }
            }
        }
    }

    def queryCount = {
        def result = sql "SELECT COUNT(*) FROM ${tableName}"
        return ((Number) result[0][0]).longValue()
    }

    def waitForCountAtLeast = { long expectedCount, int timeoutSec ->
        long lastCount = -1
        for (int i = 0; i < timeoutSec; i++) {
            lastCount = queryCount()
            if (lastCount >= expectedCount) {
                logger.info("Table ${tableName} row count reached ${lastCount} (expected >= ${expectedCount})")
                return lastCount
            }
            Thread.sleep(1000)
        }
        assertTrue(false, "Timeout waiting row count >= ${expectedCount}, last count=${lastCount}")
    }

    try {
        logger.info("Creating Kinesis stream: ${streamName}")
        kinesisClient.createStream(new CreateStreamRequest()
            .withStreamName(streamName)
            .withShardCount(1))

        logger.info("Waiting for stream ${streamName} to become active")
        def describeRequest = new DescribeStreamRequest().withStreamName(streamName)
        def streamReady = false
        for (int i = 0; i < 60; i++) {
            try {
                def result = kinesisClient.describeStream(describeRequest)
                def description = result.getStreamDescription()
                if (description.getStreamStatus() == "ACTIVE" && !description.getShards().isEmpty()) {
                    streamReady = true
                    break
                }
            } catch (ResourceNotFoundException e) {
                // Metadata may not be visible immediately after create.
            }
            Thread.sleep(1000)
        }
        assertTrue(streamReady, "Stream ${streamName} failed to become active")

        writeRange(1, 30)

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                id INT,
                name VARCHAR(100),
                age INT
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """

        sql """
            CREATE ROUTINE LOAD ${jobName} ON ${tableName}
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

        waitForCountAtLeast(30, 120)

        sql "PAUSE ROUTINE LOAD FOR ${jobName}"
        waitForJobState("PAUSED", 60)

        writeRange(31, 60)

        long pausedCountBeforeWait = queryCount()
        Thread.sleep(8000)
        long pausedCountAfterWait = queryCount()
        logger.info("Row count while paused: before=${pausedCountBeforeWait}, after=${pausedCountAfterWait}")
        assertEquals(pausedCountBeforeWait, pausedCountAfterWait)

        sql "RESUME ROUTINE LOAD FOR ${jobName}"
        waitForCountAtLeast(60, 120)

        sql "STOP ROUTINE LOAD FOR ${jobName}"

    } finally {
        try {
            kinesisClient.deleteStream(new DeleteStreamRequest().withStreamName(streamName))
            logger.info("Deleted stream: ${streamName}")
        } catch (Exception e) {
            logger.warn("Failed to delete stream ${streamName}: ${e.message}")
        }
        kinesisClient.shutdown()
        sql "DROP TABLE IF EXISTS ${tableName}"
    }
}
