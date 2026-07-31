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
import org.apache.doris.regression.suite.ClusterOptions
import java.nio.ByteBuffer

suite("test_kinesis_routine_load_fe_restart", "docker") {
    def region = context.config.awsRegion ?: context.config.otherConfigs.get("awsRegion")
    def ak = context.config.awsAccessKey ?: context.config.otherConfigs.get("awsAccessKey")
    def sk = context.config.awsSecretKey ?: context.config.otherConfigs.get("awsSecretKey")

    if (!region || !ak || !sk) {
        logger.info("Skip ${name} case, missing AWS config: region=${region}, ak=${ak != null}, sk=${sk != null}")
        return
    }

    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    // Override image-baked JAVA_HOME that may point to a host-only path.
    options.feConfigs.add("JAVA_HOME=")
    docker(options) {
        def suffix = UUID.randomUUID().toString().substring(0, 8)
        def streamName = "doris-fe-restart-${suffix}"
        def tableName = "test_kinesis_fe_restart"
        def jobName = "test_kinesis_fe_restart_${suffix}"

        def credentials = new BasicAWSCredentials(ak, sk)
        def kinesisClient = AmazonKinesisClientBuilder.standard()
            .withRegion(region)
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .build()
        def streamCreated = false
        def tableCreated = false
        def jobCreated = false
        def frontendsStopped = false
        def masterFeIndex = -1

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

        def writeRange = { int startId, int endId ->
            logger.info("Writing records ${startId}-${endId} to stream ${streamName}")
            for (int i = startId; i <= endId; i++) {
                def data = "{\"id\": ${i}, \"value\": ${i * 100}}"
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

        def getJobState = {
            def result = sql "SHOW ROUTINE LOAD FOR ${jobName}"
            assertTrue(result.size() > 0, "SHOW ROUTINE LOAD returned empty result for ${jobName}")
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

        try {
            logger.info("Creating Kinesis stream: ${streamName}")
            kinesisClient.createStream(new CreateStreamRequest()
                .withStreamName(streamName)
                .withShardCount(1))
            streamCreated = true
            waitForStreamReady(120)

            sql "DROP TABLE IF EXISTS ${tableName}"
            sql """
                CREATE TABLE ${tableName} (
                    id INT,
                    value INT
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            tableCreated = true

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
            jobCreated = true
            waitForJobStateIn(["RUNNING", "NEED_SCHEDULE"] as Set<String>, 120)

            writeRange(1, 50)
            long beforeRestartCount = waitForCountAtLeast(50, 120)
            logger.info("Loaded rows before FE restart: ${beforeRestartCount}")
            assertEquals(50L, beforeRestartCount)

            masterFeIndex = cluster.getMasterFe().index
            logger.info("Stopping master FE index=${masterFeIndex}")
            frontendsStopped = true
            try {
                cluster.stopFrontends(masterFeIndex)
            } catch (Exception e) {
                // In single-FE docker cluster, stop may timeout while checking FE liveness.
                // The FE container can already be stopped at this point, so continue the restart flow.
                logger.warn("Stop master FE index=${masterFeIndex} returned exception, continue restart flow: ${e.message}")
            }

            writeRange(51, 100)

            logger.info("Starting master FE index=${masterFeIndex}")
            cluster.startFrontends(masterFeIndex)
            frontendsStopped = false
            Thread.sleep(30000)
            context.reconnectFe()

            def stateAfterRestart = waitForJobStateIn(["RUNNING", "NEED_SCHEDULE", "PAUSED"] as Set<String>, 120)
            logger.info("Routine load state after FE restart: ${stateAfterRestart}")
            assertNotEquals("CANCELLED", stateAfterRestart)

            long finalCount = waitForCountAtLeast(100, 180)
            logger.info("Loaded rows after FE restart: ${finalCount}")
            assertEquals(100L, finalCount)
            def result = sql "SELECT COUNT(*), COUNT(DISTINCT id), MIN(id), MAX(id), SUM(value) FROM ${tableName}"
            assertEquals(100L, ((Number) result[0][0]).longValue())
            assertEquals(100L, ((Number) result[0][1]).longValue())
            assertEquals(1, ((Number) result[0][2]).intValue())
            assertEquals(100, ((Number) result[0][3]).intValue())
            assertEquals(505000L, ((Number) result[0][4]).longValue())

        } finally {
            if (frontendsStopped) {
                try {
                    if (masterFeIndex > 0) {
                        cluster.startFrontends(masterFeIndex)
                    } else {
                        cluster.startFrontends()
                    }
                    Thread.sleep(30000)
                    context.reconnectFe()
                    frontendsStopped = false
                } catch (Exception e) {
                    logger.warn("Failed to restart FE in cleanup: ${e.message}")
                }
            }
            if (jobCreated) {
                try {
                    sql "STOP ROUTINE LOAD FOR ${jobName}"
                } catch (Exception e) {
                    logger.warn("Failed to stop routine load ${jobName}: ${e.message}")
                }
            }
            if (streamCreated) {
                try {
                    kinesisClient.deleteStream(new DeleteStreamRequest().withStreamName(streamName))
                    logger.info("Deleted stream: ${streamName}")
                } catch (Exception e) {
                    logger.warn("Failed to delete stream ${streamName}: ${e.message}")
                }
            }
            kinesisClient.shutdown()
            if (tableCreated) {
                try {
                    sql "DROP TABLE IF EXISTS ${tableName}"
                } catch (Exception e) {
                    logger.warn("Failed to drop table ${tableName}: ${e.message}")
                }
            }
        }
    }
}
