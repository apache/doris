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
import java.math.BigInteger
import java.nio.ByteBuffer

suite("test_kinesis_routine_load", "nonConcurrent") {
    def ak = context.config.awsAccessKey ?: context.config.otherConfigs.get("awsAccessKey")
    def sk = context.config.awsSecretKey ?: context.config.otherConfigs.get("awsSecretKey")
    def region = context.config.awsRegion ?: context.config.otherConfigs.get("awsRegion")

    if (!region || !ak || !sk) {
        logger.info("Skip ${name} case, AWS config not provided")
        return
    }

    def suffix = UUID.randomUUID().toString().substring(0, 8)
    def sharedStreamName = "doris-test-kinesis-shared-${suffix}"
    def sharedTableName = "kinesis_test_table_shared"
    def sharedJobName = "kinesis_routine_load_job_shared_${suffix}"
    def multiStreamName = "doris-test-kinesis-multi-${suffix}"
    def multiTableName = "kinesis_test_table_multi"
    def multiJobName = "kinesis_routine_load_job_multi_${suffix}"

    def credentials = new BasicAWSCredentials(ak, sk)
    def kinesisClient = AmazonKinesisClientBuilder.standard()
        .withRegion(region)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .build()

    def waitForStreamReady = { String streamName, int timeoutSec ->
        def describeRequest = new DescribeStreamRequest().withStreamName(streamName)
        for (int i = 0; i < timeoutSec; i++) {
            try {
                def result = kinesisClient.describeStream(describeRequest)
                def streamDesc = result.getStreamDescription()
                if (streamDesc.getStreamStatus() == "ACTIVE" && !streamDesc.getShards().isEmpty()) {
                    return streamDesc
                }
            } catch (ResourceNotFoundException e) {
                // Stream metadata may not be visible immediately after create.
            }
            Thread.sleep(1000)
        }
        assertTrue(false, "Stream ${streamName} failed to become ready")
    }

    def putRecordWithRetry = { String streamName, String partitionKey, String jsonData, String explicitHashKey ->
        def request = new PutRecordRequest()
            .withStreamName(streamName)
            .withPartitionKey(partitionKey)
            .withData(ByteBuffer.wrap(jsonData.getBytes("UTF-8")))
        if (explicitHashKey != null) {
            request.withExplicitHashKey(explicitHashKey)
        }
        for (int retry = 0; retry < 20; retry++) {
            try {
                kinesisClient.putRecord(request)
                return
            } catch (ResourceNotFoundException e) {
                if (retry == 19) {
                    throw e
                }
                Thread.sleep(500)
            }
        }
    }

    def waitForCountAtLeast = { String tableName, long expectedCount, int timeoutSec ->
        long lastCount = -1
        for (int i = 0; i < timeoutSec; i++) {
            def result = sql "SELECT COUNT(*) FROM ${tableName}"
            lastCount = ((Number) result[0][0]).longValue()
            if (lastCount >= expectedCount) {
                logger.info("Table ${tableName} row count reached ${lastCount} (expected >= ${expectedCount})")
                return lastCount
            }
            Thread.sleep(1000)
        }
        assertTrue(false, "Timeout waiting table ${tableName} count >= ${expectedCount}, last count=${lastCount}")
    }

    def sharedStreamCreated = false
    def sharedJobCreated = false
    def test1PreparedForReuse = false
    def multiStreamCreated = false
    def multiJobCreated = false

    try {
        // test 导入stream中已有数据
        try {
            logger.info("Test1 create stream: ${sharedStreamName}")
            kinesisClient.createStream(new CreateStreamRequest().withStreamName(sharedStreamName).withShardCount(1))
            sharedStreamCreated = true
            waitForStreamReady(sharedStreamName, 120)

            // stream 已有数据（先写后建 job）
            def firstData = "{\"id\": 1, \"name\": \"user_1\", \"age\": 21}"
            putRecordWithRetry(sharedStreamName, "shared_key_1", firstData, null)

            sql "DROP TABLE IF EXISTS ${sharedTableName}"
            sql """
                CREATE TABLE ${sharedTableName} (
                    id INT,
                    name VARCHAR(100),
                    age INT
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """

            sql """
                CREATE ROUTINE LOAD ${sharedJobName} ON ${sharedTableName}
                PROPERTIES ("format" = "json", "desired_concurrent_number" = "1")
                FROM KINESIS (
                    "aws.region" = "${region}",
                    "aws.access_key" = "${ak}",
                    "aws.secret_key" = "${sk}",
                    "kinesis_stream" = "${sharedStreamName}",
                    "property.kinesis_default_pos" = "TRIM_HORIZON"
                )
            """
            sharedJobCreated = true

            waitForCountAtLeast(sharedTableName, 1, 180)
            test1PreparedForReuse = true
        } finally {
            // test2 要复用 test1 的 stream/table/job，仅在 test1 失败时兜底清理
            if (!test1PreparedForReuse) {
                if (sharedJobCreated) {
                    try {
                        sql "STOP ROUTINE LOAD FOR ${sharedJobName}"
                    } catch (Exception e) {
                        logger.warn("Failed to stop shared routine load ${sharedJobName}: ${e.message}")
                    }
                    sharedJobCreated = false
                }
                if (sharedStreamCreated) {
                    try {
                        kinesisClient.deleteStream(new DeleteStreamRequest().withStreamName(sharedStreamName))
                    } catch (Exception e) {
                        logger.warn("Failed to delete shared stream ${sharedStreamName}: ${e.message}")
                    }
                    sharedStreamCreated = false
                }
                sql "DROP TABLE IF EXISTS ${sharedTableName}"
            }
        }

        // test 导入stream中新写入数据
        try {
            assertTrue(test1PreparedForReuse, "Test2 requires test1 to prepare shared stream/table/job")
            def secondData = "{\"id\": 2, \"name\": \"user_2\", \"age\": 22}"
            putRecordWithRetry(sharedStreamName, "shared_key_2", secondData, null)
            waitForCountAtLeast(sharedTableName, 2, 180)

            def dedupResult = sql "SELECT COUNT(*), COUNT(DISTINCT id), MIN(id), MAX(id) FROM ${sharedTableName}"
            assertEquals(2L, ((Number) dedupResult[0][0]).longValue())
            assertEquals(2L, ((Number) dedupResult[0][1]).longValue())
            assertEquals(1, ((Number) dedupResult[0][2]).intValue())
            assertEquals(2, ((Number) dedupResult[0][3]).intValue())
        } finally {
            if (sharedJobCreated) {
                try {
                    sql "STOP ROUTINE LOAD FOR ${sharedJobName}"
                } catch (Exception e) {
                    logger.warn("Failed to stop shared routine load ${sharedJobName}: ${e.message}")
                }
                sharedJobCreated = false
            }
            if (sharedStreamCreated) {
                try {
                    kinesisClient.deleteStream(new DeleteStreamRequest().withStreamName(sharedStreamName))
                    logger.info("Deleted shared stream: ${sharedStreamName}")
                } catch (Exception e) {
                    logger.warn("Failed to delete shared stream ${sharedStreamName}: ${e.message}")
                }
                sharedStreamCreated = false
            }
            sql "DROP TABLE IF EXISTS ${sharedTableName}"
        }

        // test 并行消费多分片的数据
        try {
            logger.info("Test3 create multi-shard stream: ${multiStreamName}")
            kinesisClient.createStream(new CreateStreamRequest().withStreamName(multiStreamName).withShardCount(2))
            multiStreamCreated = true
            def streamDesc = waitForStreamReady(multiStreamName, 120)
            def openShards = streamDesc.getShards().findAll {
                it.getSequenceNumberRange().getEndingSequenceNumber() == null
            }
            assertTrue(openShards.size() >= 2, "Expected at least 2 open shards, actual=${openShards.size()}")

            int totalRows = 0
            for (int shardIdx = 0; shardIdx < openShards.size(); shardIdx++) {
                def shard = openShards[shardIdx]
                def start = new BigInteger(shard.getHashKeyRange().getStartingHashKey())
                def end = new BigInteger(shard.getHashKeyRange().getEndingHashKey())
                def mid = start.add(end).divide(BigInteger.valueOf(2)).toString()
                for (int i = 1; i <= 25; i++) {
                    def id = shardIdx * 1000 + i
                    def data = "{\"id\": ${id}, \"name\": \"multi_${shardIdx}_${i}\", \"age\": ${30 + i % 10}}"
                    putRecordWithRetry(multiStreamName, "multi_key_${shardIdx}_${i}", data, mid)
                    totalRows++
                }
            }

            sql "DROP TABLE IF EXISTS ${multiTableName}"
            sql """
                CREATE TABLE ${multiTableName} (
                    id INT,
                    name VARCHAR(100),
                    age INT
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """

            sql """
                CREATE ROUTINE LOAD ${multiJobName} ON ${multiTableName}
                PROPERTIES ("format" = "json", "desired_concurrent_number" = "2")
                FROM KINESIS (
                    "aws.region" = "${region}",
                    "aws.access_key" = "${ak}",
                    "aws.secret_key" = "${sk}",
                    "kinesis_stream" = "${multiStreamName}",
                    "property.kinesis_default_pos" = "TRIM_HORIZON"
                )
            """
            multiJobCreated = true

            waitForCountAtLeast(multiTableName, totalRows, 240)

            boolean seenParallelTask = false
            for (int i = 0; i < 120; i++) {
                def jobInfo = sql "SHOW ROUTINE LOAD FOR ${multiJobName}"
                assertTrue(jobInfo.size() > 0)
                int currentTaskNum = ((Number) jobInfo[0][10]).intValue()
                if (currentTaskNum >= 2) {
                    seenParallelTask = true
                    break
                }
                def taskRows = sql "SHOW ROUTINE LOAD TASK WHERE JobName = \"${multiJobName}\""
                if (taskRows.size() >= 2) {
                    seenParallelTask = true
                    break
                }
                Thread.sleep(1000)
            }
            assertTrue(seenParallelTask, "Failed to observe parallel routine load task scheduling")
        } finally {
            if (multiJobCreated) {
                try {
                    sql "STOP ROUTINE LOAD FOR ${multiJobName}"
                } catch (Exception e) {
                    logger.warn("Failed to stop multi routine load ${multiJobName}: ${e.message}")
                }
            }
            if (multiStreamCreated) {
                try {
                    kinesisClient.deleteStream(new DeleteStreamRequest().withStreamName(multiStreamName))
                    logger.info("Deleted multi stream: ${multiStreamName}")
                } catch (Exception e) {
                    logger.warn("Failed to delete multi stream ${multiStreamName}: ${e.message}")
                }
            }
            sql "DROP TABLE IF EXISTS ${multiTableName}"
        }
    } finally {
        kinesisClient.shutdown()
    }
}
