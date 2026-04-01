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

suite("test_kinesis_routine_load_property", "nonConcurrent") {
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
    def streamName = "doris-test-prop-${suffix}"
    def tableName = "test_kinesis_routine_load_property"
    def jobName = "test_kinesis_property_${suffix}"

    def credentials = new BasicAWSCredentials(ak, sk)
    def kinesisClient = AmazonKinesisClientBuilder.standard()
        .withRegion(region)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .build()
    def jobCreated = false

    def writeCsvRecords = { int startId, int endId ->
        logger.info("Writing CSV records ${startId}-${endId} to stream ${streamName}")
        for (int i = startId; i <= endId; i++) {
            def data = "\"${i}\",\"name\\\"${i}\",\"2023-08-01\",\"value,${i}\",\"2023-08-01 12:00:00\",\"extra${i}\""
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
                return
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

        writeCsvRecords(1, 10)

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                k1 INT NULL,
                k2 STRING NULL,
                v1 DATE NULL,
                v2 STRING NULL,
                v3 DATETIME NULL,
                v4 STRING NULL
            )
            ENGINE=OLAP
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 3
            PROPERTIES ("replication_num" = "1")
        """

        sql """
            CREATE ROUTINE LOAD ${jobName} ON ${tableName}
            COLUMNS TERMINATED BY ","
            PROPERTIES (
                "enclose" = "\\"",
                "escape" = "\\\\",
                "max_batch_interval" = "5",
                "max_batch_rows" = "200000",
                "max_batch_size" = "209715200"
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

        waitForCountAtLeast(10, 120)

        def parsedRow = sql """
            SELECT k1, k2, v2
            FROM ${tableName}
            WHERE k1 = 1
            ORDER BY v3
            LIMIT 1
        """
        assertTrue(parsedRow.size() > 0, "Expected at least one row for k1=1")
        assertEquals(1, ((Number) parsedRow[0][0]).intValue())
        assertEquals("name\"1", parsedRow[0][1].toString())
        assertEquals("value,1", parsedRow[0][2].toString())

    } finally {
        if (jobCreated) {
            try {
                sql "STOP ROUTINE LOAD FOR ${jobName}"
            } catch (Exception e) {
                logger.warn("Failed to stop routine load ${jobName}: ${e.message}")
            }
        }
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
