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

suite("test_kinesis_routine_load_property") {
    def region = context.config.awsRegion ?: context.config.otherConfigs.get("awsRegion")
    def ak = context.config.awsAccessKey ?: context.config.otherConfigs.get("awsAccessKey")
    def sk = context.config.awsSecretKey ?: context.config.otherConfigs.get("awsSecretKey")

    if (!region || !ak || !sk) {
        logger.info("Skip ${name} case, missing AWS config: region=${region}, ak=${ak != null}, sk=${sk != null}")
        return
    }

    def suffix = UUID.randomUUID().toString().substring(0, 8)
    def streamName = "doris-test-prop-${suffix}"
    def jobName = "test_kinesis_property_${suffix}"

    def credentials = new BasicAWSCredentials(ak, sk)
    def kinesisClient = AmazonKinesisClientBuilder.standard()
        .withRegion(region)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .build()

    def toLongValue = { Object value ->
        if (value instanceof Number) {
            return ((Number) value).longValue()
        }
        return Long.parseLong(value.toString().trim())
    }

    def toIntValue = { Object value ->
        if (value instanceof Number) {
            return ((Number) value).intValue()
        }
        return Integer.parseInt(value.toString().trim())
    }

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

    def writeCsvRecords = { int startId, int endId, String partitionKeyPrefix ->
        logger.info("Writing CSV records ${startId}-${endId} to stream ${streamName}")
        for (int i = startId; i <= endId; i++) {
            def data = "\"${i}\",\"name\\\"${i}\",\"2023-08-01\",\"value,${i}\",\"2023-08-01 12:00:00\",\"extra${i}\""
            putRecordWithRetry("${partitionKeyPrefix}_${i}", data)
        }
    }

    def queryCount = {
        def result = sql "SELECT COUNT(*) FROM test_kinesis_routine_load_property"
        return toLongValue(result[0][0])
    }

    def waitForCountAtLeast = { long expectedCount, int timeoutSec ->
        long lastCount = -1
        for (int i = 0; i < timeoutSec; i++) {
            lastCount = queryCount()
            if (lastCount >= expectedCount) {
                logger.info("Table test_kinesis_routine_load_property row count reached ${lastCount} " +
                    "(expected >= ${expectedCount})")
                return
            }
            Thread.sleep(1000)
        }
        assertTrue(false, "Timeout waiting row count >= ${expectedCount}, last count=${lastCount}")
    }

    def checkParsedRow = { int id ->
        def expectedName = "name\"" + id
        def expectedValue = "value," + id
        def parsedRow = sql """
            SELECT k1, k2, v2
            FROM test_kinesis_routine_load_property
            WHERE k1 = ${id}
            ORDER BY v3
            LIMIT 1
        """
        assertTrue(parsedRow.size() > 0, "Expected at least one row for k1=${id}")
        assertEquals(id, toIntValue(parsedRow[0][0]))
        assertEquals(expectedName, parsedRow[0][1].toString())
        assertEquals(expectedValue, parsedRow[0][2].toString())
    }

    def streamCreated = false
    def tableCreated = false
    def jobCreated = false
    def test1PreparedForReuse = false

    def stopRoutineLoadIfCreated = {
        if (jobCreated) {
            try {
                sql "STOP ROUTINE LOAD FOR ${jobName}"
            } catch (Exception e) {
                logger.warn("Failed to stop routine load ${jobName}: ${e.message}")
            }
            jobCreated = false
        }
    }

    def deleteStreamIfCreated = {
        if (streamCreated) {
            try {
                kinesisClient.deleteStream(new DeleteStreamRequest().withStreamName(streamName))
                logger.info("Deleted stream: ${streamName}")
            } catch (Exception e) {
                logger.warn("Failed to delete stream ${streamName}: ${e.message}")
            }
            streamCreated = false
        }
    }

    def dropTableIfCreated = {
        if (tableCreated) {
            sql "DROP TABLE IF EXISTS test_kinesis_routine_load_property"
            tableCreated = false
        }
    }

    try {
        // test1 : create routine load with CSV properties and verify TRIM_HORIZON consumes existing records
        try {
            logger.info("Creating Kinesis stream: ${streamName}")
            kinesisClient.createStream(new CreateStreamRequest()
                .withStreamName(streamName)
                .withShardCount(1))
            streamCreated = true
            waitForStreamReady(120)

            writeCsvRecords(1, 10, "before_create")

            sql "DROP TABLE IF EXISTS test_kinesis_routine_load_property"
            sql """
                CREATE TABLE IF NOT EXISTS test_kinesis_routine_load_property (
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
            tableCreated = true

            sql """
                CREATE ROUTINE LOAD ${jobName} ON test_kinesis_routine_load_property
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
            checkParsedRow(1)
            test1PreparedForReuse = true
        } finally {
            if (!test1PreparedForReuse) {
                stopRoutineLoadIfCreated()
                deleteStreamIfCreated()
                dropTableIfCreated()
            }
        }

        // test2 : verify running job can continue importing newly written records with the same CSV properties
        try {
            assertTrue(test1PreparedForReuse, "Test2 requires test1 to prepare stream/table/job")

            writeCsvRecords(11, 20, "after_create")
            waitForCountAtLeast(20, 120)
            checkParsedRow(11)

            def finalResult = sql "SELECT COUNT(*), COUNT(DISTINCT k1), MIN(k1), MAX(k1) " +
                "FROM test_kinesis_routine_load_property"
            assertEquals(20L, toLongValue(finalResult[0][0]))
            assertEquals(20L, toLongValue(finalResult[0][1]))
            assertEquals(1, toIntValue(finalResult[0][2]))
            assertEquals(20, toIntValue(finalResult[0][3]))
        } finally {
            stopRoutineLoadIfCreated()
            deleteStreamIfCreated()
            dropTableIfCreated()
        }
    } finally {
        kinesisClient.shutdown()
    }
}
