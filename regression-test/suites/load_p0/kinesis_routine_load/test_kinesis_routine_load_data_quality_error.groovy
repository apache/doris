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

suite("test_kinesis_routine_load_data_quality_error") {
    def region = context.config.awsRegion ?: context.config.otherConfigs.get("awsRegion")
    def ak = context.config.awsAccessKey ?: context.config.otherConfigs.get("awsAccessKey")
    def sk = context.config.awsSecretKey ?: context.config.otherConfigs.get("awsSecretKey")

    if (!region || !ak || !sk) {
        logger.info("Skip ${name} case, missing AWS config: region=${region}, ak=${ak != null}, sk=${sk != null}")
        return
    }

    def suffix = UUID.randomUUID().toString().substring(0, 8)
    def streamName = "doris-quality-${suffix}"
    def jobName = "test_kinesis_quality_${suffix}"

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

    def writeMixedJsonRecords = { int startId, int endId, String partitionKeyPrefix ->
        int written = 0
        int bad = 0
        logger.info("Writing mixed JSON records ${startId}-${endId} to stream ${streamName}")
        for (int i = startId; i <= endId; i++) {
            boolean isBadRecord = (i % 4 == 0)
            def data
            if (isBadRecord) {
                if (i % 8 == 0) {
                    data = "{\"id\": \"bad_id_${i}\", \"age\": ${20 + i}}"
                } else {
                    data = "{\"id\": ${i}, \"age\": \"bad_age_${i}\"}"
                }
                bad++
            } else {
                data = "{\"id\": ${i}, \"age\": ${20 + i}}"
            }
            putRecordWithRetry("${partitionKeyPrefix}_${i}", data)
            written++
        }
        return [written: written, bad: bad, good: written - bad]
    }

    def writeNotNullViolationRecords = { int startId, int endId, String partitionKeyPrefix, String targetStream ->
        int written = 0
        int bad = 0
        logger.info("Writing NOT NULL violation records ${startId}-${endId} to stream ${targetStream}")
        for (int i = startId; i <= endId; i++) {
            boolean isBadRecord = (i % 4 == 0)
            def data
            if (isBadRecord) {
                data = "{\"id\": null, \"age\": ${20 + i}}"
                bad++
            } else {
                data = "{\"id\": ${i}, \"age\": ${20 + i}}"
            }
            def putRequest = new PutRecordRequest()
                .withStreamName(targetStream)
                .withPartitionKey("${partitionKeyPrefix}_${i}")
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
            written++
        }
        return [written: written, bad: bad, good: written - bad]
    }

    def queryCount = {
        def result = sql "SELECT COUNT(*) FROM test_kinesis_quality"
        return toLongValue(result[0][0])
    }

    def waitForCountAtLeast = { long expectedCount, int timeoutSec ->
        long lastCount = -1
        for (int i = 0; i < timeoutSec; i++) {
            lastCount = queryCount()
            if (lastCount >= expectedCount) {
                logger.info("Table test_kinesis_quality row count reached ${lastCount} (expected >= ${expectedCount})")
                return
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
            sql "DROP TABLE IF EXISTS test_kinesis_quality"
            tableCreated = false
        }
    }

    try {
        // test1 : load mixed-quality records and verify bad records are tolerated under max_filter_ratio
        try {
            logger.info("Creating Kinesis stream: ${streamName}")
            kinesisClient.createStream(new CreateStreamRequest()
                .withStreamName(streamName)
                .withShardCount(1))
            streamCreated = true
            waitForStreamReady(120)

            def firstBatch = writeMixedJsonRecords(1, 40, "first")
            assertEquals(40, firstBatch.written)
            assertEquals(10, firstBatch.bad)
            assertEquals(30, firstBatch.good)

            sql "DROP TABLE IF EXISTS test_kinesis_quality"
            sql """
                CREATE TABLE test_kinesis_quality (
                    id INT,
                    age INT
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            tableCreated = true

            sql """
                CREATE ROUTINE LOAD ${jobName} ON test_kinesis_quality
                PROPERTIES (
                    "format" = "json",
                    "max_filter_ratio" = "0.3"
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

            waitForCountAtLeast(30L, 180)

            def stateAfterFirstBatch = getJobState()
            assertNotEquals("CANCELLED", stateAfterFirstBatch)
            test1PreparedForReuse = true
        } finally {
            if (!test1PreparedForReuse) {
                stopRoutineLoadIfCreated()
                deleteStreamIfCreated()
                dropTableIfCreated()
            }
        }

        // test2 : continue loading mixed-quality records and verify job keeps ingesting valid records
        try {
            assertTrue(test1PreparedForReuse, "Test2 requires test1 to prepare stream/table/job")

            def secondBatch = writeMixedJsonRecords(41, 60, "second")
            assertEquals(20, secondBatch.written)
            assertEquals(5, secondBatch.bad)
            assertEquals(15, secondBatch.good)

            waitForCountAtLeast(45L, 360)

            def finalResult = sql "SELECT COUNT(*), COUNT(DISTINCT id), MIN(id), MAX(id) FROM test_kinesis_quality"
            assertTrue(toLongValue(finalResult[0][0]) >= 45L)
            assertTrue(toLongValue(finalResult[0][1]) >= 45L)
            assertEquals(1, toIntValue(finalResult[0][2]))
            assertTrue(toIntValue(finalResult[0][3]) >= 59)

            def finalState = getJobState()
            assertNotEquals("CANCELLED", finalState)

            // test2.1 : verify NOT NULL violation records are filtered under max_filter_ratio
            def notNullStream = "doris-quality-not-null-${suffix}"
            def notNullTable = "test_kinesis_quality_not_null"
            def notNullJob = "${jobName}_not_null"
            def notNullStreamCreated = false
            def notNullTableCreated = false
            def notNullJobCreated = false
            try {
                kinesisClient.createStream(new CreateStreamRequest()
                    .withStreamName(notNullStream)
                    .withShardCount(1))
                notNullStreamCreated = true

                def notNullStreamActive = false
                for (int i = 0; i < 120; i++) {
                    try {
                        def describeResult = kinesisClient.describeStream(
                            new DescribeStreamRequest().withStreamName(notNullStream))
                        if (describeResult.getStreamDescription().getStreamStatus() == "ACTIVE" &&
                                !describeResult.getStreamDescription().getShards().isEmpty()) {
                            notNullStreamActive = true
                            break
                        }
                    } catch (ResourceNotFoundException e) {
                        // Metadata may not be visible immediately after create.
                    }
                    Thread.sleep(1000)
                }
                assertTrue(notNullStreamActive, "Stream ${notNullStream} failed to become active")

                def thirdBatch = writeNotNullViolationRecords(61, 80, "third_not_null", notNullStream)
                assertEquals(20, thirdBatch.written)
                assertEquals(5, thirdBatch.bad)
                assertEquals(15, thirdBatch.good)

                sql "DROP TABLE IF EXISTS ${notNullTable}"
                sql """
                    CREATE TABLE ${notNullTable} (
                        id INT NOT NULL,
                        age INT
                    )
                    DUPLICATE KEY(id)
                    DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES ("replication_num" = "1")
                """
                notNullTableCreated = true

                sql """
                    CREATE ROUTINE LOAD ${notNullJob} ON ${notNullTable}
                    PROPERTIES (
                        "format" = "json",
                        "max_filter_ratio" = "0.5"
                    )
                    FROM KINESIS (
                        "aws.region" = "${region}",
                        "aws.access_key" = "${ak}",
                        "aws.secret_key" = "${sk}",
                        "kinesis_stream" = "${notNullStream}",
                        "property.kinesis_default_pos" = "TRIM_HORIZON"
                    )
                """
                notNullJobCreated = true

                def stateRetry = 0
                while (true) {
                    Thread.sleep(1000)
                    def stateResult = sql "SHOW ROUTINE LOAD FOR ${notNullJob}"
                    def state = stateResult[0][8].toString()
                    if (state == "RUNNING" || state == "NEED_SCHEDULE") {
                        break
                    }
                    stateRetry++
                    if (stateRetry > 120) {
                        fail("NOT NULL job failed to start")
                    }
                }

                long notNullCount = -1
                for (int i = 0; i < 360; i++) {
                    def notNullResult = sql "SELECT COUNT(*) FROM ${notNullTable}"
                    notNullCount = toLongValue(notNullResult[0][0])
                    if (notNullCount >= 15L) {
                        logger.info("Table ${notNullTable} row count reached ${notNullCount} (expected >= 15)")
                        break
                    }
                    Thread.sleep(1000)
                }
                assertTrue(notNullCount >= 15L, "Timeout waiting row count >= 15, last count=${notNullCount}")
            } finally {
                if (notNullJobCreated) {
                    try {
                        sql "STOP ROUTINE LOAD FOR ${notNullJob}"
                    } catch (Exception e) {
                        logger.warn("Failed to stop routine load ${notNullJob}: ${e.message}")
                    }
                }
                if (notNullStreamCreated) {
                    try {
                        kinesisClient.deleteStream(new DeleteStreamRequest().withStreamName(notNullStream))
                    } catch (Exception e) {
                        logger.warn("Failed to delete stream ${notNullStream}: ${e.message}")
                    }
                }
                if (notNullTableCreated) {
                    sql "DROP TABLE IF EXISTS ${notNullTable}"
                }
            }
        } finally {
            stopRoutineLoadIfCreated()
            deleteStreamIfCreated()
            dropTableIfCreated()
        }
    } finally {
        kinesisClient.shutdown()
    }
}
