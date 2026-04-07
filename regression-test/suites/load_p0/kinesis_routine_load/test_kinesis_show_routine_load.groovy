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

suite("test_kinesis_show_routine_load") {

    def awsRegion = context.config.awsRegion ?: context.config.otherConfigs.get("awsRegion")
    def awsAccessKey = context.config.awsAccessKey ?: context.config.otherConfigs.get("awsAccessKey")
    def awsSecretKey = context.config.awsSecretKey ?: context.config.otherConfigs.get("awsSecretKey")

    if (!awsRegion || !awsAccessKey || !awsSecretKey) {
        logger.info("Skip ${name} case, AWS config not provided")
        return
    }

    def suffix = UUID.randomUUID().toString().substring(0, 8)
    String streamName = "doris-test-show-${suffix}"
    String tableName = "test_kinesis_show_${suffix}"
    String jobName = "test_kinesis_show_${suffix}"

    def credentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    def kinesisClient = AmazonKinesisClientBuilder.standard()
        .withRegion(awsRegion)
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
                def streamDesc = result.getStreamDescription()
                if (streamDesc.getStreamStatus() == "ACTIVE" && !streamDesc.getShards().isEmpty()) {
                    return
                }
            } catch (ResourceNotFoundException e) {
                // Stream metadata may not be visible immediately after create.
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
        for (int i = startId; i <= endId; i++) {
            def data = "${i},name_${i},2023-08-01,value_${i},2023-08-01 12:00:00,extra_${i}"
            putRecordWithRetry("${partitionKeyPrefix}_${i}", data)
        }
    }

    def waitForCountAtLeast = { long expectedCount, int timeoutSec ->
        long lastCount = -1
        for (int i = 0; i < timeoutSec; i++) {
            def result = sql "SELECT COUNT(*) FROM ${tableName}"
            lastCount = toLongValue(result[0][0])
            if (lastCount >= expectedCount) {
                logger.info("Table ${tableName} row count reached ${lastCount} (expected >= ${expectedCount})")
                return lastCount
            }
            Thread.sleep(1000)
        }
        assertTrue(false, "Timeout waiting table ${tableName} count >= ${expectedCount}, last count=${lastCount}")
    }

    def waitForJobStateIn = { Set<String> expectedStates, int timeoutSec, boolean includeHistory = false ->
        def showSql = includeHistory
            ? "SHOW ALL ROUTINE LOAD FOR ${jobName}"
            : "SHOW ROUTINE LOAD FOR ${jobName}"
        def lastState = "UNKNOWN"
        for (int i = 0; i < timeoutSec; i++) {
            def result = sql showSql
            if (result.size() > 0) {
                lastState = result[0][8].toString()
                if (expectedStates.contains(lastState)) {
                    logger.info("Routine load job ${jobName} reached state ${lastState}")
                    return result[0]
                }
            }
            Thread.sleep(1000)
        }
        assertTrue(false, "Timeout waiting job ${jobName} to reach states ${expectedStates}, last state=${lastState}")
    }

    def waitForShowRowWithOpenShards = { int timeoutSec ->
        for (int i = 0; i < timeoutSec; i++) {
            def result = sql "SHOW ROUTINE LOAD FOR ${jobName}"
            if (result.size() > 0) {
                def row = result[0]
                def dataSourceProperties = parseJson(row[12].toString())
                if (dataSourceProperties.containsKey("openKinesisShards")
                        && dataSourceProperties.openKinesisShards.toString().length() > 0) {
                    return row
                }
            }
            Thread.sleep(1000)
        }
        assertTrue(false, "Timeout waiting job ${jobName} to expose openKinesisShards")
    }

    def streamCreated = false
    def tableCreated = false
    def jobCreated = false
    def test1PreparedForReuse = false

    try {
        // test1 : verify SHOW ROUTINE LOAD core fields and kinesis-specific display content
        try {
            logger.info("Creating Kinesis stream: ${streamName}")
            kinesisClient.createStream(new CreateStreamRequest()
                .withStreamName(streamName)
                .withShardCount(1))
            streamCreated = true
            waitForStreamReady(120)

            writeCsvRecords(1, 10, "before_create")

            sql "DROP TABLE IF EXISTS ${tableName}"
            sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    k1 int(20) NULL,
                    k2 string NULL,
                    v1 date NULL,
                    v2 string NULL,
                    v3 datetime NULL,
                    v4 string NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 3
                PROPERTIES ("replication_num" = "1")
            """
            tableCreated = true

            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS TERMINATED BY ","
                PROPERTIES (
                    "max_batch_interval" = "5",
                    "max_batch_rows" = "300000",
                    "max_batch_size" = "209715200"
                )
                FROM KINESIS (
                    "aws.region" = "${awsRegion}",
                    "aws.access_key" = "${awsAccessKey}",
                    "aws.secret_key" = "${awsSecretKey}",
                    "kinesis_stream" = "${streamName}",
                    "property.kinesis_default_pos" = "TRIM_HORIZON"
                )
            """
            jobCreated = true

            waitForJobStateIn(["RUNNING", "NEED_SCHEDULE"] as Set<String>, 120)
            def showRow = waitForShowRowWithOpenShards(120)

            assertTrue(showRow.size() >= 23, "SHOW ROUTINE LOAD column count should be >= 23")
            assertEquals(jobName, showRow[1].toString())
            assertEquals(tableName, showRow[6].toString())
            assertEquals("false", showRow[7].toString().toLowerCase())
            assertEquals("KINESIS", showRow[9].toString())
            assertTrue(toIntValue(showRow[10]) >= 0)

            def jobProperties = showRow[11].toString()
            assertTrue(jobProperties.contains("max_batch_interval"))
            assertTrue(jobProperties.contains("max_batch_rows"))
            assertTrue(jobProperties.contains("max_batch_size"))

            def dataSourceProperties = parseJson(showRow[12].toString())
            assertEquals(awsRegion, dataSourceProperties.region.toString())
            assertEquals(streamName, dataSourceProperties.stream.toString())
            assertTrue(dataSourceProperties.containsKey("openKinesisShards"))
            assertTrue(dataSourceProperties.openKinesisShards.toString().length() > 0)
            assertTrue(dataSourceProperties.containsKey("closedKinesisShards"))

            def customProperties = parseJson(showRow[13].toString())
            assertEquals("******", customProperties["aws.secret_key"].toString())
            assertTrue(customProperties["aws.access_key"].toString().length() > 0)

            // Verify additional SHOW ROUTINE LOAD paths.
            String dbName = context.config.getDbNameByFile(context.file)
            def qualifiedResult = sql "SHOW ROUTINE LOAD FOR ${dbName}.${jobName}"
            assertTrue(qualifiedResult.size() == 1)
            assertEquals(showRow[0].toString(), qualifiedResult[0][0].toString())

            def likeResult = sql "SHOW ROUTINE LOAD LIKE \"%${suffix}%\""
            assertTrue(likeResult.size() >= 1)

            waitForCountAtLeast(10L, 180)

            def showAfterLoad = sql "SHOW ROUTINE LOAD FOR ${jobName}"
            assertTrue(showAfterLoad.size() > 0)
            def loadedRow = showAfterLoad[0]

            def statistic = parseJson(loadedRow[14].toString())
            assertTrue(statistic.containsKey("openShardNum"))
            assertTrue(toLongValue(statistic.openShardNum) >= 1L)
            assertTrue(statistic.containsKey("trackedShardNum"))

            def progress = parseJson(loadedRow[15].toString())
            assertTrue(progress.size() >= 1, "Progress should contain at least one shard")

            def lag = parseJson(loadedRow[16].toString())
            assertTrue(lag.size() >= 1, "Lag should contain at least one shard")
            lag.each { shardId, lagMs ->
                assertTrue(toLongValue(lagMs) >= -1L, "Invalid lag value for shard ${shardId}: ${lagMs}")
            }
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
                    } catch (Exception e) {
                        logger.warn("Failed to delete stream: ${e.message}")
                    }
                    streamCreated = false
                }
                if (tableCreated) {
                    sql "DROP TABLE IF EXISTS ${tableName}"
                    tableCreated = false
                }
            }
        }

        // test2 : stop job and verify SHOW ALL ROUTINE LOAD includes final state
        try {
            assertTrue(test1PreparedForReuse, "Test2 requires test1 to prepare stream/table/job")

            sql "STOP ROUTINE LOAD FOR ${jobName}"
            jobCreated = false

            def historyRow = waitForJobStateIn(["STOPPED", "CANCELLED"] as Set<String>, 120, true)
            assertEquals(jobName, historyRow[1].toString())
            assertEquals("KINESIS", historyRow[9].toString())
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
                } catch (Exception e) {
                    logger.warn("Failed to delete stream: ${e.message}")
                }
                streamCreated = false
            }
            if (tableCreated) {
                sql "DROP TABLE IF EXISTS ${tableName}"
                tableCreated = false
            }
        }
    } finally {
        kinesisClient.shutdown()
    }
}
