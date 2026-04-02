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

suite("test_kinesis_routine_load_shard_change", "nonConcurrent") {
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
    def streamName = "doris-test-shard-change-${suffix}"
    def tableName = "test_kinesis_shard_change"
    def jobName = "test_kinesis_shard_change_${suffix}"

    def credentials = new BasicAWSCredentials(ak, sk)
    def kinesisClient = AmazonKinesisClientBuilder.standard()
        .withRegion(region)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .build()
    def jobCreated = false

    def queryCount = {
        def result = sql "SELECT COUNT(*) FROM ${tableName}"
        return ((Number) result[0][0]).longValue()
    }

    def waitForExactCount = { long expectedCount, int timeoutSec ->
        long lastCount = -1
        for (int i = 0; i < timeoutSec; i++) {
            lastCount = queryCount()
            if (lastCount == expectedCount) {
                logger.info("Table ${tableName} row count reached exact ${expectedCount}")
                return
            }
            if (lastCount > expectedCount) {
                assertTrue(false,
                    "Row count exceeded expected ${expectedCount}, actual=${lastCount}. Possible duplicate consumption")
            }
            Thread.sleep(1000)
        }
        assertTrue(false, "Timeout waiting row count == ${expectedCount}, last count=${lastCount}")
    }

    def writeRange = { int startId, int endId ->
        logger.info("Writing records ${startId}-${endId} to stream ${streamName}")
        for (int i = startId; i <= endId; i++) {
            def data = "{\"id\": ${i}, \"name\": \"user_${i}\", \"age\": ${20 + i % 50}}"
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

    def waitForOpenShardCount = { int expectedOpenShardNum, int timeoutSec ->
        def describeRequest = new DescribeStreamRequest().withStreamName(streamName)
        for (int i = 0; i < timeoutSec; i++) {
            try {
                def result = kinesisClient.describeStream(describeRequest)
                def desc = result.getStreamDescription()
                if (desc.getStreamStatus() == "ACTIVE" && !desc.getShards().isEmpty()) {
                    def openShards = desc.getShards().findAll {
                        it.getSequenceNumberRange().getEndingSequenceNumber() == null
                    }
                    if (openShards.size() == expectedOpenShardNum) {
                        logger.info("Open shard count reached ${expectedOpenShardNum}")
                        return openShards
                    }
                }
            } catch (ResourceNotFoundException e) {
                // Metadata may not be visible immediately after create.
            }
            Thread.sleep(1000)
        }
        assertTrue(false, "Timeout waiting open shard count = ${expectedOpenShardNum}")
    }

    def getJobState = {
        def result = sql "SHOW ROUTINE LOAD FOR ${jobName}"
        assertTrue(result.size() > 0, "SHOW ROUTINE LOAD returned empty result for ${jobName}")
        return result[0][8].toString()
    }

    def waitForDataSourceShardView = { List<String> expectedShardIds, int timeoutSec ->
        def expected = expectedShardIds.findAll { it != null }
        for (int i = 0; i < timeoutSec; i++) {
            def result = sql "SHOW ROUTINE LOAD FOR ${jobName}"
            assertTrue(result.size() > 0)
            def dataSourceProperties = result[0][12].toString()
            def allFound = true
            for (String shardId : expected) {
                if (!dataSourceProperties.contains(shardId)) {
                    allFound = false
                    break
                }
            }
            if (allFound) {
                logger.info("Routine load datasource shard view contains expected shard ids: ${expected}")
                return
            }
            Thread.sleep(1000)
        }
        assertTrue(false, "Timeout waiting datasource shard view to include ${expected}")
    }

    try {
        logger.info("Creating Kinesis stream: ${streamName}")
        kinesisClient.createStream(new CreateStreamRequest()
            .withStreamName(streamName)
            .withShardCount(1))

        def initialOpenShards = waitForOpenShardCount(1, 120)
        def parentShard = initialOpenShards[0]
        def parentShardId = parentShard.getShardId()

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
                "desired_concurrent_number" = "2"
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

        // Stage 1: baseline load before shard change.
        writeRange(1, 50)
        waitForExactCount(50, 180)

        // Stage 2: shard split.
        def startHash = new BigInteger(parentShard.getHashKeyRange().getStartingHashKey())
        def endHash = new BigInteger(parentShard.getHashKeyRange().getEndingHashKey())
        def splitHash = startHash.add(endHash).divide(BigInteger.valueOf(2))
        logger.info("Splitting shard ${parentShardId} at hash ${splitHash}")
        kinesisClient.splitShard(new SplitShardRequest()
            .withStreamName(streamName)
            .withShardToSplit(parentShardId)
            .withNewStartingHashKey(splitHash.toString()))

        def openShardsAfterSplit = waitForOpenShardCount(2, 180)
        def childShards = openShardsAfterSplit.findAll {
            it.getParentShardId() == parentShardId
        }
        assertEquals(2, childShards.size())
        def childShardIds = childShards.collect { it.getShardId() }
        logger.info("Split produced child shards: ${childShardIds} from parent ${parentShardId}")

        // Parent/child relation and shard progress visibility in SHOW ROUTINE LOAD.
        waitForDataSourceShardView([parentShardId, childShardIds[0], childShardIds[1]], 180)

        writeRange(51, 100)
        waitForExactCount(100, 240)

        // Stage 3: shard merge on split children.
        def sortedChildren = childShards.sort {
            new BigInteger(it.getHashKeyRange().getStartingHashKey())
        }
        def leftChild = sortedChildren[0]
        def rightChild = sortedChildren[1]
        logger.info("Merging adjacent shards: ${leftChild.getShardId()} + ${rightChild.getShardId()}")
        kinesisClient.mergeShards(new MergeShardsRequest()
            .withStreamName(streamName)
            .withShardToMerge(leftChild.getShardId())
            .withAdjacentShardToMerge(rightChild.getShardId()))

        def openShardsAfterMerge = waitForOpenShardCount(1, 180)
        def mergedShard = openShardsAfterMerge[0]
        def mergedParentIds = [mergedShard.getParentShardId(), mergedShard.getAdjacentParentShardId()].findAll { it != null }
        assertEquals(2, mergedParentIds.size())
        assertTrue(mergedParentIds.contains(leftChild.getShardId()))
        assertTrue(mergedParentIds.contains(rightChild.getShardId()))
        logger.info("Merged shard ${mergedShard.getShardId()} has parents ${mergedParentIds}")

        // Progress inheritance across old/new shards should be visible in datasource shard view.
        waitForDataSourceShardView([leftChild.getShardId(), rightChild.getShardId(), mergedShard.getShardId()], 180)

        writeRange(101, 150)
        waitForExactCount(150, 240)

        // Validate no data loss and no duplicate consumption during split+merge.
        def finalResult = sql "SELECT COUNT(*), COUNT(DISTINCT id), MIN(id), MAX(id) FROM ${tableName}"
        def totalCount = ((Number) finalResult[0][0]).longValue()
        def distinctCount = ((Number) finalResult[0][1]).longValue()
        def minId = ((Number) finalResult[0][2]).intValue()
        def maxId = ((Number) finalResult[0][3]).intValue()
        assertEquals(150L, totalCount)
        assertEquals(150L, distinctCount)
        assertEquals(1, minId)
        assertEquals(150, maxId)

        def finalState = getJobState()
        assertNotEquals("CANCELLED", finalState)
        logger.info("Final routine load state after shard split+merge: ${finalState}")

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
