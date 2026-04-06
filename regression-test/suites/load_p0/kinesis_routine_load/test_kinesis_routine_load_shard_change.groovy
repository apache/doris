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

suite("test_kinesis_routine_load_shard_change") {
    def region = context.config.awsRegion ?: context.config.otherConfigs.get("awsRegion")
    def ak = context.config.awsAccessKey ?: context.config.otherConfigs.get("awsAccessKey")
    def sk = context.config.awsSecretKey ?: context.config.otherConfigs.get("awsSecretKey")

    if (!region || !ak || !sk) {
        logger.info("Skip ${name} case, missing AWS config: region=${region}, ak=${ak != null}, sk=${sk != null}")
        return
    }

    def suffix = UUID.randomUUID().toString().substring(0, 8)
    def streamName = "doris-test-shard-change-${suffix}"
    def jobName = "test_kinesis_shard_change_${suffix}"

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

    def queryCount = {
        def result = sql "SELECT COUNT(*) FROM test_kinesis_shard_change"
        return toLongValue(result[0][0])
    }

    def waitForExactCount = { long expectedCount, int timeoutSec ->
        long lastCount = -1
        for (int i = 0; i < timeoutSec; i++) {
            lastCount = queryCount()
            if (lastCount == expectedCount) {
                logger.info("Table test_kinesis_shard_change row count reached exact ${expectedCount}")
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

    def putRecordWithRetry = { String partitionKey, String data, String explicitHashKey ->
        def putRequest = new PutRecordRequest()
            .withStreamName(streamName)
            .withPartitionKey(partitionKey)
            .withData(ByteBuffer.wrap(data.getBytes("UTF-8")))
        if (explicitHashKey != null) {
            putRequest.withExplicitHashKey(explicitHashKey)
        }
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

    def writeRange = { int startId, int endId, String partitionKeyPrefix, String explicitHashKey ->
        logger.info("Writing records ${startId}-${endId} to stream ${streamName}, " +
            "partitionKeyPrefix=${partitionKeyPrefix}, explicitHashKey=${explicitHashKey}")
        for (int i = startId; i <= endId; i++) {
            def data = "{\"id\": ${i}, \"name\": \"user_${i}\", \"age\": ${20 + i % 50}}"
            putRecordWithRetry("${partitionKeyPrefix}_${i}", data, explicitHashKey)
        }
    }

    def getShardMiddleHashKey = { shard ->
        def startHash = new BigInteger(shard.getHashKeyRange().getStartingHashKey())
        def endHash = new BigInteger(shard.getHashKeyRange().getEndingHashKey())
        return startHash.add(endHash).divide(BigInteger.valueOf(2)).toString()
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
            assertTrue(result.size() > 0, "SHOW ROUTINE LOAD returned empty result for ${jobName}")
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

    def streamCreated = false
    def tableCreated = false
    def jobCreated = false
    def test1PreparedForReuse = false
    def test2PreparedForReuse = false

    def parentShard = null
    def splitChildShards = null

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
            sql "DROP TABLE IF EXISTS test_kinesis_shard_change"
            tableCreated = false
        }
    }

    try {
        // test1 : create stream/table/job and verify baseline consumption before shard change
        try {
            logger.info("Creating Kinesis stream: ${streamName}")
            kinesisClient.createStream(new CreateStreamRequest()
                .withStreamName(streamName)
                .withShardCount(1))
            streamCreated = true

            def initialOpenShards = waitForOpenShardCount(1, 120)
            assertEquals(1, initialOpenShards.size())
            parentShard = initialOpenShards[0]

            sql "DROP TABLE IF EXISTS test_kinesis_shard_change"
            sql """
                CREATE TABLE test_kinesis_shard_change (
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
                CREATE ROUTINE LOAD ${jobName} ON test_kinesis_shard_change
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

            // Step 1: write first batch before split.
            writeRange(1, 50, "before_split", null)
            waitForExactCount(50, 180)
            test1PreparedForReuse = true
        } finally {
            if (!test1PreparedForReuse) {
                stopRoutineLoadIfCreated()
                deleteStreamIfCreated()
                dropTableIfCreated()
            }
        }

        // test2 : split shard and verify parent/child shard progress inheritance
        try {
            assertTrue(test1PreparedForReuse, "Test2 requires test1 to prepare stream/table/job")
            assertTrue(parentShard != null, "Parent shard should be available before split")

            def parentShardId = parentShard.getShardId()
            def splitHash = getShardMiddleHashKey(parentShard)

            // Step 2: split parent shard.
            logger.info("Splitting shard ${parentShardId} at hash ${splitHash}")
            kinesisClient.splitShard(new SplitShardRequest()
                .withStreamName(streamName)
                .withShardToSplit(parentShardId)
                .withNewStartingHashKey(splitHash.toString()))

            def openShardsAfterSplit = waitForOpenShardCount(2, 180)
            splitChildShards = openShardsAfterSplit.findAll {
                it.getParentShardId() == parentShardId
            }
            assertEquals(2, splitChildShards.size())
            def childShardIds = splitChildShards.collect { it.getShardId() }
            logger.info("Split produced child shards: ${childShardIds} from parent ${parentShardId}")

            def sortedChildren = splitChildShards.sort {
                new BigInteger(it.getHashKeyRange().getStartingHashKey())
            }
            def leftChild = sortedChildren[0]
            def rightChild = sortedChildren[1]
            def leftChildHash = getShardMiddleHashKey(leftChild)
            def rightChildHash = getShardMiddleHashKey(rightChild)

            // Step 3: write to both child shards explicitly.
            writeRange(51, 75, "split_left", leftChildHash)
            writeRange(76, 100, "split_right", rightChildHash)
            // Step 4: verify post-split data is imported.
            waitForExactCount(100, 240)

            // Extra check: shard relation/progress is visible in SHOW ROUTINE LOAD.
            waitForDataSourceShardView([parentShardId, leftChild.getShardId(), rightChild.getShardId()], 180)
            test2PreparedForReuse = true
        } finally {
            if (!test2PreparedForReuse) {
                stopRoutineLoadIfCreated()
                deleteStreamIfCreated()
                dropTableIfCreated()
            }
        }

        // test3 : merge split child shards and verify no data loss or duplicate consumption
        try {
            assertTrue(test2PreparedForReuse, "Test3 requires test2 to complete split stage")
            assertTrue(splitChildShards != null && splitChildShards.size() == 2,
                "Expected two child shards after split before merge")

            def sortedChildren = splitChildShards.sort {
                new BigInteger(it.getHashKeyRange().getStartingHashKey())
            }
            def leftChild = sortedChildren[0]
            def rightChild = sortedChildren[1]

            // Step 5: merge split child shards.
            logger.info("Merging adjacent shards: ${leftChild.getShardId()} + ${rightChild.getShardId()}")
            kinesisClient.mergeShards(new MergeShardsRequest()
                .withStreamName(streamName)
                .withShardToMerge(leftChild.getShardId())
                .withAdjacentShardToMerge(rightChild.getShardId()))

            def openShardsAfterMerge = waitForOpenShardCount(1, 180)
            assertEquals(1, openShardsAfterMerge.size())
            def mergedShard = openShardsAfterMerge[0]
            def mergedParentIds = [mergedShard.getParentShardId(), mergedShard.getAdjacentParentShardId()].findAll {
                it != null
            }
            assertEquals(2, mergedParentIds.size())
            assertTrue(mergedParentIds.contains(leftChild.getShardId()))
            assertTrue(mergedParentIds.contains(rightChild.getShardId()))
            logger.info("Merged shard ${mergedShard.getShardId()} has parents ${mergedParentIds}")

            def mergedHash = getShardMiddleHashKey(mergedShard)
            // Step 6: write to merged shard explicitly.
            writeRange(101, 150, "merged", mergedHash)
            // Step 7: verify post-merge data is imported.
            waitForExactCount(150, 240)

            // Extra check: progress inheritance across old/new shards is visible.
            waitForDataSourceShardView([leftChild.getShardId(), rightChild.getShardId(), mergedShard.getShardId()], 180)

            def finalResult = sql "SELECT COUNT(*), COUNT(DISTINCT id), MIN(id), MAX(id) FROM test_kinesis_shard_change"
            assertEquals(150L, toLongValue(finalResult[0][0]))
            assertEquals(150L, toLongValue(finalResult[0][1]))
            assertEquals(1, toIntValue(finalResult[0][2]))
            assertEquals(150, toIntValue(finalResult[0][3]))

            def finalState = getJobState()
            assertNotEquals("CANCELLED", finalState)
            logger.info("Final routine load state after shard split+merge: ${finalState}")
        } finally {
            stopRoutineLoadIfCreated()
            deleteStreamIfCreated()
            dropTableIfCreated()
        }
    } finally {
        kinesisClient.shutdown()
    }
}
