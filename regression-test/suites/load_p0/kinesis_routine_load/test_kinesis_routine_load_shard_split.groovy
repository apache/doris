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

suite("test_kinesis_routine_load_shard_split") {

    String enabled = context.config.otherConfigs.get("enableKinesisTest")
    String awsRegion = context.config.otherConfigs.get("awsRegion")
    String awsAccessKey = context.config.otherConfigs.get("awsAccessKey")
    String awsSecretKey = context.config.otherConfigs.get("awsSecretKey")

    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Skip ${name} case, Kinesis test not enabled")
        return
    }

    if (!awsRegion || !awsAccessKey || !awsSecretKey) {
        logger.info("Skip ${name} case, AWS config not provided")
        return
    }

    def streamName = "doris-test-split-${UUID.randomUUID().toString().substring(0, 8)}"
    def tableName = "test_kinesis_shard_split"
    def jobName = "testKinesisSplit"

    def credentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    def kinesisClient = AmazonKinesisClientBuilder.standard()
        .withRegion(awsRegion)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .build()

    try {
        logger.info("Creating stream: ${streamName}")
        kinesisClient.createStream(new CreateStreamRequest()
            .withStreamName(streamName)
            .withShardCount(1))

        def streamActive = false
        for (int i = 0; i < 30; i++) {
            def result = kinesisClient.describeStream(new DescribeStreamRequest().withStreamName(streamName))
            if (result.getStreamDescription().getStreamStatus() == "ACTIVE") {
                streamActive = true
                break
            }
            Thread.sleep(2000)
        }
        assertTrue(streamActive)

        // Write initial data
        logger.info("Writing initial data")
        for (int i = 1; i <= 50; i++) {
            def data = "{\"id\": ${i}, \"name\": \"user_${i}\", \"age\": ${20 + i % 50}}"
            kinesisClient.putRecord(new PutRecordRequest()
                .withStreamName(streamName)
                .withPartitionKey("key_${i}")
                .withData(ByteBuffer.wrap(data.getBytes("UTF-8"))))
        }

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
            PROPERTIES ("format" = "json", "desired_concurrent_number" = "2")
            FROM KINESIS (
                "aws.region" = "${awsRegion}",
                "aws.access_key" = "${awsAccessKey}",
                "aws.secret_key" = "${awsSecretKey}",
                "kinesis_stream" = "${streamName}",
                "property.kinesis_default_pos" = "TRIM_HORIZON"
            )
        """

        Thread.sleep(15000)

        // Split shard
        logger.info("Splitting shard")
        def streamDesc = kinesisClient.describeStream(new DescribeStreamRequest().withStreamName(streamName))
        def shards = streamDesc.getStreamDescription().getShards()
        def openShard = shards.find { it.getSequenceNumberRange().getEndingSequenceNumber() == null }

        if (openShard != null) {
            def startHash = new BigInteger(openShard.getHashKeyRange().getStartingHashKey())
            def endHash = new BigInteger(openShard.getHashKeyRange().getEndingHashKey())
            def newHash = startHash.add(endHash).divide(BigInteger.valueOf(2))

            kinesisClient.splitShard(new SplitShardRequest()
                .withStreamName(streamName)
                .withShardToSplit(openShard.getShardId())
                .withNewStartingHashKey(newHash.toString()))

            Thread.sleep(10000)
        }

        // Write more data after split
        logger.info("Writing data after split")
        for (int i = 51; i <= 100; i++) {
            def data = "{\"id\": ${i}, \"name\": \"user_${i}\", \"age\": ${20 + i % 50}}"
            kinesisClient.putRecord(new PutRecordRequest()
                .withStreamName(streamName)
                .withPartitionKey("key_${i}")
                .withData(ByteBuffer.wrap(data.getBytes("UTF-8"))))
        }

        Thread.sleep(20000)

        def result = sql "SELECT COUNT(*) FROM ${tableName}"
        logger.info("Total loaded rows: ${result[0][0]}")
        assertTrue(result[0][0] >= 100)

        sql "STOP ROUTINE LOAD FOR ${jobName}"

    } finally {
        try {
            kinesisClient.deleteStream(new DeleteStreamRequest().withStreamName(streamName))
        } catch (Exception e) {
            logger.warn("Failed to delete stream: ${e.message}")
        }
        kinesisClient.shutdown()
        sql "DROP TABLE IF EXISTS ${tableName}"
    }
}
