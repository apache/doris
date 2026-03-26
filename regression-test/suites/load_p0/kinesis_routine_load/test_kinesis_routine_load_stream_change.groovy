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

suite("test_kinesis_routine_load_stream_change", "nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableKinesisTest")
    String awsRegion = context.config.otherConfigs.get("awsRegion")
    String awsAccessKey = context.config.otherConfigs.get("awsAccessKey")
    String awsSecretKey = context.config.otherConfigs.get("awsSecretKey")

    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Skip Kinesis test")
        return
    }

    def streamName1 = "doris-stream1-${UUID.randomUUID().toString().substring(0, 8)}"
    def streamName2 = "doris-stream2-${UUID.randomUUID().toString().substring(0, 8)}"
    def tableName = "test_kinesis_stream_change"
    def jobName = "testKinesisStreamChange"

    def credentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    def kinesisClient = AmazonKinesisClientBuilder.standard()
        .withRegion(awsRegion)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .build()

    try {
        // Create two streams
        kinesisClient.createStream(new CreateStreamRequest().withStreamName(streamName1).withShardCount(1))
        kinesisClient.createStream(new CreateStreamRequest().withStreamName(streamName2).withShardCount(1))

        for (def stream : [streamName1, streamName2]) {
            def streamActive = false
            for (int i = 0; i < 30; i++) {
                def result = kinesisClient.describeStream(new DescribeStreamRequest().withStreamName(stream))
                if (result.getStreamDescription().getStreamStatus() == "ACTIVE") {
                    streamActive = true
                    break
                }
                Thread.sleep(2000)
            }
            assertTrue(streamActive)
        }

        // Write to stream1
        for (int i = 1; i <= 30; i++) {
            def data = "{\"id\": ${i}, \"name\": \"user_${i}\"}"
            kinesisClient.putRecord(new PutRecordRequest()
                .withStreamName(streamName1)
                .withPartitionKey("key_${i}")
                .withData(ByteBuffer.wrap(data.getBytes("UTF-8"))))
        }

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                id INT,
                name VARCHAR(100)
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """

        sql """
            CREATE ROUTINE LOAD ${jobName} ON ${tableName}
            PROPERTIES ("format" = "json")
            FROM KINESIS (
                "aws.region" = "${awsRegion}",
                "aws.access_key" = "${awsAccessKey}",
                "aws.secret_key" = "${awsSecretKey}",
                "kinesis_stream" = "${streamName1}",
                "property.kinesis_default_pos" = "TRIM_HORIZON"
            )
        """

        Thread.sleep(20000)

        def result1 = sql "SELECT COUNT(*) FROM ${tableName}"
        logger.info("Loaded from stream1: ${result1[0][0]}")

        // Change to stream2
        sql "PAUSE ROUTINE LOAD FOR ${jobName}"
        Thread.sleep(5000)

        sql """
            ALTER ROUTINE LOAD FOR ${jobName}
            FROM KINESIS (
                "kinesis_stream" = "${streamName2}"
            )
        """

        // Write to stream2
        for (int i = 31; i <= 60; i++) {
            def data = "{\"id\": ${i}, \"name\": \"user_${i}\"}"
            kinesisClient.putRecord(new PutRecordRequest()
                .withStreamName(streamName2)
                .withPartitionKey("key_${i}")
                .withData(ByteBuffer.wrap(data.getBytes("UTF-8"))))
        }

        sql "RESUME ROUTINE LOAD FOR ${jobName}"
        Thread.sleep(20000)

        def result2 = sql "SELECT COUNT(*) FROM ${tableName}"
        logger.info("Total loaded: ${result2[0][0]}")
        assertTrue(result2[0][0] > result1[0][0])

        sql "STOP ROUTINE LOAD FOR ${jobName}"

    } finally {
        try {
            kinesisClient.deleteStream(new DeleteStreamRequest().withStreamName(streamName1))
            kinesisClient.deleteStream(new DeleteStreamRequest().withStreamName(streamName2))
        } catch (Exception e) {
            logger.warn("Failed to delete streams: ${e.message}")
        }
        kinesisClient.shutdown()
        sql "DROP TABLE IF EXISTS ${tableName}"
    }
}
