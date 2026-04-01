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

suite("test_kinesis_routine_load") {
    def ak = context.config.awsAccessKey
    def sk = context.config.awsSecretKey
    def region = context.config.awsRegion

    if (!region || !ak || !sk) {
        logger.info("Skip ${name} case, AWS config not provided")
        return
    }

    def streamName = "doris-test-kinesis-${UUID.randomUUID().toString().substring(0, 8)}"
    def tableName = "kinesis_test_table"
    def jobName = "kinesis_routine_load_job"

    // Create Kinesis client
    def credentials = new BasicAWSCredentials(ak, sk)
    def kinesisClient = AmazonKinesisClientBuilder.standard()
        .withRegion(region)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .build()

    try {
        // Create stream
        logger.info("Creating Kinesis stream: ${streamName}")
        def createRequest = new CreateStreamRequest()
            .withStreamName(streamName)
            .withShardCount(1)
        kinesisClient.createStream(createRequest)

        // Wait for stream to be active and shard metadata to be available.
        logger.info("Waiting for stream to be active...")
        def describeRequest = new DescribeStreamRequest().withStreamName(streamName)
        def streamReady = false
        for (int i = 0; i < 60; i++) {
            try {
                def result = kinesisClient.describeStream(describeRequest)
                def streamDescription = result.getStreamDescription()
                if (streamDescription.getStreamStatus() == "ACTIVE" && !streamDescription.getShards().isEmpty()) {
                    streamReady = true
                    break
                }
            } catch (ResourceNotFoundException e) {
                // Stream metadata may not be visible immediately after create.
            }
            Thread.sleep(1000)
        }
        assertTrue(streamReady, "Stream failed to become ready")
        logger.info("Stream is ready")

        // Write test data
        logger.info("Writing test data to Kinesis...")
        for (int i = 1; i <= 100; i++) {
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
        logger.info("Test data written successfully")

        // Create Doris table
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

        // Create routine load job
        sql """
            CREATE ROUTINE LOAD ${jobName} ON ${tableName}
            PROPERTIES ("format" = "json", "desired_concurrent_number" = "1")
            FROM KINESIS (
                "aws.region" = "${region}",
                "aws.access_key" = "${ak}",
                "aws.secret_key" = "${sk}",
                "kinesis_stream" = "${streamName}",
                "property.kinesis_default_pos" = "TRIM_HORIZON"
            )
        """

        // Wait for job to start
        Thread.sleep(5000)

        // Check job status
        def jobState = sql "SHOW ROUTINE LOAD FOR ${jobName}"
        logger.info("Routine load job state: ${jobState}")
        assertTrue(jobState.size() > 0)

        // Wait for data to be loaded
        Thread.sleep(30000)

        // Verify data
        def result = sql "SELECT COUNT(*) FROM ${tableName}"
        logger.info("Loaded rows: ${result[0][0]}")
        assertTrue(result[0][0] >= 100, "Expected at least 100 rows, got ${result[0][0]}")

        // Stop routine load
        sql "STOP ROUTINE LOAD FOR ${jobName}"

    } finally {
        // Cleanup
        try {
            kinesisClient.deleteStream(new DeleteStreamRequest().withStreamName(streamName))
            logger.info("Deleted stream: ${streamName}")
        } catch (Exception e) {
            logger.warn("Failed to delete stream: ${e.message}")
        }
        kinesisClient.shutdown()
        sql "DROP TABLE IF EXISTS ${tableName}"
    }
}
