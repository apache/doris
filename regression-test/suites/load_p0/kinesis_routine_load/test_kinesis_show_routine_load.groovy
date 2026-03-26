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

    def streamName = "doris-test-show-${UUID.randomUUID().toString().substring(0, 8)}"
    def tableName = "test_kinesis_show_routine_load"
    def jobName = "testKinesisShow"

    def credentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    def kinesisClient = AmazonKinesisClientBuilder.standard()
        .withRegion(awsRegion)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .build()

    try {
        // Create stream
        logger.info("Creating Kinesis stream: ${streamName}")
        kinesisClient.createStream(new CreateStreamRequest()
            .withStreamName(streamName)
            .withShardCount(1))

        // Wait for stream active
        def streamActive = false
        for (int i = 0; i < 30; i++) {
            def result = kinesisClient.describeStream(new DescribeStreamRequest().withStreamName(streamName))
            if (result.getStreamDescription().getStreamStatus() == "ACTIVE") {
                streamActive = true
                break
            }
            Thread.sleep(2000)
        }
        assertTrue(streamActive, "Stream failed to become active")

        // Write test data
        for (int i = 1; i <= 10; i++) {
            def data = "${i},name_${i},2023-08-01,value_${i},2023-08-01 12:00:00,extra_${i}"
            kinesisClient.putRecord(new PutRecordRequest()
                .withStreamName(streamName)
                .withPartitionKey("key_${i}")
                .withData(ByteBuffer.wrap(data.getBytes("UTF-8"))))
        }

        // Create table
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

        // Create routine load job
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

        Thread.sleep(5000)

        // Test SHOW ROUTINE LOAD
        def result = sql "SHOW ROUTINE LOAD FOR ${jobName}"
        assertTrue(result.size() > 0)
        logger.info("Job state: ${result[0][8]}")

        // Test SHOW ROUTINE LOAD with WHERE
        result = sql "SHOW ROUTINE LOAD WHERE Name = '${jobName}'"
        assertTrue(result.size() > 0)

        // Wait for data load
        Thread.sleep(20000)

        // Verify data
        result = sql "SELECT COUNT(*) FROM ${tableName}"
        logger.info("Loaded rows: ${result[0][0]}")
        assertTrue(result[0][0] >= 10)

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
