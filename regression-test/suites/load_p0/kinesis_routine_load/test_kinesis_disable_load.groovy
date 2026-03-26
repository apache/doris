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

suite("test_kinesis_disable_load", "nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableKinesisTest")
    String awsRegion = context.config.otherConfigs.get("awsRegion")
    String awsAccessKey = context.config.otherConfigs.get("awsAccessKey")
    String awsSecretKey = context.config.otherConfigs.get("awsSecretKey")

    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Skip Kinesis test")
        return
    }

    def streamName = "doris-disable-${UUID.randomUUID().toString().substring(0, 8)}"
    def tableName = "test_kinesis_disable_load"
    def jobName = "testKinesisDisable"

    def credentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    def kinesisClient = AmazonKinesisClientBuilder.standard()
        .withRegion(awsRegion)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .build()

    try {
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

        for (int i = 1; i <= 10; i++) {
            def data = "${i},name_${i},2023-08-01,value_${i},2023-08-01 12:00:00,extra_${i}"
            kinesisClient.putRecord(new PutRecordRequest()
                .withStreamName(streamName)
                .withPartitionKey("key_${i}")
                .withData(ByteBuffer.wrap(data.getBytes("UTF-8"))))
        }

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                k1 INT,
                k2 STRING,
                v1 DATE,
                v2 STRING,
                v3 DATETIME,
                v4 STRING
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 3
            PROPERTIES ("replication_num" = "1")
        """

        def backends = sql_return_maparray('SHOW BACKENDS')
        def beId = backends[0].BackendId

        try {
            sql """ALTER SYSTEM MODIFY BACKEND "${beId}" SET ("disable_load" = "true")"""

            sql """
                CREATE ROUTINE LOAD ${jobName} ON ${tableName}
                COLUMNS TERMINATED BY ","
                FROM KINESIS (
                    "aws.region" = "${awsRegion}",
                    "aws.access_key" = "${awsAccessKey}",
                    "aws.secret_key" = "${awsSecretKey}",
                    "kinesis_stream" = "${streamName}",
                    "property.kinesis_default_pos" = "TRIM_HORIZON"
                )
            """

            def count = 0
            while (true) {
                def state = sql "SHOW ROUTINE LOAD FOR ${jobName}"
                logger.info("State: ${state[0][8]}, Reason: ${state[0][17]}")

                if (state[0][17].toString().contains("no alive backends")) {
                    break
                }
                if (count >= 60) {
                    logger.error("Timeout waiting for error")
                    assertEquals(1, 2)
                    break
                }
                Thread.sleep(1000)
                count++
            }
        } finally {
            sql """ALTER SYSTEM MODIFY BACKEND "${beId}" SET ("disable_load" = "false")"""
            sql "STOP ROUTINE LOAD FOR ${jobName}"
        }

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
