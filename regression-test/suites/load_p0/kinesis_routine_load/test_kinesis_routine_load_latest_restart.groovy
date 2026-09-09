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
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.model.*
import org.apache.doris.regression.suite.ClusterOptions

import java.nio.ByteBuffer

suite("test_kinesis_routine_load_latest_restart", "docker") {
    def region = context.config.awsRegion ?: context.config.otherConfigs.get("awsRegion")
    def ak = context.config.awsAccessKey ?: context.config.otherConfigs.get("awsAccessKey")
    def sk = context.config.awsSecretKey ?: context.config.otherConfigs.get("awsSecretKey")

    if (!region || !ak || !sk) {
        logger.info("Skip ${name} case, missing AWS config: region=${region}, ak=${ak != null}, sk=${sk != null}")
        return
    }

    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    docker(options) {
        def suffix = UUID.randomUUID().toString().substring(0, 8)
        def streamName = "doris-latest-restart-${suffix}"
        def jobName = "test_kinesis_latest_restart_${suffix}"
        def kinesisClient = AmazonKinesisClientBuilder.standard()
            .withRegion(region)
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(ak, sk)))
            .build()
        def streamCreated = false
        def jobCreated = false

        def getJob = {
            return sql_return_maparray("SHOW ROUTINE LOAD FOR ${jobName}")[0]
        }
        def writeRecord = { shard, int id ->
            def request = new PutRecordRequest()
                .withStreamName(streamName)
                .withPartitionKey("key_${id}")
                .withExplicitHashKey(shard.hashKeyRange.startingHashKey)
                .withData(ByteBuffer.wrap("{\"id\":${id},\"value\":${id * 100}}".getBytes("UTF-8")))
            // A newly ACTIVE stream may not be visible to PutRecord immediately.
            for (int retry = 0; retry < 20; retry++) {
                try {
                    return kinesisClient.putRecord(request).sequenceNumber
                } catch (ResourceNotFoundException e) {
                    if (retry == 19) {
                        throw e
                    }
                    Thread.sleep(500)
                }
            }
        }

        try {
            kinesisClient.createStream(new CreateStreamRequest().withStreamName(streamName).withShardCount(2))
            streamCreated = true
            def shards = []
            awaitUntil(120) {
                try {
                    def description = kinesisClient.describeStream(new DescribeStreamRequest()
                        .withStreamName(streamName)).streamDescription
                    shards = description.shards.sort { it.shardId }
                    return description.streamStatus == "ACTIVE" && shards.size() == 2
                } catch (ResourceNotFoundException e) {
                    // Metadata may not be visible immediately after create.
                    return false
                }
            }

            // One shard has a historical prefix; the other is empty at initialization.
            def expectedProgress = [:]
            for (int id = 1; id <= 3; id++) {
                expectedProgress[shards[0].shardId] = writeRecord(shards[0], id)
            }
            expectedProgress[shards[1].shardId] = "TRIM_HORIZON"

            sql "DROP TABLE IF EXISTS test_kinesis_latest_restart"
            sql """
                CREATE TABLE test_kinesis_latest_restart (
                    id INT,
                    value INT
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            sql """
                CREATE ROUTINE LOAD ${jobName} ON test_kinesis_latest_restart
                PROPERTIES (
                    "format" = "json",
                    "desired_concurrent_number" = "1"
                )
                FROM KINESIS (
                    "aws.region" = "${region}",
                    "aws.access_key" = "${ak}",
                    "aws.secret_key" = "${sk}",
                    "kinesis_stream" = "${streamName}",
                    "property.kinesis_default_pos" = "LATEST"
                )
            """
            jobCreated = true

            // No new records arrive yet: progress must be resolved without a data commit.
            // Merely waiting for RUNNING would also pass on the old symbolic LATEST path.
            awaitUntil(180) {
                def job = getJob()
                return job.State == "RUNNING" && parseJson(job.Progress) == expectedProgress
            }
            sql "PAUSE ROUTINE LOAD FOR ${jobName}"
            awaitUntil(60) { getJob().State == "PAUSED" }
            qt_before_restart "SELECT COUNT(*) FROM test_kinesis_latest_restart"
            assertEquals(expectedProgress, parseJson(getJob().Progress))

            // Discard all BE iterators before writing records, so a cached iterator cannot
            // hide a lost FE starting position. The paused job cannot consume these records.
            cluster.restartBackends()
            for (int id = 4; id <= 9; id++) {
                writeRecord(shards[id % 2], id)
            }

            // Replay the initialization journal before the first successful data task.
            cluster.restartFrontends(cluster.getMasterFe().index)
            Thread.sleep(30000)
            context.reconnectFe()
            awaitUntil(120) { getJob().State == "PAUSED" }
            assertEquals(expectedProgress, parseJson(getJob().Progress))

            sql "RESUME ROUTINE LOAD FOR ${jobName}"
            awaitUntil(180) {
                return sql("SELECT COUNT(*) FROM test_kinesis_latest_restart")[0][0] >= 6
            }
            // Exact rows detect skipped arrivals, reloaded history, and duplicate ingestion.
            order_qt_after_restart "SELECT id, value FROM test_kinesis_latest_restart"
        } finally {
            if (jobCreated) {
                try {
                    sql "STOP ROUTINE LOAD FOR ${jobName}"
                } catch (Exception e) {
                    logger.warn("Failed to stop routine load ${jobName}: ${e.message}")
                }
            }
            try {
                if (streamCreated) {
                    kinesisClient.deleteStream(new DeleteStreamRequest().withStreamName(streamName))
                }
            } finally {
                kinesisClient.shutdown()
            }
        }
    }
}
