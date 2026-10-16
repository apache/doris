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
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import groovy.json.JsonOutput
import org.awaitility.Awaitility
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest
import software.amazon.awssdk.services.sqs.model.QueueAttributeName
import software.amazon.awssdk.services.sqs.model.SendMessageRequest

import static java.util.concurrent.TimeUnit.SECONDS

suite("test_streaming_insert_job_s3_event_aksk") {
    String queueUrl = getConf("s3EventQueueUrl", "")
    if (!queueUrl) {
        logger.info("Skip ${name}: s3EventQueueUrl is not configured")
        return
    }
    String eventPrefix = getConf("s3EventPrefix", "")
    def config = context.config
    ["awsEndpoint", "awsRegion", "awsBucket", "awsAccessKey", "awsSecretKey"].each { key ->
        if (!config[key]?.trim()) {
            throw new IllegalArgumentException("${key} is required for ${name}")
        }
    }
    if (!eventPrefix) {
        throw new IllegalArgumentException("s3EventPrefix is required for ${name}")
    }
    def client = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(config.awsEndpoint, config.awsRegion))
            .withCredentials(new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(config.awsAccessKey, config.awsSecretKey))).build()
    String bucket = config.awsBucket
    String prefix = "${eventPrefix.replaceAll('/+$', '')}/${name}/${UUID.randomUUID()}"
    String jobName = name
    def sqsClient = SqsClient.builder().region(Region.of(config.awsRegion))
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.awsAccessKey, config.awsSecretKey))).build()
    def queueCounts = {
        sqsClient.getQueueAttributes(GetQueueAttributesRequest.builder().queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE).build()).attributes()
    }
    def awaitQueueEmpty = {
        // Wait for acknowledgement and eventual consistency before measuring a new backlog.
        Awaitility.await().pollInSameThread().atMost(180, SECONDS).pollInterval(1, SECONDS).until {
            def counts = queueCounts()
            counts[QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES].toLong() == 0 &&
                    counts[QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE].toLong() == 0
        }
    }
    def upload = { String file, String content ->
        String key = "${prefix}/${file}"
        client.putObject(bucket, key, content)
    }
    def awaitRows = { int rows ->
        Awaitility.await().pollInSameThread().atMost(300, SECONDS).pollInterval(1, SECONDS).until {
            def job = sql("SELECT Status, ErrorMsg FROM jobs(\"type\"=\"insert\") WHERE Name='${jobName}'")
            if (job && job[0][0] == "PAUSED") {
                throw new IllegalStateException("S3 EVENT job paused: ${job[0][1]}")
            }
            sql("SELECT COUNT(*) FROM test_s3_event_aksk")[0][0] as long == rows
        }
    }
    def awaitOffsets = { List<String> files ->
        Awaitility.await().pollInSameThread().atMost(120, SECONDS).pollInterval(1, SECONDS).until {
            def rows = sql("SELECT CurrentOffset, EndOffset, LastSourceEventTimestamp FROM jobs(\"type\"=\"insert\") WHERE Name='${jobName}'")
            if (!rows || !rows[0][0] || !rows[0][1]) {
                return false
            }
            def current = parseJson(rows[0][0])
            def end = parseJson(rows[0][1])
            // Queue counts are approximate; check their shape and range, not an exact backlog.
            files.any { current == [fileName: "${prefix}/${it}".toString()] } &&
                    end.keySet() == ["fileName", "lagMessages"].toSet() &&
                    end.fileName == current.fileName && end.lagMessages instanceof Number &&
                    end.lagMessages >= 0 &&
                    rows[0][2]?.toString()?.isLong() &&
                    rows[0][2].toLong() > 0 && rows[0][2].toLong() < 10_000_000_000L
        }
    }
    try {
        sql "DROP JOB IF EXISTS WHERE jobname='${jobName}'"
        sql "DROP TABLE IF EXISTS test_s3_event_aksk"
        sql """CREATE TABLE test_s3_event_aksk (id INT, value STRING)
               UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES("replication_num"="1")"""

        // Upload before CREATE JOB so TVF analysis has a real file to inspect.
        upload("z.csv", "1,first\n")
        upload("ignored.txt", "999,excluded\n")
        sql """CREATE JOB ${jobName}
               PROPERTIES(
                   "s3.ingestion_mode" = "NOTIFICATION",
                   "s3.event.source" = "SQS",
                   "s3.sqs.queue_url" = ${JsonOutput.toJson(queueUrl)},
                   "s3.max_batch_files" = "1",
                   "max_interval" = "1"
               )
               ON STREAMING DO INSERT INTO test_s3_event_aksk
               SELECT * FROM S3(
                   "uri" = ${JsonOutput.toJson("s3://${bucket}/${prefix}/{z,a,b,c,comma*}.csv")},
                   "format" = "csv",
                   "provider" = "S3",
                   "column_separator" = ",",
                   "s3.endpoint" = ${JsonOutput.toJson(config.awsEndpoint)},
                   "s3.region" = ${JsonOutput.toJson(config.awsRegion)},
                   "s3.access_key" = ${JsonOutput.toJson(config.awsAccessKey)},
                   "s3.secret_key" = ${JsonOutput.toJson(config.awsSecretKey)}
               )"""
        awaitRows(1)
        awaitOffsets(["z.csv"])
        order_qt_first "SELECT * FROM test_s3_event_aksk"

        // A smaller key uploaded after the first commit must still be discovered in NOTIFICATION mode.
        upload("a.csv", "2,second\n")
        awaitRows(2)
        awaitOffsets(["a.csv"])
        order_qt_second "SELECT * FROM test_s3_event_aksk"

        awaitQueueEmpty()
        sql "PAUSE JOB WHERE jobname='${jobName}'"
        def pausedProgress = sql("SELECT CurrentOffset, LastSourceEventTimestamp FROM jobs(\"type\"=\"insert\") WHERE Name='${jobName}'")[0]
        upload("b.csv", "3,third\n")
        upload("c.csv", "4,fourth\n")
        // Uploaded files must not advance the paused job; RESUME below verifies they are eventually consumed.
        Awaitility.await().pollInSameThread().atMost(120, SECONDS).pollInterval(1, SECONDS).until {
            def job = sql("SELECT Status, CurrentOffset, LastSourceEventTimestamp FROM jobs(\"type\"=\"insert\") WHERE Name='${jobName}'")
            job[0][0] == "PAUSED" && job[0][1] == pausedProgress[0] && job[0][2] == pausedProgress[1] &&
                    (sql("SELECT COUNT(*) FROM test_s3_event_aksk")[0][0] as long) == 2
        }
        sql "RESUME JOB WHERE jobname='${jobName}'"
        awaitRows(4)
        // SQS does not guarantee the receive order of these two uploads.
        awaitOffsets(["b.csv", "c.csv"])
        order_qt_final "SELECT id, value, COUNT(*) FROM test_s3_event_aksk GROUP BY id, value"
        awaitQueueEmpty()

        // These decoys do not match the job glob. They must not be read when the comma key is rewritten.
        upload("comma", "9001,wrong_first\n")
        upload("other.csv", "9002,wrong_second\n")
        upload("comma,other.csv", "5,comma\n")
        awaitRows(5)
        awaitOffsets(["comma,other.csv"])
        awaitQueueEmpty()
        order_qt_event_comma "SELECT * FROM test_s3_event_aksk"

        def committedProgress = sql("SELECT Id, LastSourceEventTimestamp FROM jobs(\"type\"=\"insert\") WHERE Name='${jobName}'")[0]
        String jobId = committedProgress[0].toString()
        long committedEventTime = committedProgress[1].toLong()
        Awaitility.await().pollInSameThread().atMost(120, SECONDS).pollInterval(1, SECONDS).until {
            boolean metricsReady = false
            httpTest {
                endpoint getMasterIp() + ":" + getMasterPort("http")
                uri "/metrics?type=json"
                op "get"
                check { code, body ->
                    assertEquals(200, code)
                    def metrics = parseJson(body).findAll {
                        it.tags?.job_id == jobId && it.tags?.job_name == jobName
                    }
                    def lag = metrics.find { it.tags.metric == "doris_fe_streaming_job_per_job_lag_messages" }
                    def timestamp = metrics.find {
                        it.tags.metric == "doris_fe_streaming_job_per_job_last_source_event_timestamp_seconds"
                    }
                    metricsReady = lag?.value != null && new BigDecimal(lag.value.toString()).signum() >= 0 &&
                            timestamp?.value != null &&
                            new BigDecimal(timestamp.value.toString()).compareTo(BigDecimal.valueOf(committedEventTime)) == 0
                }
            }
            metricsReady
        }

        String committedOffset = sql("SELECT CurrentOffset FROM jobs(\"type\"=\"insert\") WHERE Name='${jobName}'")[0][0]
        // A message without S3 records is acknowledged without advancing the job.
        sql "PAUSE JOB WHERE jobname='${jobName}'"
        sqsClient.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody("{}").build())
        Awaitility.await().pollInSameThread().atMost(120, SECONDS).pollInterval(1, SECONDS).until {
            queueCounts()[QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES].toLong() > 0
        }
        sql "RESUME JOB WHERE jobname='${jobName}'"
        awaitQueueEmpty()
        Awaitility.await().pollInSameThread().atMost(120, SECONDS).pollInterval(1, SECONDS).until {
            def job = sql("SELECT Status, CurrentOffset FROM jobs(\"type\"=\"insert\") WHERE Name='${jobName}'")
            job[0][0] == "RUNNING" && job[0][1] == committedOffset &&
                    (sql("SELECT COUNT(*) FROM test_s3_event_aksk")[0][0] as long) == 5
        }
    } finally {
        try {
            sql "DROP JOB IF EXISTS WHERE jobname='${jobName}'"
        } finally {
            sqsClient.close()
            client.shutdown()
        }
    }
}
