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

import static java.util.concurrent.TimeUnit.SECONDS

suite("test_streaming_insert_job_s3_event_iam_role") {
    String queueUrl = getConf("s3EventIamRoleQueueUrl", "")
    if (!queueUrl) {
        logger.info("Skip ${name}: s3EventIamRoleQueueUrl is not configured")
        return
    }
    String eventPrefix = getConf("s3EventIamRolePrefix", "")
    def config = context.config
    ["awsEndpoint", "awsRegion", "awsBucket", "awsAccessKey", "awsSecretKey"].each { key ->
        if (!config[key]?.trim()) {
            throw new IllegalArgumentException("${key} is required for ${name}")
        }
    }
    if (!eventPrefix) {
        throw new IllegalArgumentException("s3EventIamRolePrefix is required for ${name}")
    }
    if (!config.awsRoleArn?.trim()) {
        throw new IllegalArgumentException("awsRoleArn is required for ${name}")
    }
    // Upload with test AKSK; the Doris TVF uses only IAM Role properties.
    def client = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(config.awsEndpoint, config.awsRegion))
            .withCredentials(new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(config.awsAccessKey, config.awsSecretKey))).build()
    String bucket = config.awsBucket
    String prefix = "${eventPrefix.replaceAll('/+$', '')}/${name}/${UUID.randomUUID()}"
    String jobName = name
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
            sql("SELECT COUNT(*) FROM test_s3_event_iam_role")[0][0] as long == rows
        }
    }
    def awaitOffsets = { String file ->
        Awaitility.await().pollInSameThread().atMost(120, SECONDS).pollInterval(1, SECONDS).until {
            def rows = sql("SELECT CurrentOffset, EndOffset, LastSourceEventTimestamp FROM jobs(\"type\"=\"insert\") WHERE Name='${jobName}'")
            if (!rows || !rows[0][0] || !rows[0][1]) {
                return false
            }
            def current = parseJson(rows[0][0])
            def end = parseJson(rows[0][1])
            // A numeric backlog also verifies GetQueueAttributes access through the assumed role.
            current == [fileName: "${prefix}/${file}".toString()] &&
                    end.keySet() == ["fileName", "lagMessages"].toSet() &&
                    end.fileName == current.fileName && end.lagMessages instanceof Number &&
                    end.lagMessages >= 0 &&
                    rows[0][2]?.toString()?.isLong() &&
                    rows[0][2].toLong() > 0 && rows[0][2].toLong() < 10_000_000_000L
        }
    }
    try {
        sql "DROP JOB IF EXISTS WHERE jobname='${jobName}'"
        sql "DROP TABLE IF EXISTS test_s3_event_iam_role"
        sql """CREATE TABLE test_s3_event_iam_role (id INT, value STRING)
               UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES("replication_num"="1")"""
        upload("z.csv", "1,role_first\n")
        sql """CREATE JOB ${jobName}
               PROPERTIES(
                   "s3.ingestion_mode" = "NOTIFICATION",
                   "s3.event.source" = "SQS",
                   "s3.sqs.queue_url" = ${JsonOutput.toJson(queueUrl)},
                   "s3.max_batch_files" = "1",
                   "max_interval" = "1"
               )
               ON STREAMING DO INSERT INTO test_s3_event_iam_role
               SELECT * FROM S3(
                   "uri" = ${JsonOutput.toJson("s3://${bucket}/${prefix}/*.csv")},
                   "format" = "csv",
                   "provider" = "S3",
                   "column_separator" = ",",
                   "s3.endpoint" = ${JsonOutput.toJson(config.awsEndpoint)},
                   "s3.region" = ${JsonOutput.toJson(config.awsRegion)},
                   "s3.role_arn" = ${JsonOutput.toJson(config.awsRoleArn)}
                   ${config.awsExternalId ? ', "s3.external_id"=' + JsonOutput.toJson(config.awsExternalId) : ''}
               )"""
        awaitRows(1)
        awaitOffsets("z.csv")
        order_qt_first "SELECT * FROM test_s3_event_iam_role"

        // The next batch also exercises message acknowledgement with the assumed role.
        upload("a.csv", "2,role_second\n")
        awaitRows(2)
        awaitOffsets("a.csv")
        order_qt_final "SELECT id, value, COUNT(*) FROM test_s3_event_iam_role GROUP BY id, value"
        // Let the next fetchMeta acknowledge the final committed SQS message before dropping the job.
        sleep(5_000)
    } finally {
        try {
            sql "DROP JOB IF EXISTS WHERE jobname='${jobName}'"
        } finally {
            client.shutdown()
        }
    }
}
