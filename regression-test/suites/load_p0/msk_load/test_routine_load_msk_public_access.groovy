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

import com.google.common.base.Strings;

suite("test_routine_load_msk_public_access") {
    def topicName = "routineload-test"
    def tableName = "test_routine_load_iam_aksk_table"
    def jobName = "test_routine_load_iam_aksk_job"

    if (Strings.isNullOrEmpty(context.config.otherConfigs.get("publicMskBrokerList"))) {
        return
    }

    def ak = context.config.awsMskAccessKey
    def sk = context.config.awsMskSecretKey
    def region = context.config.awsMskRegion
    String bootstrapBrokers = context.config.otherConfigs.get("mskBrokerList")
    String role = context.config.otherConfigs.get("awsMskRole")
    String sslCaLocation = context.config.otherConfigs.get("MskSslCaLocation")

    logger.info("Using topic: ${topicName}")
    logger.info("Bootstrap brokers: ${bootstrapBrokers}")
    logger.info("AWS region: ${region}")

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            name VARCHAR(50)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE ROUTINE LOAD ${jobName} ON ${tableName}
        COLUMNS TERMINATED BY ",",
        COLUMNS(id, name)
        FROM KAFKA (
            "kafka_broker_list" = "${bootstrapBrokers}",
            "kafka_topic" = "${topicName}",
            "aws.region" = "${region}",
            "aws.access_key" = "${ak}",
            "aws.secret_key" = "${sk}",
            "property.kafka_default_offsets" = "OFFSET_BEGINNING",
            "property.security.protocol" = "SASL_SSL",
            "property.sasl.mechanism" = "OAUTHBEARER",
            "property.ssl.ca.location" = "${sslCaLocation}"
        )
    """
    logger.info("Created routine load job: ${jobName}")

    def maxRetries = 60
    def retryCount = 0
    def jobState = ""

    while (retryCount < maxRetries) {
        Thread.sleep(5000)

        def jobStatus = sql """ SHOW ROUTINE LOAD FOR ${jobName} """
        if (jobStatus.size() > 0) {
            jobState = jobStatus[0][8]
            logger.info("Job state: ${jobState}, retry: ${retryCount}")

            if (jobState == "RUNNING") {
                def result = sql """ SELECT COUNT(*) FROM ${tableName} """
                if (result[0][0] > 0) {
                    logger.info("Data loaded successfully: ${result[0][0]} rows")
                    break
                }
            } else if (jobState == "PAUSED" || jobState == "CANCELLED") {
                logger.error("Job failed with state: ${jobState}")
                logger.error("Job status: ${jobStatus[0]}")
                break
            }
        }
        retryCount++
    }

    def result = sql """ SELECT COUNT(*) FROM ${tableName} """
    logger.info("Final row count: ${result[0][0]}")

    assertTrue(result[0][0] > 0, "No data loaded from MSK. Job state: ${jobState}")

    def sampleData = sql """ SELECT * FROM ${tableName} LIMIT 5 """
    logger.info("Sample data: ${sampleData}")

    sql """ STOP ROUTINE LOAD FOR ${jobName} """
}
