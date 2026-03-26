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

suite("test_kinesis_black_list") {
    String awsRegion = context.config.otherConfigs.get("awsRegion")
    String awsAccessKey = context.config.otherConfigs.get("awsAccessKey")
    String awsSecretKey = context.config.otherConfigs.get("awsSecretKey")

    def tableName = "test_kinesis_blacklist"

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

    try {
        sql """
            CREATE ROUTINE LOAD testKinesisBlacklist ON ${tableName}
            PROPERTIES ("format" = "json")
            FROM KINESIS (
                "aws.region" = "${awsRegion}",
                "aws.access_key" = "${awsAccessKey}",
                "aws.secret_key" = "${awsSecretKey}",
                "kinesis_stream" = "invalid_stream_name_for_blacklist_test",
                "property.kinesis_default_pos" = "TRIM_HORIZON"
            )
        """
        fail("Should fail with invalid stream")
    } catch (Exception e) {
        logger.info("Expected error: ${e.message}")
        assertTrue(e.message.contains("ResourceNotFoundException") || e.message.contains("not found"))
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
}
