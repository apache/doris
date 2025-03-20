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

suite("test_partition_storage_policy") {
    def resource_name = "test_partition_storage_policy_resource"
    def policy_name= "test_partition_storage_policy"
    def tableNameSynced = "test_partition_storage_policy_tbl_synced"
    def tableNameNotSynced = "test_partition_storage_policy_tbl_not_synced"

    sql "DROP TABLE IF EXISTS ${tableNameSynced}"
    sql "DROP TABLE IF EXISTS ${tableNameNotSynced}"

    def check_storage_policy_exist = { name->
        def polices = sql"""
        show storage policy;
        """
        for (p in polices) {
            if (name == p[0]) {
                return true;
            }
        }
        return false;
    }

    if (check_storage_policy_exist(policy_name)) {
        sql """
            DROP STORAGE POLICY ${policy_name}
        """
    }

    def has_resouce = sql """
        SHOW RESOURCES WHERE NAME = "${resource_name}";
    """

    if (has_resouce.size() > 0) {
        sql """
            DROP RESOURCE ${resource_name}
        """
    }

    sql """
        CREATE RESOURCE IF NOT EXISTS "${resource_name}"
        PROPERTIES(
            "type"="s3",
            "AWS_ENDPOINT" = "${getS3Endpoint()}",
            "AWS_REGION" = "${getS3Region()}",
            "AWS_ROOT_PATH" = "regression/cooldown",
            "AWS_ACCESS_KEY" = "${getS3AK()}",
            "AWS_SECRET_KEY" = "${getS3SK()}",
            "AWS_MAX_CONNECTIONS" = "50",
            "AWS_REQUEST_TIMEOUT_MS" = "3000",
            "AWS_CONNECTION_TIMEOUT_MS" = "1000",
            "AWS_BUCKET" = "${getS3BucketName()}",
            "s3_validity_check" = "true"
        );
    """

    sql """
        CREATE STORAGE POLICY IF NOT EXISTS ${policy_name}
        PROPERTIES(
            "storage_resource" = "${resource_name}",
            "cooldown_ttl" = "300"
        )
    """

    sql """
        CREATE TABLE IF NOT EXISTS ${tableNameSynced}
        (
            k1 DATETIMEV2(3),
            k2 INT,
            V1 VARCHAR(2048)
        ) PARTITION BY RANGE (k1) (
            PARTITION p1 VALUES LESS THAN ("2022-01-01 00:00:00.111") ("storage_policy" = ${policy_name} ,"replication_num"="1"),
            PARTITION p2 VALUES LESS THAN ("2022-02-01 00:00:00.111") ("storage_policy" = ${policy_name} ,"replication_num"="1")
        ) DISTRIBUTED BY HASH(k2) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "true"
        );
    """

    sql """
        CREATE TABLE IF NOT EXISTS ${tableNameNotSynced}
        (
            k1 DATETIMEV2(3),
            k2 INT,
            V1 VARCHAR(2048)
        ) PARTITION BY RANGE (k1) (
            PARTITION p1 VALUES LESS THAN ("2022-01-01 00:00:00.111") ("storage_policy" = ${policy_name} ,"replication_num"="1"),
            PARTITION p2 VALUES LESS THAN ("2022-02-01 00:00:00.111") ("storage_policy" = ${policy_name} ,"replication_num"="1")
        ) DISTRIBUTED BY HASH(k2) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    def res = sql "SHOW CREATE TABLE ${tableNameSynced}"

    logger.info(res[0][1])
    assertTrue(!res[0][1].contains("\"${policy_name}\""))
    assertTrue(!res[0][1].contains("\"${policy_name}\""))

    res = sql "SHOW CREATE TABLE ${tableNameNotSynced}"

    logger.info(res[0][1])
    assertTrue(res[0][1].contains("\"${policy_name}\""))
    assertTrue(res[0][1].contains("\"${policy_name}\""))
}
