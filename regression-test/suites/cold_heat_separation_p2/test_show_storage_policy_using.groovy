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

suite("test_show_storage_policy_using") {

    def resource_name = "test_remote_s3_resource"
    def policy_name= "test_storage_policy"
    def policy_name_2= "test_storage_policy_2"
    def policy_name_not_exist = "policy_name_not_exist"

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
        CREATE STORAGE POLICY IF NOT EXISTS ${policy_name_2}
        PROPERTIES(
            "storage_resource" = "${resource_name}",
            "cooldown_ttl" = "600"
        )
    """

    sql """ DROP TABLE IF EXISTS table_with_storage_policy_1 """
    sql """
        CREATE TABLE IF NOT EXISTS table_with_storage_policy_1
        (
            k1 BIGINT,
            v1 VARCHAR(48)
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH (k1) BUCKETS 3
        PROPERTIES(
            "storage_policy" = "${policy_name}",
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ DROP TABLE IF EXISTS table_no_storage_policy_1 """
    sql """
        CREATE TABLE IF NOT EXISTS table_no_storage_policy_1
        (
            k1 BIGINT,
            v1 VARCHAR(32)
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH (k1) BUCKETS 3
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ DROP TABLE IF EXISTS partition_with_multiple_storage_policy """
    sql """
        CREATE TABLE `partition_with_multiple_storage_policy` (
        `id` int(11) NOT NULL COMMENT '',
        `name` int(11) NOT NULL COMMENT '',
        `event_date` date NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT "OLAP"
        PARTITION BY RANGE(`event_date`)
        (
                PARTITION p201701 VALUES [('0000-01-01'), ('2017-02-01')) ("storage_policy" = "${policy_name}"),
                PARTITION `p201702` VALUES LESS THAN ("2017-03-01")("storage_policy" = "${policy_name_2}"),
                PARTITION `p2018` VALUES [("2018-01-01"), ("2019-01-01"))
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 8
        PROPERTIES(
                "replication_allocation" = "tag.location.default: 1"
         );
    """

    sql """ DROP TABLE IF EXISTS table_with_storage_policy_2"""
    sql """
        CREATE TABLE IF NOT EXISTS table_with_storage_policy_2
        (
            k1 BIGINT,
            v1 VARCHAR(48)
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH (k1) BUCKETS 3
        PROPERTIES(
            "storage_policy" = "${policy_name_2}",
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    show_result = sql """
        show storage policy using for ${policy_name}
    """
    assertEquals(show_result.size(), 2)
    assertTrue(show_result[0][2].equals("table_with_storage_policy_1") || show_result[1][2].equals("table_with_storage_policy_1"))
    assertTrue(show_result[0][2].equals("partition_with_multiple_storage_policy") || show_result[1][2].equals("partition_with_multiple_storage_policy"))
    if (show_result[0][2].equals("partition_with_multiple_storage_policy")) {
        show_result[0][3].equals("p201701")
    }
    if (show_result[1][2].equals("partition_with_multiple_storage_policy")) {
        show_result[1][3].equals("p201701")
    }

    show_result = sql """
        show storage policy using for ${policy_name_2}
    """
    assertTrue(show_result[0][2].equals("table_with_storage_policy_2") || show_result[1][2].equals("table_with_storage_policy_2"))
    assertTrue(show_result[0][2].equals("partition_with_multiple_storage_policy") || show_result[1][2].equals("partition_with_multiple_storage_policy"))
    if (show_result[0][2].equals("partition_with_multiple_storage_policy")) {
        show_result[0][3].equals("p201702")
    }
    if (show_result[1][2].equals("partition_with_multiple_storage_policy")) {
        show_result[1][3].equals("p201702")
    }


    show_result = sql """
        show storage policy using for ${policy_name_not_exist}
    """
    assertTrue(show_result.size() == 0)

    show_result = sql """
        show storage policy using
    """
    assertTrue(show_result.size() >= 4)

    // test cancel a partition's storage policy
    sql """
        ALTER TABLE partition_with_multiple_storage_policy MODIFY PARTITION (`p201701`) SET ("storage_policy"="")
    """
    show_result = sql """
        show storage policy using for ${policy_name}
    """
    assertEquals(show_result.size(), 1)
    assertTrue(show_result[0][2].equals("table_with_storage_policy_1"))

    sql """
        ALTER TABLE partition_with_multiple_storage_policy MODIFY PARTITION (`p201701`) SET ("storage_policy"="${policy_name}")
    """
    show_result = sql """
        show storage policy using for ${policy_name}
    """
    assertEquals(show_result.size(), 2)

    sql """
        ALTER TABLE partition_with_multiple_storage_policy MODIFY PARTITION (`p201701`) SET ("replication_num"="1")
    """
    show_result = sql """
        show storage policy using for ${policy_name}
    """
    assertEquals(show_result.size(), 2)

    // cleanup
    sql """ DROP TABLE IF EXISTS table_with_storage_policy_1 """
    sql """ DROP TABLE IF EXISTS table_no_storage_policy_1 """
    sql """ DROP TABLE IF EXISTS partition_with_multiple_storage_policy """
    sql """ DROP TABLE IF EXISTS table_with_storage_policy_2"""
}