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

import org.junit.Assert;

suite("test_mtmv_property","mtmv") {
    if (isCloudMode()) {
        return
    }
    String dbName = context.config.getDbNameByFile(context.file)
    String suiteName = "test_mtmv_property"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName}
        (
            k2 INT,
            k3 varchar(32)
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        DISTRIBUTED BY hash(k2) BUCKETS 2
        PROPERTIES (
        'replication_num' = '1',
        'store_row_column' = 'true',
        'skip_write_index_on_load' = 'false',
        "binlog.enable" = "false",
        "binlog.ttl_seconds" = "86400",
        "binlog.max_bytes" = "9223372036854775807",
        "binlog.max_history_nums" = "9223372036854775807",
        "enable_duplicate_without_keys_by_default" = "true"
        )
        AS
        SELECT * from ${tableName};
        """

    def showCreateTableResult = sql """show create materialized view ${mvName}"""
    logger.info("showCreateTableResult: " + showCreateTableResult.toString())
    // Cannot compare the number of replicas to 1, as the pipeline may force the number of replicas to be set
    assertTrue(showCreateTableResult.toString().contains('tag.location.default:'))
    assertTrue(showCreateTableResult.toString().contains('"min_load_replica_num" = "-1"'))
    assertTrue(showCreateTableResult.toString().contains('"storage_medium" = "hdd"'))
    assertTrue(showCreateTableResult.toString().contains('"store_row_column" = "true"'))
    assertTrue(showCreateTableResult.toString().contains('"enable_duplicate_without_keys_by_default" = "true"'))

    sql """
        insert into ${tableName} values (2,1),(2,2);
        """
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO;
        """
    def jobName = getJobName(dbName, mvName)
    waitingMTMVTaskFinished(jobName)
    sql """select * from ${mvName}"""

    // replication_num
    sql """
        ALTER TABLE ${mvName} set ("replication_num" = "1");
        """
    showCreateTableResult = sql """show create materialized view ${mvName}"""
    logger.info("showCreateTableResult: " + showCreateTableResult.toString())
    assertTrue(showCreateTableResult.toString().contains('tag.location.default: 1'))

    // replication_allocation
    sql """
        ALTER TABLE ${mvName} set ("replication_allocation" = "tag.location.default: 1");
        """
    showCreateTableResult = sql """show create materialized view ${mvName}"""
    logger.info("showCreateTableResult: " + showCreateTableResult.toString())
    assertTrue(showCreateTableResult.toString().contains('tag.location.default: 1'))

    // min_load_replica_num
    sql """
        ALTER TABLE ${mvName} set ("min_load_replica_num" = "1");
        """
    showCreateTableResult = sql """show create materialized view ${mvName}"""
    logger.info("showCreateTableResult: " + showCreateTableResult.toString())
    assertTrue(showCreateTableResult.toString().contains('"min_load_replica_num" = "1"'))

    // type
    sql """
        insert into ${tableName} values (3,1),(3,2);
        """
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO;
        """
    jobName = getJobName(dbName, mvName)
    waitingMTMVTaskFinished(jobName)
    def tablets_res = sql """show tablets from ${mvName}"""
    assertTrue(tablets_res.size() > 0)
    sql """
        admin check tablet (${tablets_res[0][0].toString()}) properties("type"="consistency");
        """

    // storage_policy
    def resource_name = "test_mtmv_remote_s3_resource"
    def policy_name= "test_mtmv_storage_policy"
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

    sql """ALTER TABLE ${mvName} SET ("storage_policy" = "${policy_name}");"""
    showCreateTableResult = sql """show create materialized view ${mvName}"""
    logger.info("showCreateTableResult: " + showCreateTableResult.toString())
    assertTrue(showCreateTableResult.toString().contains('"storage_policy" = "test_mtmv_storage_policy"'))

    // store_row_column
    try {
        sql """ALTER TABLE ${mvName} SET ("store_row_column" = "false");"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Unknown table property: [store_row_column]"))
    }

    // is_being_synced
    sql """
        ALTER TABLE ${mvName} set ("is_being_synced" = "true");
        """
    showCreateTableResult = sql """show create materialized view ${mvName}"""
    logger.info("showCreateTableResult: " + showCreateTableResult.toString())
    assertTrue(showCreateTableResult.toString().contains('"is_being_synced" = "true"'))

    // skip_write_index_on_load
    def desc_res = sql """desc ${mvName}"""
    assertTrue(desc_res.size() == 2)
    assertTrue(desc_res[0][3] == "false")
    assertTrue(desc_res[1][3] == "false")

    // enable_unique_key_merge_on_write
    try {
        sql """
        CREATE MATERIALIZED VIEW mv_unique_key_merge_on_write
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        DISTRIBUTED BY hash(k2) BUCKETS 2
        PROPERTIES (
        'replication_num' = '1',
        "enable_unique_key_merge_on_write" = "true"
        )
        AS
        SELECT * from ${tableName};
        """
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Unknown properties"))
    }

    // group_commit_interval_ms
    sql """ALTER TABLE ${mvName} SET ("group_commit_interval_ms" = "2000");"""
    showCreateTableResult = sql """show create materialized view ${mvName}"""
    logger.info("showCreateTableResult: " + showCreateTableResult.toString())
    assertTrue(showCreateTableResult.toString().contains('"group_commit_interval_ms" = "2000"'))

    // group_commit_data_bytes
    sql """ALTER TABLE ${mvName} SET ("group_commit_data_bytes" = "234217728");"""
    showCreateTableResult = sql """show create materialized view ${mvName}"""
    logger.info("showCreateTableResult: " + showCreateTableResult.toString())
    assertTrue(showCreateTableResult.toString().contains('"group_commit_data_bytes" = "234217728"'))

    // check mv can update normally
    sql """
        insert into ${tableName} values (1,1),(1,2);
        """
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO;
        """
    jobName = getJobName(dbName, mvName)
    waitingMTMVTaskFinished(jobName)
    sql """select * from ${mvName}"""

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
