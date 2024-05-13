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

suite("test_backup_restore_with_two_phase_fetch_opt", "backup_restore") {
    String suiteName = "test_backup_restore_with_two_phase_fetch_opt"
    String repoName = "${suiteName}_repo"
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String snapshotName = "${suiteName}_snapshot"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
    CREATE TABLE ${dbName}.${tableName} (
        `distinct_id` varchar(255) NULL,
        `dt` date NULL,
        `device_id` text NULL,
        `ep_user_id` bigint(20) NULL,
        `ep_corp_name` text NULL,
        `ep_product_sid` bigint(20) NULL,
        `supply_id` bigint(20) NULL,
        `ep_supply_type` text NULL,
        `ep_split_code` text NULL,
        `ep_symbol_name` text NULL,
        `ep_manufactor_short_name` text NULL,
        `event_type` text NULL,
        `platform_type` text NULL,
        `is_first` text NULL,
        `os` text NULL,
        `area_address` text NULL,
        `ts` text NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`distinct_id`, `dt`)
    COMMENT 'OLAP'
    PARTITION BY RANGE (dt)(
     FROM ("2023-12-01") TO ("2023-12-25") INTERVAL 1 DAY
    )
    DISTRIBUTED BY HASH(`distinct_id`) BUCKETS AUTO
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "dynamic_partition.enable" = "true",
        "dynamic_partition.time_unit" = "DAY",
        "dynamic_partition.end" = "3",
        "dynamic_partition.prefix" = "p"
    );
        """

    sql """
    INSERT INTO ${dbName}.${tableName} VALUES
        ("1231", "2023-12-01", "device_id", 123123, "corp_name", 123321, 1231, "supply_type", "split_code", "symbol_name", "short_name", "event_type", "platform_type", "is_first", "os", "area_address", "ts"),
        ("1232", "2023-12-01", "device_id", 123123, "corp_name", 123321, 1231, "supply_type", "split_code", "symbol_name", "short_name", "event_type", "platform_type", "is_first", "os", "area_address", "ts"),
        ("1233", "2023-12-02", "device_id", 123123, "corp_name", 123321, 1231, "supply_type", "split_code", "symbol_name", "short_name", "event_type", "platform_type", "is_first", "os", "area_address", "ts"),
        ("1234", "2023-12-03", "device_id", 123123, "corp_name", 123321, 1231, "supply_type", "split_code", "symbol_name", "short_name", "event_type", "platform_type", "is_first", "os", "area_address", "ts"),
        ("1235", "2023-12-04", "device_id", 123123, "corp_name", 123321, 1231, "supply_type", "split_code", "symbol_name", "short_name", "event_type", "platform_type", "is_first", "os", "area_address", "ts"),
        ("1236", "2023-12-04", "device_id", 123123, "corp_name", 123321, 1231, "supply_type", "split_code", "symbol_name", "short_name", "event_type", "platform_type", "is_first", "os", "area_address", "ts");
    """
    def result = sql "SELECT * FROM ${dbName}.${tableName} ORDER BY dt DESC LIMIT 5"
    assertEquals(result.size(), 5)

    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    sql "DROP TABLE ${dbName}.${tableName} FORCE"

    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_dynamic_partition_enable"="true",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    result = sql "SELECT * FROM ${dbName}.${tableName} ORDER BY dt DESC LIMIT 5"
    assertEquals(result.size(), 5)

    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

