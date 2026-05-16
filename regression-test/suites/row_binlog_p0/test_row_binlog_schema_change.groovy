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

suite("test_row_binlog_schema_change", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    def tsoFeatureConfig = sql "SHOW FRONTEND CONFIG like '%experimental_enable_tso_feature%';"
    def tsoPersistConfig = sql "SHOW FRONTEND CONFIG like '%enable_tso_persist_journal%';"
    try {
        sql "ADMIN SET FRONTEND CONFIG ('enable_tso_persist_journal' = 'true')"
        sql "ADMIN SET FRONTEND CONFIG ('experimental_enable_tso_feature' = 'true')"
        sleep(1000)

    sql "DROP TABLE IF EXISTS test_mow_schema_change_with_binlog FORCE"

    sql """
        CREATE TABLE test_mow_schema_change_with_binlog (
            k1 INT,
            k2 INT,
            k3 INT,
            v1 INT,
            v2 VARCHAR(2)
        )
        UNIQUE KEY(k1, k2, k3)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "enable_tso" = "true"
        )
    """

    sql """
        INSERT INTO test_mow_schema_change_with_binlog VALUES
            (1, 1, 1, 10, '10'),
            (2, 2, 2, 20, '20')
    """

    sql "ALTER TABLE test_mow_schema_change_with_binlog ADD COLUMN v3 INT"
    sql "UPDATE test_mow_schema_change_with_binlog SET v2 = '11', v3 = 111 WHERE k1 = 1 AND k2 = 1 AND k3 = 1"
    sql "sync"
    qt_row_binlog_schema_change_add_column """
        SELECT __DORIS_BINLOG_OP__ AS op,
               k1,
               k2,
               k3,
               v1,
               v2,
               v3,
               __BEFORE__v1__,
               __BEFORE__v2__,
               __BEFORE__v3__
        FROM binlog("table" = "test_mow_schema_change_with_binlog")
        ORDER BY __DORIS_BINLOG_LSN__
    """

    sql "ALTER TABLE test_mow_schema_change_with_binlog DROP COLUMN v1"
    sql "INSERT INTO test_mow_schema_change_with_binlog(k1, k2, k3, v2, v3) VALUES (3, 3, 3, '30', 300)"
    sql "sync"
    qt_row_binlog_schema_change_drop_column """
        SELECT __DORIS_BINLOG_OP__ AS op,
               k1,
               k2,
               k3,
               v2,
               v3,
               __BEFORE__v2__,
               __BEFORE__v3__
        FROM binlog("table" = "test_mow_schema_change_with_binlog")
        ORDER BY __DORIS_BINLOG_LSN__
    """

    sql "ALTER TABLE test_mow_schema_change_with_binlog ADD COLUMN v1 STRING"
    sql "INSERT INTO test_mow_schema_change_with_binlog(k1, k2, k3, v1, v2, v3) VALUES (4, 4, 4, '40', '40', 400)"
    sql "UPDATE test_mow_schema_change_with_binlog SET v1 = '33' WHERE k1 = 3 AND k2 = 3 AND k3 = 3"
    sql "sync"
    qt_row_binlog_schema_change_add_back_column """
        SELECT __DORIS_BINLOG_OP__ AS op,
               k1,
               k2,
               k3,
               v1,
               v2,
               v3,
               __BEFORE__v1__,
               __BEFORE__v2__,
               __BEFORE__v3__
        FROM binlog("table" = "test_mow_schema_change_with_binlog")
        ORDER BY __DORIS_BINLOG_LSN__
    """

    test {
        sql "ALTER TABLE test_mow_schema_change_with_binlog MODIFY COLUMN v2 VARCHAR(10)"
        exception "Not allowed to perform current operation on Table With binlog<row>"
    }
    } finally {
        sql "ADMIN SET FRONTEND CONFIG ('experimental_enable_tso_feature' = 'false')"
        sql "ADMIN SET FRONTEND CONFIG ('enable_tso_persist_journal' = '${tsoPersistConfig[0][1]}')"
        sql "ADMIN SET FRONTEND CONFIG ('experimental_enable_tso_feature' = '${tsoFeatureConfig[0][1]}')"
    }
}
