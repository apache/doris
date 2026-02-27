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

suite("test_tablet_size_semantics") {
    if (!isCloudMode()) {
        return
    }

    def dbName = "test_tablet_size_semantics_db"
    def tableName = "test_tablet_size_semantics_tbl"

    sql "drop database if exists ${dbName}"
    sql "create database ${dbName}"
    sql "use ${dbName}"

    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
            `k1` INT NOT NULL,
            `v1` STRING NULL
        )
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """
        INSERT INTO `${tableName}` VALUES
        (1, 'a'),
        (2, 'b'),
        (3, 'c'),
        (4, 'd'),
        (5, 'e');
    """
    sql "sync"

    def showTabletRows = sql_return_maparray("show tablets from ${tableName}")
    assertTrue(showTabletRows.size() == 1)
    def showTabletRow = showTabletRows[0]
    long tabletId = (showTabletRow["TabletId"] as Number).longValue()
    long beId = (showTabletRow["BackendId"] as Number).longValue()
    long showLocalSize = (showTabletRow["LocalDataSize"] as Number).longValue()
    long showRemoteSize = (showTabletRow["RemoteDataSize"] as Number).longValue()

    def backendTabletRows = sql_return_maparray("""
        SELECT
            tablet_local_size AS be_local_size,
            tablet_remote_size AS be_remote_size
        FROM information_schema.backend_tablets
        WHERE tablet_id = ${tabletId}
          AND be_id = ${beId}
        ORDER BY update_time DESC
        LIMIT 1
    """)
    assertTrue(backendTabletRows.size() == 1)
    def backendTabletRow = backendTabletRows[0]
    long backendLocalSize = (backendTabletRow["be_local_size"] as Number).longValue()
    long backendRemoteSize = (backendTabletRow["be_remote_size"] as Number).longValue()

    assertEquals(showLocalSize, backendLocalSize)
    assertEquals(showRemoteSize, backendRemoteSize)
    assertTrue(backendLocalSize > 0 || backendRemoteSize > 0)

    def partitionRows = sql_return_maparray("""
        SELECT
            parse_data_size(local_data_size) AS part_local_size,
            parse_data_size(remote_data_size) AS part_remote_size
        FROM information_schema.partitions
        WHERE table_schema = '${dbName}'
          AND table_name = '${tableName}'
        ORDER BY partition_name
        LIMIT 1
    """)
    assertTrue(partitionRows.size() == 1)
    def partitionRow = partitionRows[0]
    long partitionLocalSize = (partitionRow["part_local_size"] as Number).longValue()
    long partitionRemoteSize = (partitionRow["part_remote_size"] as Number).longValue()

    if (backendLocalSize == 0) {
        assertEquals(0L, partitionLocalSize)
    } else {
        assertTrue(partitionLocalSize > 0)
    }
    if (backendRemoteSize == 0) {
        assertEquals(0L, partitionRemoteSize)
    } else {
        assertTrue(partitionRemoteSize > 0)
    }
}

