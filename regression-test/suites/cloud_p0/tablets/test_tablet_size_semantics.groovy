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
    def parseDataSizeToBytes = { Object dataSize ->
        String normalized = dataSize == null ? "" : dataSize.toString().replaceAll("\\s+", "")
        if (normalized.isEmpty()) {
            return 0L
        }
        if (!(normalized ==~ /.*[A-Za-z]$/)) {
            normalized = normalized + "B"
        }
        def parsedRows = sql_return_maparray("SELECT parse_data_size('${normalized}') AS bytes")
        assertTrue(parsedRows.size() == 1)
        return parsedRows[0]["bytes"].toString().toLong()
    }

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
    long tabletId = showTabletRow["TabletId"].toString().toLong()
    long showLocalSize = -1L
    long showRemoteSize = -1L
    long backendLocalSize = -1L
    long backendRemoteSize = -1L
    boolean sizeAligned = false

    // SHOW TABLETS size stats may lag behind backend_tablets for tens of seconds.
    for (int i = 0; i < 120; i++) {
        def latestShowTabletRows = sql_return_maparray("show tablets from ${tableName}")
        assertTrue(latestShowTabletRows.size() == 1)
        def latestShowTabletRow = latestShowTabletRows[0]
        showLocalSize = latestShowTabletRow["LocalDataSize"].toString().toLong()
        showRemoteSize = latestShowTabletRow["RemoteDataSize"].toString().toLong()
        long backendIdForCheck = latestShowTabletRow["BackendId"].toString().toLong()

        def backendTabletRows = sql_return_maparray("""
            SELECT
                tablet_local_size AS be_local_size,
                tablet_remote_size AS be_remote_size
            FROM information_schema.backend_tablets
            WHERE tablet_id = ${tabletId}
              AND be_id = ${backendIdForCheck}
            ORDER BY update_time DESC
            LIMIT 1
        """)
        if (backendTabletRows.size() == 1) {
            def backendTabletRow = backendTabletRows[0]
            backendLocalSize = backendTabletRow["be_local_size"].toString().toLong()
            backendRemoteSize = backendTabletRow["be_remote_size"].toString().toLong()
            if (showLocalSize == backendLocalSize
                    && showRemoteSize == backendRemoteSize
                    && backendRemoteSize > 0) {
                sizeAligned = true
                break
            }
        }

        logger.info("wait tablet size aligned, show local/remote: {}/{}, backend local/remote: {}/{}",
                showLocalSize, showRemoteSize, backendLocalSize, backendRemoteSize)
        sleep(1000)
    }
    assertTrue(sizeAligned,
            "tablet size not aligned within 120s, show local/remote=${showLocalSize}/${showRemoteSize}, " +
                    "backend local/remote=${backendLocalSize}/${backendRemoteSize}")
    logger.info("final tablet size aligned, show local/remote: {}/{}, backend local/remote: {}/{}",
                showLocalSize, showRemoteSize, backendLocalSize, backendRemoteSize)
    def partitionRows = sql_return_maparray("""
        SELECT
            local_data_size AS part_local_size,
            remote_data_size AS part_remote_size
        FROM information_schema.partitions
        WHERE table_schema = '${dbName}'
          AND table_name = '${tableName}'
        ORDER BY partition_name
        LIMIT 1
    """)
    assertTrue(partitionRows.size() == 1)
    def partitionRow = partitionRows[0]
    long partitionLocalSize = parseDataSizeToBytes(partitionRow["part_local_size"])
    long partitionRemoteSize = parseDataSizeToBytes(partitionRow["part_remote_size"])

    assertEquals(0L, partitionLocalSize)
    assertTrue(partitionRemoteSize > 0)
}
