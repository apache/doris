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

import groovy.json.JsonSlurper

suite("test_load_to_single_tablet", "p0") {
    sql "show tables"

    def tableName = "test_load_to_single_tablet"

    // test unpartitioned table
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
          `k1` date NULL,
          `k2` text NULL,
          `k3` char(50) NULL,
          `k4` varchar(200) NULL,
          `k5` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY RANDOM BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    
    // load first time
    streamLoad {
        table "${tableName}"

        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'load_to_single_tablet', 'true'

        file 'test_load_to_single_tablet.json'
        time 20000 // limit inflight 10s
    }

    sql "sync"
    def totalCount = sql "select count() from ${tableName}"
    assertEquals(10, totalCount[0][0])
    String[][] res = sql "show tablets from ${tableName}"
    res = deduplicate_tablets(res)

    def tablets = []
    for (int i = 0; i < res.size(); i++) {
        tablets.add(res[i][0])
    }

    def beginIdx = -1
    def rowCounts = []

    for (int i = tablets.size() - 1; i >= 0; i--) {
        def countResult = sql "select count() from ${tableName} tablet(${tablets[i]})"
        rowCounts[i] = countResult[0][0]
        log.info("tablet: ${tablets[i]}, rowCount: ${rowCounts[i]}")
        if (rowCounts[i] > 0 && (beginIdx == -1 || beginIdx == i + 1)) {
            beginIdx = i;
        }
    }

    assertEquals(10, rowCounts[beginIdx])
    for (int i = 1; i < tablets.size(); i++) {
        assertEquals(0, rowCounts[(beginIdx + i) % tablets.size()])
    }

    // load second time
    streamLoad {
        table "${tableName}"

        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'load_to_single_tablet', 'true'

        file 'test_load_to_single_tablet.json'
        time 20000 // limit inflight 10s
    }
    sql "sync"
    totalCount = sql "select count() from ${tableName}"
    assertEquals(20, totalCount[0][0])

    for (int i = 0; i < tablets.size(); i++) {
        def countResult = sql "select count() from ${tableName} tablet(${tablets[i]})"
        rowCounts[i] = countResult[0][0]
        log.info("tablet: ${tablets[i]}, rowCount: ${rowCounts[i]}")
    }

    assertEquals(10, rowCounts[beginIdx])
    assertEquals(10, rowCounts[(beginIdx + 1) % tablets.size()])
    for (int i = 2; i < tablets.size(); i++) {
        assertEquals(0, rowCounts[(beginIdx + i) % tablets.size()])
    }

    // load third time
    streamLoad {
        table "${tableName}"

        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'load_to_single_tablet', 'true'

        file 'test_load_to_single_tablet.json'
        time 20000 // limit inflight 10s
    }
    sql "sync"
    totalCount = sql "select count() from ${tableName}"
    assertEquals(30, totalCount[0][0])

    for (int i = 0; i < tablets.size(); i++) {
        def countResult = sql "select count() from ${tableName} tablet(${tablets[i]})"
        rowCounts[i] = countResult[0][0]
        log.info("tablet: ${tablets[i]}, rowCount: ${rowCounts[i]}")
    }

    assertEquals(10, rowCounts[beginIdx])
    assertEquals(10, rowCounts[(beginIdx + 1) % tablets.size()])
    assertEquals(10, rowCounts[(beginIdx + 2) % tablets.size()])

    for (int i = 3; i < tablets.size(); i++) {
        assertEquals(0, rowCounts[(beginIdx + i) % tablets.size()])
    }

    // test partitioned table
    tableName = "test_load_to_single_tablet_partitioned"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
          `k1` date NULL,
          `k2` text NULL,
          `k3` char(50) NULL,
          `k4` varchar(200) NULL,
          `k5` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`k1`)
        (PARTITION p20231011 VALUES [('2023-10-11'), ('2023-10-12')),
        PARTITION p20231012 VALUES [('2023-10-12'), ('2023-10-13')),
        PARTITION p20231013 VALUES [('2023-10-13'), ('2023-10-14')),
        PARTITION p20231014 VALUES [('2023-10-14'), ('2023-10-15')))
        DISTRIBUTED BY RANDOM BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    // load first time
    streamLoad {
        table "${tableName}"

        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'load_to_single_tablet', 'true'

        file 'test_load_to_single_tablet.json'
        time 20000 // limit inflight 10s
    }

    sql "sync"

    totalCount = sql "select count() from ${tableName}"
    assertEquals(10, totalCount[0][0])

    def partitionTablets = []
    def partitionRowCounts = []
    def partitionBeginIdx = []
    res = sql "show tablets from ${tableName} partitions(p20231011, p20231012)"
    res = deduplicate_tablets(res)
    for (int i = 0; i < res.size(); i++) {
        if (i % 10 == 0) {
            partitionTablets[i/10] = []
            partitionRowCounts[i/10] = []
        }
        partitionTablets[i/10][i%10] = res[i][0]
    }

    for (int i = 0; i < partitionTablets.size(); i++) {
        for (int j = partitionTablets[i].size() - 1; j >= 0; j--) {
            def countResult = sql "select count() from ${tableName} tablet(${partitionTablets[i][j]})"
            partitionRowCounts[i][j] = countResult[0][0]
            log.info("tablet: ${partitionTablets[i][j]}, rowCount: ${partitionRowCounts[i][j]}")
            if (partitionRowCounts[i][j] > 0 &&
                (partitionBeginIdx[i] == null || partitionBeginIdx[i] == j + 1)) {
                partitionBeginIdx[i] = j
            }
        }
    }

    assertEquals(5, partitionRowCounts[0][partitionBeginIdx[0]])
    for (int i = 1; i < partitionTablets[0].size(); i++) {
        assertEquals(0, partitionRowCounts[0][(partitionBeginIdx[0] + i) % partitionTablets[0].size()])
    }
    assertEquals(5, partitionRowCounts[1][partitionBeginIdx[1]])
    for (int i = 1; i < partitionTablets[1].size(); i++) {
        assertEquals(0, partitionRowCounts[1][(partitionBeginIdx[1] + i) % partitionTablets[1].size()])
    }

    // load second time
    streamLoad {
        table "${tableName}"

        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'load_to_single_tablet', 'true'

        file 'test_load_to_single_tablet.json'
        time 20000 // limit inflight 10s
    }
    sql "sync"
    totalCount = sql "select count() from ${tableName}"
    assertEquals(20, totalCount[0][0])

    for (int i = 0; i < partitionTablets.size(); i++) {
        for (int j = 0; j < partitionTablets[i].size(); j++) {
            def countResult = sql "select count() from ${tableName} tablet(${partitionTablets[i][j]})"
            partitionRowCounts[i][j] = countResult[0][0]
            log.info("tablet: ${partitionTablets[i][j]}, rowCount: ${partitionRowCounts[i][j]}")
        }
    }

    assertEquals(5, partitionRowCounts[0][partitionBeginIdx[0]])
    assertEquals(5, partitionRowCounts[0][(partitionBeginIdx[0] + 1) % partitionTablets[0].size()])
    for (int i = 2; i < partitionTablets[0].size(); i++) {
        assertEquals(0, partitionRowCounts[0][(partitionBeginIdx[0] + i) % partitionTablets[0].size()])
    }

    assertEquals(5, partitionRowCounts[1][partitionBeginIdx[1]])
    assertEquals(5, partitionRowCounts[1][(partitionBeginIdx[1] + 1) % partitionTablets[1].size()])
    for (int i = 2; i < partitionTablets[1].size(); i++) {
        assertEquals(0, partitionRowCounts[1][(partitionBeginIdx[1] + i) % partitionTablets[1].size()])
    }

    // load third time
    streamLoad {
        table "${tableName}"

        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'load_to_single_tablet', 'true'

        file 'test_load_to_single_tablet.json'
        time 20000 // limit inflight 10s
    }
    sql "sync"
    totalCount = sql "select count() from ${tableName}"
    assertEquals(30, totalCount[0][0])

    for (int i = 0; i < partitionTablets.size(); i++) {
        for (int j = 0; j < partitionTablets[i].size(); j++) {
            def countResult = sql "select count() from ${tableName} tablet(${partitionTablets[i][j]})"
            partitionRowCounts[i][j] = countResult[0][0]
            log.info("tablet: ${partitionTablets[i][j]}, rowCount: ${partitionRowCounts[i][j]}")
        }
    }

    assertEquals(5, partitionRowCounts[0][partitionBeginIdx[0]])
    assertEquals(5, partitionRowCounts[0][(partitionBeginIdx[0] + 1) % partitionTablets[0].size()])
    assertEquals(5, partitionRowCounts[0][(partitionBeginIdx[0] + 2) % partitionTablets[0].size()])

    for (int i = 3; i < partitionTablets[0].size(); i++) {
        assertEquals(0, partitionRowCounts[0][(partitionBeginIdx[0] + i) % partitionTablets[0].size()])
    }

    assertEquals(5, partitionRowCounts[1][partitionBeginIdx[1]])
    assertEquals(5, partitionRowCounts[1][(partitionBeginIdx[1] + 1) % partitionTablets[1].size()])
    assertEquals(5, partitionRowCounts[1][(partitionBeginIdx[1] + 2) % partitionTablets[1].size()])
    for (int i = 3; i < partitionTablets[1].size(); i++) {
        assertEquals(0, partitionRowCounts[1][(partitionBeginIdx[1] + i) % partitionTablets[1].size()])
    }
}

