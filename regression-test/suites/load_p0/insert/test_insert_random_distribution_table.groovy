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

suite("test_insert_random_distribution_table", "p0") {
    def tableName = "test_insert_random_distribution_table"

    // ${tableName} unpartitioned table
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
          `k1` date NULL,
          `k2` char(100) NULL,
          `v1` char(100) NULL,
          `v2` text NULL,
          `v3` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY RANDOM BUCKETS 5
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql "set batch_size=2"
    // insert first time
    sql "insert into ${tableName} values('2021-11-14', '2', '3', '4', 55), ('2022-12-13', '3', '31', '4', 55), ('2023-10-14', '23', '45', '66', 88), ('2023-10-16', '2', '3', '4', 55)"

    sql "sync"
    def totalCount = sql "select count() from ${tableName}"
    assertEquals(totalCount[0][0], 4)
    def res = sql "show tablets from ${tableName}"
    def getTablets = { result -> 
      def map = [:]
      def tablets = []
      for (int i = 0; i < result.size(); i++) {
          if (!map.get(result[i][0])) {
              tablets.add(result[i][0])
              map.put(result[i][0], result[i])
          }
      }
      return tablets
    }

    def tablets = getTablets.call(res)

    // define an array to store count of each tablet
    def rowCounts = []   

    def beginIdx = -1
    for (int i = tablets.size() - 1; i >= 0; i--) {
        def countResult = sql "select count() from ${tableName} tablet(${tablets[i]})"
        rowCounts[i] = countResult[0][0]
        log.info("tablet = ${tablets[i]}, rowCounts[${i}] = ${rowCounts[i]}")
        if (rowCounts[i] > 0 && (beginIdx == (i + 1) || beginIdx == -1)) {
            beginIdx = i
        }
    }

    assertEquals(rowCounts[beginIdx], 2)
    assertEquals(rowCounts[(beginIdx + 1) % 5], 2)
    assertEquals(rowCounts[(beginIdx + 2) % 5], 0)
    assertEquals(rowCounts[(beginIdx + 3) % 5], 0)
    assertEquals(rowCounts[(beginIdx + 4) % 5], 0)

    sql "set batch_size=2"
    // insert second time
    sql "insert into ${tableName} values('2021-11-14', '2', '3', '4', 55), ('2022-12-13', '3', '31', '4', 55), ('2023-10-14', '23', '45', '66', 88), ('2023-10-16', '2', '3', '4', 55)"

    totalCount = sql "select count() from ${tableName}"
    assertEquals(totalCount[0][0], 8)

    for (int i = 0; i < tablets.size(); i++) {
        def countResult = sql "select count() from ${tableName} tablet(${tablets[i]})"
        rowCounts[i] = countResult[0][0]
        log.info("tablet = ${tablets[i]}, rowCounts[${i}] = ${rowCounts[i]}")
    }
    assertEquals(rowCounts[(beginIdx + 0) % 5], 2)
    assertEquals(rowCounts[(beginIdx + 1) % 5], 4)
    assertEquals(rowCounts[(beginIdx + 2) % 5], 2)
    assertEquals(rowCounts[(beginIdx + 3) % 5], 0)
    assertEquals(rowCounts[(beginIdx + 4) % 5], 0)

    sql "set batch_size=2"
    // insert third time
    sql "insert into ${tableName} values('2021-11-14', '2', '3', '4', 55), ('2022-12-13', '3', '31', '4', 55), ('2023-10-14', '23', '45', '66', 88), ('2023-10-16', '2', '3', '4', 55)"

    totalCount = sql "select count() from ${tableName}"
    assertEquals(totalCount[0][0], 12)

    for (int i = 0; i < tablets.size(); i++) {
        def countResult = sql "select count() from ${tableName} tablet(${tablets[i]})"
        rowCounts[i] = countResult[0][0]
        log.info("tablet = ${tablets[i]}, rowCounts[${i}] = ${rowCounts[i]}")
    }

    assertEquals(rowCounts[(beginIdx + 0) % 5], 2)
    assertEquals(rowCounts[(beginIdx + 1) % 5], 4)
    assertEquals(rowCounts[(beginIdx + 2) % 5], 4)
    assertEquals(rowCounts[(beginIdx + 3) % 5], 2)
    assertEquals(rowCounts[(beginIdx + 4) % 5], 0)

    // ${tableName} partitioned table
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
        PARTITION p20231013 VALUES [('2023-10-13'), ('2023-10-14')))
        DISTRIBUTED BY RANDOM BUCKETS 10
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql "set batch_size=1"
    // insert first time
    sql "insert into ${tableName} values('2023-10-11', '2', '3', '4', 55), ('2023-10-11', '3', '31', '4', 55), ('2023-10-11', '23', '45', '66', 88), ('2023-10-12', '2', '3', '4', 55),('2023-10-12', '12', '13', '4', 55)"

    sql "sync"
    totalCount = sql "select count() from ${tableName}"
    assertEquals(totalCount[0][0], 5)

    def partitions = ["p20231011", "p20231012", "p20231013"]
    def partitionTablets = []
    def partitionRowCounts = []
    def partitionBeginIdx = [];
    for (int p = 0; p < 3; p++) {
        res = sql "show tablets from ${tableName} partition ${partitions[p]}"
        partitionTablets[p] = getTablets.call(res)
        partitionRowCounts[p] = []
        def numTablets = partitionTablets[p].size()
        for (int i = numTablets - 1; i >= 0; i--) {
            def countResult = sql "select count() from ${tableName} tablet(${partitionTablets[p][i]})"
            partitionRowCounts[p][i] = countResult[0][0]
            log.info("tablet = ${partitionTablets[p][i]}, partitionRowCounts[${p}][${i}] = " +
                     "${partitionRowCounts[p][i]}")
            if (partitionRowCounts[p][i] > 0 &&
                (partitionBeginIdx[p] == (i + 1) || partitionBeginIdx[p] == null)) {
                partitionBeginIdx[p] = i
            }
        }
    }

    assertEquals(partitionRowCounts[0][partitionBeginIdx[0]], 1)
    assertEquals(partitionRowCounts[0][(partitionBeginIdx[0] + 1) % 10], 1)
    assertEquals(partitionRowCounts[0][(partitionBeginIdx[0] + 2) % 10], 1)
    for (int i = 3; i < 10; i++) {
      assertEquals(partitionRowCounts[0][(partitionBeginIdx[0] + i) % 10], 0)
    }
    
    assertEquals(partitionRowCounts[1][partitionBeginIdx[1]], 1)
    assertEquals(partitionRowCounts[1][(partitionBeginIdx[1] + 1) % 10], 1)
    for (int i = 2; i < 10; i++) {
      assertEquals(partitionRowCounts[1][(partitionBeginIdx[1] + i) % 10], 0)
    }

    for (int i = 0; i < 10; i++) {
      assertEquals(partitionRowCounts[2][i], 0)
    }

    sql "set batch_size=1"
    // insert second time
    sql "insert into ${tableName} values('2023-10-12', '2', '3', '4', 55), ('2023-10-12', '3', '31', '4', 55), ('2023-10-12', '23', '45', '66', 88), ('2023-10-13', '2', '3', '4', 55),('2023-10-11', '11', '13', '4', 55)"

    sql "sync"
    totalCount = sql "select count() from ${tableName}"
    assertEquals(totalCount[0][0], 10)


    for (int p = 0; p < 3; p++) {
        def numTablets = partitionTablets[p].size()
        for (int i = numTablets - 1; i >= 0; i--) {
            def countResult = sql "select count() from ${tableName} tablet(${partitionTablets[p][i]})"
            partitionRowCounts[p][i] = countResult[0][0]
            if (p == 2) {
               if ((partitionRowCounts[p][i]) > 0 &&
                   (partitionBeginIdx[p] == (i + 1) || partitionBeginIdx[p] == null)) {
                   partitionBeginIdx[p] = i
               }
            }
            log.info("tablet = ${partitionTablets[p][i]}, partitionRowCounts[${p}][${i}] = " +
                     "${partitionRowCounts[p][i]}")
        }
    }

    assertEquals(partitionRowCounts[0][partitionBeginIdx[0]], 1)
    assertEquals(partitionRowCounts[0][(partitionBeginIdx[0] + 1) % 10], 2)
    assertEquals(partitionRowCounts[0][(partitionBeginIdx[0] + 2) % 10], 1)
    for (int i = 3; i < 10; i++) {
      assertEquals(partitionRowCounts[0][(partitionBeginIdx[0] + i) % 10], 0)
    }

    assertEquals(partitionRowCounts[1][partitionBeginIdx[1]], 1)
    assertEquals(partitionRowCounts[1][(partitionBeginIdx[1] + 1) % 10], 2)
    assertEquals(partitionRowCounts[1][(partitionBeginIdx[1] + 2) % 10], 1)
    assertEquals(partitionRowCounts[1][(partitionBeginIdx[1] + 3) % 10], 1)
    for (int i = 4; i < 10; i++) {
      assertEquals(partitionRowCounts[1][(partitionBeginIdx[1] + i) % 10], 0)
    }

    assertEquals(partitionRowCounts[2][partitionBeginIdx[2]], 1)
    for (int i = 1; i < 10; i++) {
        assertEquals(partitionRowCounts[2][(partitionBeginIdx[2] + i) % 10], 0)
    }

    sql "set batch_size=1"
    // insert third time
    sql "insert into ${tableName} values('2023-10-13', '2', '3', '4', 55), ('2023-10-13', '3', '31', '4', 55), ('2023-10-12', '23', '45', '66', 88), ('2023-10-13', '2', '3', '4', 55),('2023-10-11', '11', '13', '4', 55),('2023-10-11', '13', '145', '4', 55)"
    sql "set batch_size=4064"

    sql "sync"
    totalCount = sql "select count() from ${tableName}"
    assertEquals(totalCount[0][0], 16)

    for (int p = 0; p < 3; p++) {
        for (int i = 0; i < partitionTablets[p].size(); i++) {
            def countResult = sql "select count() from ${tableName} tablet(${partitionTablets[p][i]})"
            partitionRowCounts[p][i] = countResult[0][0]
            log.info("tablet = ${partitionTablets[p][i]}, partitionRowCounts[${p}][${i}] = " +
                     "${partitionRowCounts[p][i]}")
        }
    }

    assertEquals(partitionRowCounts[0][partitionBeginIdx[0]], 1)
    assertEquals(partitionRowCounts[0][(partitionBeginIdx[0] + 1) % 10], 2)
    assertEquals(partitionRowCounts[0][(partitionBeginIdx[0] + 2) % 10], 2)
    assertEquals(partitionRowCounts[0][(partitionBeginIdx[0] + 3) % 10], 1)
    for (int i = 4; i < 10; i++) {
      assertEquals(partitionRowCounts[0][(partitionBeginIdx[0] + i) % 10], 0)
    }

    assertEquals(partitionRowCounts[1][partitionBeginIdx[1]], 1)
    assertEquals(partitionRowCounts[1][(partitionBeginIdx[1] + 1) % 10], 2)
    assertEquals(partitionRowCounts[1][(partitionBeginIdx[1] + 2) % 10], 2)
    assertEquals(partitionRowCounts[1][(partitionBeginIdx[1] + 3) % 10], 1)
    for (int i = 4; i < 10; i++) {
      assertEquals(partitionRowCounts[1][(partitionBeginIdx[1] + i) % 10], 0)
    }

    assertEquals(partitionRowCounts[2][partitionBeginIdx[2]], 1)
    assertEquals(partitionRowCounts[2][(partitionBeginIdx[2] + 1) % 10], 1)
    assertEquals(partitionRowCounts[2][(partitionBeginIdx[2] + 2) % 10], 1)
    assertEquals(partitionRowCounts[2][(partitionBeginIdx[2] + 3) % 10], 1)
    for (int i = 4; i < 10; i++) {
      assertEquals(partitionRowCounts[2][(partitionBeginIdx[2] + i) % 10], 0)
    }
}
