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

    def tabletId1 = tablets[0]
    def tabletId2 = tablets[1]
    def tabletId3 = tablets[2] 
    def tabletId4 = tablets[3] 
    def tabletId5 = tablets[4] 

    def rowCount1 = sql "select count() from ${tableName} tablet(${tabletId1})"
    def rowCount2 = sql "select count() from ${tableName} tablet(${tabletId2})"
    def rowCount3 = sql "select count() from ${tableName} tablet(${tabletId3})"
    def rowCount4 = sql "select count() from ${tableName} tablet(${tabletId4})"
    def rowCount5 = sql "select count() from ${tableName} tablet(${tabletId5})"

    assertEquals(rowCount1[0][0], 3)
    assertEquals(rowCount2[0][0], 1)
    assertEquals(rowCount3[0][0], 0)
    assertEquals(rowCount4[0][0], 0)
    assertEquals(rowCount5[0][0], 0)

    sql "set batch_size=2"
    // insert second time
    sql "insert into ${tableName} values('2021-11-14', '2', '3', '4', 55), ('2022-12-13', '3', '31', '4', 55), ('2023-10-14', '23', '45', '66', 88), ('2023-10-16', '2', '3', '4', 55)"

    totalCount = sql "select count() from ${tableName}"
    assertEquals(totalCount[0][0], 8)

    rowCount1 = sql "select count() from ${tableName} tablet(${tabletId1})"
    rowCount2 = sql "select count() from ${tableName} tablet(${tabletId2})"
    rowCount3 = sql "select count() from ${tableName} tablet(${tabletId3})"
    rowCount4 = sql "select count() from ${tableName} tablet(${tabletId4})"
    rowCount5 = sql "select count() from ${tableName} tablet(${tabletId5})"

    assertEquals(rowCount1[0][0], 3)
    assertEquals(rowCount2[0][0], 4)
    assertEquals(rowCount3[0][0], 1)
    assertEquals(rowCount4[0][0], 0)
    assertEquals(rowCount5[0][0], 0)

    sql "set batch_size=2"
    // insert third time
    sql "insert into ${tableName} values('2021-11-14', '2', '3', '4', 55), ('2022-12-13', '3', '31', '4', 55), ('2023-10-14', '23', '45', '66', 88), ('2023-10-16', '2', '3', '4', 55)"

    totalCount = sql "select count() from ${tableName}"
    assertEquals(totalCount[0][0], 12)

    rowCount1 = sql "select count() from ${tableName} tablet(${tabletId1})"
    rowCount2 = sql "select count() from ${tableName} tablet(${tabletId2})"
    rowCount3 = sql "select count() from ${tableName} tablet(${tabletId3})"
    rowCount4 = sql "select count() from ${tableName} tablet(${tabletId4})"
    rowCount5 = sql "select count() from ${tableName} tablet(${tabletId5})"

    assertEquals(rowCount1[0][0], 3)
    assertEquals(rowCount2[0][0], 4)
    assertEquals(rowCount3[0][0], 4)
    assertEquals(rowCount4[0][0], 1)
    assertEquals(rowCount5[0][0], 0)

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
    def partition1 = "p20231011"
    def partition2 = "p20231012"
    def partition3 = "p20231013"
    assertEquals(totalCount[0][0], 5)
    res = sql "show tablets from ${tableName} partition ${partition1}"
    tablets = getTablets.call(res)
    def tabletId11 = tablets[0] 
    def tabletId12 = tablets[1] 
    def tabletId13 = tablets[2] 
    def tabletId14 = tablets[3] 
    def tabletId15 = tablets[4] 

    res = sql "show tablets from ${tableName} partition ${partition2}"
    tablets = getTablets.call(res)
    def tabletId21 = tablets[0] 
    def tabletId22 = tablets[1] 
    def tabletId23 = tablets[2] 
    def tabletId24 = tablets[3] 
    def tabletId25 = tablets[4] 

    res = sql "show tablets from ${tableName} partition ${partition3}"
    tablets = getTablets.call(res)
    def tabletId31 = tablets[0] 
    def tabletId32 = tablets[1] 
    def tabletId33 = tablets[2] 
    def tabletId34 = tablets[3] 
    def tabletId35 = tablets[4] 

    def rowCount11 = sql "select count() from ${tableName} tablet(${tabletId11})"
    def rowCount12 = sql "select count() from ${tableName} tablet(${tabletId12})"
    def rowCount13 = sql "select count() from ${tableName} tablet(${tabletId13})"
    def rowCount14 = sql "select count() from ${tableName} tablet(${tabletId14})"
    def rowCount15 = sql "select count() from ${tableName} tablet(${tabletId15})"

    def rowCount21 = sql "select count() from ${tableName} tablet(${tabletId21})"
    def rowCount22 = sql "select count() from ${tableName} tablet(${tabletId22})"
    def rowCount23 = sql "select count() from ${tableName} tablet(${tabletId23})"
    def rowCount24 = sql "select count() from ${tableName} tablet(${tabletId24})"
    def rowCount25 = sql "select count() from ${tableName} tablet(${tabletId25})"

    def rowCount31 = sql "select count() from ${tableName} tablet(${tabletId31})"
    def rowCount32 = sql "select count() from ${tableName} tablet(${tabletId32})"
    def rowCount33 = sql "select count() from ${tableName} tablet(${tabletId33})"
    def rowCount34 = sql "select count() from ${tableName} tablet(${tabletId34})"
    def rowCount35 = sql "select count() from ${tableName} tablet(${tabletId35})"

    assertEquals(rowCount11[0][0], 2)
    assertEquals(rowCount12[0][0], 1)
    assertEquals(rowCount13[0][0], 0)
    assertEquals(rowCount14[0][0], 0)
    assertEquals(rowCount15[0][0], 0)

    assertEquals(rowCount21[0][0], 1)
    assertEquals(rowCount22[0][0], 1)
    assertEquals(rowCount23[0][0], 0)
    assertEquals(rowCount24[0][0], 0)
    assertEquals(rowCount25[0][0], 0)

    assertEquals(rowCount31[0][0], 0)
    assertEquals(rowCount32[0][0], 0)
    assertEquals(rowCount33[0][0], 0)
    assertEquals(rowCount34[0][0], 0)
    assertEquals(rowCount35[0][0], 0)

    sql "set batch_size=1"
    // insert second time
    sql "insert into ${tableName} values('2023-10-12', '2', '3', '4', 55), ('2023-10-12', '3', '31', '4', 55), ('2023-10-12', '23', '45', '66', 88), ('2023-10-13', '2', '3', '4', 55),('2023-10-11', '11', '13', '4', 55)"

    sql "sync"
    totalCount = sql "select count() from ${tableName}"
    assertEquals(totalCount[0][0], 10)

    rowCount11 = sql "select count() from ${tableName} tablet(${tabletId11})"
    rowCount12 = sql "select count() from ${tableName} tablet(${tabletId12})"
    rowCount13 = sql "select count() from ${tableName} tablet(${tabletId13})"
    rowCount14 = sql "select count() from ${tableName} tablet(${tabletId14})"
    rowCount15 = sql "select count() from ${tableName} tablet(${tabletId15})"

    rowCount21 = sql "select count() from ${tableName} tablet(${tabletId21})"
    rowCount22 = sql "select count() from ${tableName} tablet(${tabletId22})"
    rowCount23 = sql "select count() from ${tableName} tablet(${tabletId23})"
    rowCount24 = sql "select count() from ${tableName} tablet(${tabletId24})"
    rowCount25 = sql "select count() from ${tableName} tablet(${tabletId25})"

    rowCount31 = sql "select count() from ${tableName} tablet(${tabletId31})"
    rowCount32 = sql "select count() from ${tableName} tablet(${tabletId32})"
    rowCount33 = sql "select count() from ${tableName} tablet(${tabletId33})"
    rowCount34 = sql "select count() from ${tableName} tablet(${tabletId34})"
    rowCount35 = sql "select count() from ${tableName} tablet(${tabletId35})"

    assertEquals(rowCount11[0][0], 2)
    assertEquals(rowCount12[0][0], 2)
    assertEquals(rowCount13[0][0], 0)
    assertEquals(rowCount14[0][0], 0)
    assertEquals(rowCount15[0][0], 0)

    assertEquals(rowCount21[0][0], 1)
    assertEquals(rowCount22[0][0], 3)
    assertEquals(rowCount23[0][0], 1)
    assertEquals(rowCount24[0][0], 0)
    assertEquals(rowCount25[0][0], 0)

    assertEquals(rowCount31[0][0], 0)
    assertEquals(rowCount32[0][0], 1)
    assertEquals(rowCount33[0][0], 0)
    assertEquals(rowCount34[0][0], 0)
    assertEquals(rowCount35[0][0], 0)

    sql "set batch_size=1"
    // insert third time
    sql "insert into ${tableName} values('2023-10-13', '2', '3', '4', 55), ('2023-10-13', '3', '31', '4', 55), ('2023-10-12', '23', '45', '66', 88), ('2023-10-13', '2', '3', '4', 55),('2023-10-11', '11', '13', '4', 55),('2023-10-11', '13', '145', '4', 55)"
    sql "set batch_size=4064"

    sql "sync"
    totalCount = sql "select count() from ${tableName}"
    assertEquals(totalCount[0][0], 16)

    rowCount11 = sql "select count() from ${tableName} tablet(${tabletId11})"
    rowCount12 = sql "select count() from ${tableName} tablet(${tabletId12})"
    rowCount13 = sql "select count() from ${tableName} tablet(${tabletId13})"
    rowCount14 = sql "select count() from ${tableName} tablet(${tabletId14})"
    rowCount15 = sql "select count() from ${tableName} tablet(${tabletId15})"

    rowCount21 = sql "select count() from ${tableName} tablet(${tabletId21})"
    rowCount22 = sql "select count() from ${tableName} tablet(${tabletId22})"
    rowCount23 = sql "select count() from ${tableName} tablet(${tabletId23})"
    rowCount24 = sql "select count() from ${tableName} tablet(${tabletId24})"
    rowCount25 = sql "select count() from ${tableName} tablet(${tabletId25})"

    rowCount31 = sql "select count() from ${tableName} tablet(${tabletId31})"
    rowCount32 = sql "select count() from ${tableName} tablet(${tabletId32})"
    rowCount33 = sql "select count() from ${tableName} tablet(${tabletId33})"
    rowCount34 = sql "select count() from ${tableName} tablet(${tabletId34})"
    rowCount35 = sql "select count() from ${tableName} tablet(${tabletId35})"

    assertEquals(rowCount11[0][0], 2)
    assertEquals(rowCount12[0][0], 2)
    assertEquals(rowCount13[0][0], 2)
    assertEquals(rowCount14[0][0], 0)
    assertEquals(rowCount15[0][0], 0)

    assertEquals(rowCount21[0][0], 1)
    assertEquals(rowCount22[0][0], 3)
    assertEquals(rowCount23[0][0], 2)
    assertEquals(rowCount24[0][0], 0)
    assertEquals(rowCount25[0][0], 0)

    assertEquals(rowCount31[0][0], 0)
    assertEquals(rowCount32[0][0], 1)
    assertEquals(rowCount33[0][0], 2)
    assertEquals(rowCount34[0][0], 1)
    assertEquals(rowCount35[0][0], 0)
}
