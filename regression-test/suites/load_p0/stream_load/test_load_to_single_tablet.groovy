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
        time 10000 // limit inflight 10s
    }

    sql "sync"
    def totalCount = sql "select count() from ${tableName}"
    assertEquals(10, totalCount[0][0])
    def res = sql "show tablets from ${tableName}"
    def tablet1 = res[0][0]
    def tablet2 = res[1][0]
    def tablet3 = res[2][0]
    def rowCount1 = sql "select count() from ${tableName} tablet(${tablet1})"
    def rowCount2 = sql "select count() from ${tableName} tablet(${tablet2})"
    def rowCount3 = sql "select count() from ${tableName} tablet(${tablet3})"

    assertEquals(10, rowCount1[0][0])
    assertEquals(0, rowCount2[0][0])
    assertEquals(0, rowCount3[0][0])
    

    // load second time
    streamLoad {
        table "${tableName}"

        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'load_to_single_tablet', 'true'

        file 'test_load_to_single_tablet.json'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    totalCount = sql "select count() from ${tableName}"
    rowCount1 = sql "select count() from ${tableName} tablet(${tablet1})"
    rowCount2 = sql "select count() from ${tableName} tablet(${tablet2})"
    rowCount3 = sql "select count() from ${tableName} tablet(${tablet3})"
    assertEquals(20, totalCount[0][0])
    assertEquals(10, rowCount1[0][0])
    assertEquals(10, rowCount2[0][0])
    assertEquals(0, rowCount3[0][0])

    // load third time
    streamLoad {
        table "${tableName}"

        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'load_to_single_tablet', 'true'

        file 'test_load_to_single_tablet.json'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    totalCount = sql "select count() from ${tableName}"
    rowCount1 = sql "select count() from ${tableName} tablet(${tablet1})"
    rowCount2 = sql "select count() from ${tableName} tablet(${tablet2})"
    rowCount3 = sql "select count() from ${tableName} tablet(${tablet3})"
    assertEquals(30, totalCount[0][0])
    assertEquals(10, rowCount1[0][0])
    assertEquals(10, rowCount2[0][0])
    assertEquals(10, rowCount3[0][0])

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
        time 10000 // limit inflight 10s
    }

    sql "sync"
    totalCount = sql "select count() from ${tableName}"
    assertEquals(10, totalCount[0][0])
    res = sql "show tablets from ${tableName} partitions(p20231011, p20231012)"
    tablet1 = res[0][0]
    tablet2 = res[1][0]
    tablet3 = res[2][0]
    tablet4 = res[10][0]
    tablet5 = res[11][0]
    tablet6 = res[12][0]

    rowCount1 = sql "select count() from ${tableName} tablet(${tablet1})"
    rowCount2 = sql "select count() from ${tableName} tablet(${tablet2})"
    rowCount3 = sql "select count() from ${tableName} tablet(${tablet3})"
    def rowCount4 = sql "select count() from ${tableName} tablet(${tablet4})"
    def rowCount5 = sql "select count() from ${tableName} tablet(${tablet5})"
    def rowCount6 = sql "select count() from ${tableName} tablet(${tablet6})"
    assertEquals(5, rowCount1[0][0])
    assertEquals(0, rowCount2[0][0])
    assertEquals(0, rowCount3[0][0])
    assertEquals(5, rowCount4[0][0])
    assertEquals(0, rowCount5[0][0])
    assertEquals(0, rowCount6[0][0])

    // load second time
    streamLoad {
        table "${tableName}"

        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'load_to_single_tablet', 'true'

        file 'test_load_to_single_tablet.json'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    totalCount = sql "select count() from ${tableName}"
    rowCount1 = sql "select count() from ${tableName} tablet(${tablet1})"
    rowCount2 = sql "select count() from ${tableName} tablet(${tablet2})"
    rowCount3 = sql "select count() from ${tableName} tablet(${tablet3})"
    rowCount4 = sql "select count() from ${tableName} tablet(${tablet4})"
    rowCount5 = sql "select count() from ${tableName} tablet(${tablet5})"
    rowCount6 = sql "select count() from ${tableName} tablet(${tablet6})"
    assertEquals(20, totalCount[0][0])
    assertEquals(5, rowCount1[0][0])
    assertEquals(5, rowCount2[0][0])
    assertEquals(0, rowCount3[0][0])
    assertEquals(5, rowCount4[0][0])
    assertEquals(5, rowCount5[0][0])
    assertEquals(0, rowCount6[0][0])

    // load third time
    streamLoad {
        table "${tableName}"

        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'load_to_single_tablet', 'true'

        file 'test_load_to_single_tablet.json'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    totalCount = sql "select count() from ${tableName}"
    rowCount1 = sql "select count() from ${tableName} tablet(${tablet1})"
    rowCount2 = sql "select count() from ${tableName} tablet(${tablet2})"
    rowCount3 = sql "select count() from ${tableName} tablet(${tablet3})"
    rowCount4 = sql "select count() from ${tableName} tablet(${tablet4})"
    rowCount5 = sql "select count() from ${tableName} tablet(${tablet5})"
    rowCount6 = sql "select count() from ${tableName} tablet(${tablet6})"
    assertEquals(30, totalCount[0][0])
    assertEquals(5, rowCount1[0][0])
    assertEquals(5, rowCount2[0][0])
    assertEquals(5, rowCount3[0][0])
    assertEquals(5, rowCount4[0][0])
    assertEquals(5, rowCount5[0][0])
    assertEquals(5, rowCount6[0][0])
}

