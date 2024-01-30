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

/**
 *   @Params url is "/xxx"
 *   @Return response body
 */
def http_get(url) {
    def conn = new URL(url).openConnection()
    conn.setRequestMethod("GET")
    //token for root
    return conn.getInputStream().getText()
}

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
    assertEquals(totalCount[0][0], 10)
    def res = sql "show tablets from ${tableName}"
    def tabletMetaUrl1 = res[0][17]
    def tabletMetaUrl2 = res[1][17]
    def tabletMetaUrl3 = res[2][17]
    def tabletMetaRes1 = http_get(tabletMetaUrl1)
    def tabletMetaRes2 = http_get(tabletMetaUrl2)
    def tabletMetaRes3 = http_get(tabletMetaUrl3)

    def obj1 = new JsonSlurper().parseText(tabletMetaRes1)
    def obj2 = new JsonSlurper().parseText(tabletMetaRes2)
    def obj3 = new JsonSlurper().parseText(tabletMetaRes3)
    def rowCount1 = obj1.rs_metas[0].num_rows + obj1.rs_metas[1].num_rows
    def rowCount2 = obj2.rs_metas[0].num_rows + obj2.rs_metas[1].num_rows
    def rowCount3 = obj3.rs_metas[0].num_rows + obj3.rs_metas[1].num_rows

    assertEquals(rowCount1, 10)
    assertEquals(rowCount2, 0)
    assertEquals(rowCount3, 0)

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
    assertEquals(totalCount[0][0], 20)
    tabletMetaRes1 = http_get(tabletMetaUrl1)
    tabletMetaRes2 = http_get(tabletMetaUrl2)
    tabletMetaRes3 = http_get(tabletMetaUrl3)

    obj1 = new JsonSlurper().parseText(tabletMetaRes1)
    obj2 = new JsonSlurper().parseText(tabletMetaRes2)
    obj3 = new JsonSlurper().parseText(tabletMetaRes3)

    rowCount1 = obj1.rs_metas[0].num_rows + obj1.rs_metas[1].num_rows + obj1.rs_metas[2].num_rows
    rowCount2 = obj2.rs_metas[0].num_rows + obj2.rs_metas[1].num_rows + obj2.rs_metas[2].num_rows
    rowCount3 = obj3.rs_metas[0].num_rows + obj3.rs_metas[1].num_rows + obj3.rs_metas[2].num_rows
    assertEquals(rowCount1, 10)
    assertEquals(rowCount2, 10)
    assertEquals(rowCount3, 0)

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
    assertEquals(totalCount[0][0], 30)
    tabletMetaRes1 = http_get(tabletMetaUrl1)
    tabletMetaRes2 = http_get(tabletMetaUrl2)
    tabletMetaRes3 = http_get(tabletMetaUrl3)

    obj1 = new JsonSlurper().parseText(tabletMetaRes1)
    obj2 = new JsonSlurper().parseText(tabletMetaRes2)
    obj3 = new JsonSlurper().parseText(tabletMetaRes3)

    rowCount1 = obj1.rs_metas[0].num_rows + obj1.rs_metas[1].num_rows + obj1.rs_metas[2].num_rows + obj1.rs_metas[3].num_rows
    rowCount2 = obj2.rs_metas[0].num_rows + obj2.rs_metas[1].num_rows + obj2.rs_metas[2].num_rows + obj2.rs_metas[3].num_rows
    rowCount3 = obj3.rs_metas[0].num_rows + obj3.rs_metas[1].num_rows + obj3.rs_metas[2].num_rows + obj3.rs_metas[3].num_rows
    assertEquals(rowCount1, 10)
    assertEquals(rowCount2, 10)
    assertEquals(rowCount3, 10)

    // test partitioned table
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
    assertEquals(totalCount[0][0], 10)
    res = sql "show tablets from ${tableName} partitions(p20231011, p20231012)"
    tabletMetaUrl1 = res[0][17]
    tabletMetaUrl2 = res[1][17]
    tabletMetaUrl3 = res[2][17]
    tabletMetaUrl4 = res[10][17]
    tabletMetaUrl5 = res[11][17]
    tabletMetaUrl6 = res[12][17]
    tabletMetaRes1 = http_get(tabletMetaUrl1)
    tabletMetaRes2 = http_get(tabletMetaUrl2)
    tabletMetaRes3 = http_get(tabletMetaUrl3)
    tabletMetaRes4 = http_get(tabletMetaUrl4)
    tabletMetaRes5 = http_get(tabletMetaUrl5)
    tabletMetaRes6 = http_get(tabletMetaUrl6)

    obj1 = new JsonSlurper().parseText(tabletMetaRes1)
    obj2 = new JsonSlurper().parseText(tabletMetaRes2)
    obj3 = new JsonSlurper().parseText(tabletMetaRes3)
    obj4 = new JsonSlurper().parseText(tabletMetaRes4)
    obj5 = new JsonSlurper().parseText(tabletMetaRes5)
    obj6 = new JsonSlurper().parseText(tabletMetaRes6)

    rowCount1 = obj1.rs_metas[0].num_rows + obj1.rs_metas[1].num_rows
    rowCount2 = obj2.rs_metas[0].num_rows + obj2.rs_metas[1].num_rows
    rowCount3 = obj3.rs_metas[0].num_rows + obj3.rs_metas[1].num_rows
    def rowCount4 = obj4.rs_metas[0].num_rows + obj4.rs_metas[1].num_rows
    def rowCount5 = obj5.rs_metas[0].num_rows + obj5.rs_metas[1].num_rows
    def rowCount6 = obj6.rs_metas[0].num_rows + obj6.rs_metas[1].num_rows
    assertEquals(rowCount1, 5)
    assertEquals(rowCount2, 0)
    assertEquals(rowCount3, 0)
    assertEquals(rowCount4, 5)
    assertEquals(rowCount5, 0)
    assertEquals(rowCount6, 0)

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
    assertEquals(totalCount[0][0], 20)
    tabletMetaRes1 = http_get(tabletMetaUrl1)
    tabletMetaRes2 = http_get(tabletMetaUrl2)
    tabletMetaRes3 = http_get(tabletMetaUrl3)
    tabletMetaRes4 = http_get(tabletMetaUrl4)
    tabletMetaRes5 = http_get(tabletMetaUrl5)
    tabletMetaRes6 = http_get(tabletMetaUrl6)

    obj1 = new JsonSlurper().parseText(tabletMetaRes1)
    obj2 = new JsonSlurper().parseText(tabletMetaRes2)
    obj3 = new JsonSlurper().parseText(tabletMetaRes3)
    obj4 = new JsonSlurper().parseText(tabletMetaRes4)
    obj5 = new JsonSlurper().parseText(tabletMetaRes5)
    obj6 = new JsonSlurper().parseText(tabletMetaRes6)

    rowCount1 = obj1.rs_metas[0].num_rows + obj1.rs_metas[1].num_rows + obj1.rs_metas[2].num_rows
    rowCount2 = obj2.rs_metas[0].num_rows + obj2.rs_metas[1].num_rows + obj2.rs_metas[2].num_rows
    rowCount3 = obj3.rs_metas[0].num_rows + obj3.rs_metas[1].num_rows + obj3.rs_metas[2].num_rows
    rowCount4 = obj4.rs_metas[0].num_rows + obj4.rs_metas[1].num_rows + obj4.rs_metas[2].num_rows
    rowCount5 = obj5.rs_metas[0].num_rows + obj5.rs_metas[1].num_rows + obj5.rs_metas[2].num_rows
    rowCount6 = obj6.rs_metas[0].num_rows + obj6.rs_metas[1].num_rows + obj6.rs_metas[2].num_rows
    assertEquals(rowCount1, 5)
    assertEquals(rowCount2, 5)
    assertEquals(rowCount3, 0)
    assertEquals(rowCount4, 5)
    assertEquals(rowCount5, 5)
    assertEquals(rowCount6, 0)

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
    assertEquals(totalCount[0][0], 30)
    tabletMetaRes1 = http_get(tabletMetaUrl1)
    tabletMetaRes2 = http_get(tabletMetaUrl2)
    tabletMetaRes3 = http_get(tabletMetaUrl3)
    tabletMetaRes4 = http_get(tabletMetaUrl4)
    tabletMetaRes5 = http_get(tabletMetaUrl5)
    tabletMetaRes6 = http_get(tabletMetaUrl6)

    obj1 = new JsonSlurper().parseText(tabletMetaRes1)
    obj2 = new JsonSlurper().parseText(tabletMetaRes2)
    obj3 = new JsonSlurper().parseText(tabletMetaRes3)
    obj4 = new JsonSlurper().parseText(tabletMetaRes4)
    obj5 = new JsonSlurper().parseText(tabletMetaRes5)
    obj6 = new JsonSlurper().parseText(tabletMetaRes6)

    rowCount1 = obj1.rs_metas[0].num_rows + obj1.rs_metas[1].num_rows + obj1.rs_metas[2].num_rows + obj1.rs_metas[3].num_rows
    rowCount2 = obj2.rs_metas[0].num_rows + obj2.rs_metas[1].num_rows + obj2.rs_metas[2].num_rows + obj2.rs_metas[3].num_rows
    rowCount3 = obj3.rs_metas[0].num_rows + obj3.rs_metas[1].num_rows + obj3.rs_metas[2].num_rows + obj3.rs_metas[3].num_rows
    rowCount4 = obj4.rs_metas[0].num_rows + obj4.rs_metas[1].num_rows + obj4.rs_metas[2].num_rows + obj4.rs_metas[3].num_rows
    rowCount5 = obj5.rs_metas[0].num_rows + obj5.rs_metas[1].num_rows + obj5.rs_metas[2].num_rows + obj5.rs_metas[3].num_rows
    rowCount6 = obj6.rs_metas[0].num_rows + obj6.rs_metas[1].num_rows + obj6.rs_metas[2].num_rows + obj6.rs_metas[3].num_rows
    assertEquals(rowCount1, 5)
    assertEquals(rowCount2, 5)
    assertEquals(rowCount3, 5)
    assertEquals(rowCount4, 5)
    assertEquals(rowCount5, 5)
    assertEquals(rowCount6, 5)
}
