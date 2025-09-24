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

suite("test_random_tablet_switching_threshold", "p0") {
    def tableName = "test_random_tablet_switching_threshold"

    // Test 1: Basic threshold-based switching for unpartitioned table
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
        DISTRIBUTED BY RANDOM BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Helper function to get tablet distribution
    def getTabletDistribution = { table ->
        String[][] res = sql "show tablets from ${table}"
        res = deduplicate_tablets(res)

        def tablets = []
        def rowCounts = []
        for (int i = 0; i < res.size(); i++) {
            tablets.add(res[i][0])
        }

        for (int i = 0; i < tablets.size(); i++) {
            def countResult = sql "select count() from ${table} tablet(${tablets[i]})"
            rowCounts[i] = countResult[0][0]
        }

        return [tablets: tablets, rowCounts: rowCounts]
    }

    // Load 1: 5 rows with threshold=10 (should go to first tablet)
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'random_tablet_switching_threshold', '10'
        file 'test_random_tablet_switching_threshold.json'
        time 20000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(5, json.NumberTotalRows)
            assertEquals(5, json.NumberLoadedRows)
        }
    }

    sql "sync"
    def totalCount = sql "select count() from ${tableName}"
    assertEquals(5, totalCount[0][0])

    def distribution1 = getTabletDistribution(tableName)
    def activeTablet1 = -1
    for (int i = 0; i < distribution1.rowCounts.size(); i++) {
        if (distribution1.rowCounts[i] > 0) {
            activeTablet1 = i
            break
        }
    }
    assertTrue(activeTablet1 >= 0)
    assertEquals(5, distribution1.rowCounts[activeTablet1])

    // Verify other tablets are empty
    for (int i = 0; i < distribution1.rowCounts.size(); i++) {
        if (i != activeTablet1) {
            assertEquals(0, distribution1.rowCounts[i])
        }
    }

    // Load 2: Another 5 rows with threshold=10 (total 10, should stay in same tablet)
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'random_tablet_switching_threshold', '10'
        file 'test_random_tablet_switching_threshold.json'
        time 20000
    }

    sql "sync"
    totalCount = sql "select count() from ${tableName}"
    assertEquals(10, totalCount[0][0])

    def distribution2 = getTabletDistribution(tableName)

    assertEquals(10, distribution2.rowCounts[activeTablet1])
    // Other tablets should still be empty
    for (int i = 0; i < distribution2.rowCounts.size(); i++) {
        if (i != activeTablet1) {
            assertEquals(0, distribution2.rowCounts[i])
        }
    }

    // Load 3: Another 5 rows with threshold=10 (total 15, should switch to next tablet)
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'random_tablet_switching_threshold', '10'
        file 'test_random_tablet_switching_threshold.json'
        time 20000
    }

    sql "sync"
    totalCount = sql "select count() from ${tableName}"
    assertEquals(15, totalCount[0][0])

    def distribution3 = getTabletDistribution(tableName)
    def nextTablet = (activeTablet1 + 1) % distribution3.rowCounts.size()

    assertEquals(10, distribution3.rowCounts[activeTablet1])
    assertEquals(5, distribution3.rowCounts[nextTablet])
    for (int i = 0; i < distribution3.rowCounts.size(); i++) {
        if (i != activeTablet1 && i != nextTablet) {
            assertEquals(0, distribution3.rowCounts[i])
        }
    }

    // Test 2: Different threshold value
    def tableName2 = "test_random_tablet_switching_threshold_2"
    sql """ DROP TABLE IF EXISTS ${tableName2} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName2} (
          `k1` date NULL,
          `k2` text NULL,
          `k3` char(50) NULL,
          `k4` varchar(200) NULL,
          `k5` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Load with threshold=3 (should switch after first load since we load 5 rows)
    streamLoad {
        table "${tableName2}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'random_tablet_switching_threshold', '3'
        file 'test_random_tablet_switching_threshold.json'
        time 20000
    }

    sql "sync"
    totalCount = sql "select count() from ${tableName2}"
    assertEquals(5, totalCount[0][0])

    def distribution3 = getTabletDistribution(tableName2)
    // Should have switched to second tablet since threshold=3 and we loaded 5 rows
    assertTrue(distribution3.rowCounts[0] == 5 && distribution3.rowCounts[1] == 0 ||
               distribution3.rowCounts[0] == 0 && distribution3.rowCounts[1] == 5)

    // Test 3: Partitioned table with threshold
    def tableName3 = "test_random_tablet_switching_threshold_partitioned"
    sql """ DROP TABLE IF EXISTS ${tableName3} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName3} (
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
        PARTITION p20231012 VALUES [('2023-10-12'), ('2023-10-13')))
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    streamLoad {
        table "${tableName3}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'random_tablet_switching_threshold', '3'
        file 'test_random_tablet_switching_threshold.json'
        time 20000
    }

    sql "sync"
    totalCount = sql "select count() from ${tableName3}"
    assertEquals(5, totalCount[0][0])

    // Check partition-level distribution - all data should be in p20231011 partition
    def partitionCount1 = sql "select count() from ${tableName3} partition(p20231011)"
    def partitionCount2 = sql "select count() from ${tableName3} partition(p20231012)"
    assertEquals(5, partitionCount1[0][0])
    assertEquals(0, partitionCount2[0][0])

    // Test 4: No threshold specified (should use default BE config)
    def tableName4 = "test_random_tablet_switching_threshold_default"
    sql """ DROP TABLE IF EXISTS ${tableName4} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName4} (
          `k1` date NULL,
          `k2` text NULL,
          `k3` char(50) NULL,
          `k4` varchar(200) NULL,
          `k5` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Load without threshold (should use BE config default: 10000000)
    streamLoad {
        table "${tableName4}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        file 'test_random_tablet_switching_threshold.json'
        time 20000
    }

    sql "sync"
    totalCount = sql "select count() from ${tableName4}"
    assertEquals(5, totalCount[0][0])

    def distribution4 = getTabletDistribution(tableName4)
    // With default high threshold, should not switch
    assertTrue(distribution4.rowCounts[0] == 5 && distribution4.rowCounts[1] == 0 ||
               distribution4.rowCounts[0] == 0 && distribution4.rowCounts[1] == 5)

    // Test 5: Invalid threshold (should fail gracefully or use default)
    try {
        streamLoad {
            table "${tableName4}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'random_tablet_switching_threshold', 'invalid_value'
            file 'test_random_tablet_switching_threshold.json'
            time 20000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    // Expected to fail with invalid threshold
                    assertTrue(exception.getMessage().contains("Invalid random_tablet_switching_threshold"))
                    return
                }
                // If not failed, should use default behavior
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Invalid random_tablet_switching_threshold"))
    }

    // Test 6: Zero threshold (should use original round-robin behavior)
    def tableName5 = "test_random_tablet_switching_threshold_zero"
    sql """ DROP TABLE IF EXISTS ${tableName5} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName5} (
          `k1` date NULL,
          `k2` text NULL,
          `k3` char(50) NULL,
          `k4` varchar(200) NULL,
          `k5` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    streamLoad {
        table "${tableName5}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'random_tablet_switching_threshold', '0'
        file 'test_random_tablet_switching_threshold.json'
        time 20000
    }

    sql "sync"
    totalCount = sql "select count() from ${tableName5}"
    assertEquals(5, totalCount[0][0])

    // Load again to see round-robin behavior
    streamLoad {
        table "${tableName5}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'random_tablet_switching_threshold', '0'
        file 'test_random_tablet_switching_threshold.json'
        time 20000
    }

    sql "sync"
    totalCount = sql "select count() from ${tableName5}"
    assertEquals(10, totalCount[0][0])

    def distribution5 = getTabletDistribution(tableName5)
    assertTrue(distribution5.rowCounts[0] > 0 && distribution5.rowCounts[1] > 0)
    assertEquals(10, distribution5.rowCounts[0] + distribution5.rowCounts[1])

}
