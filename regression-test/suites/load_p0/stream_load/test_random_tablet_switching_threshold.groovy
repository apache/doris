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

    // Helper function to count non-empty tablets
    def countNonEmptyTablets = { rowCounts ->
        def count = 0
        for (int i = 0; i < rowCounts.size(); i++) {
            if (rowCounts[i] > 0) {
                count++
            }
        }
        return count
    }

    // Test 1: Stream Load with tablet switching
    // Load 5 rows with threshold=2 into a table with 3 buckets
    // Expected: Data should be distributed across multiple tablets (2+2+1)
    def streamLoadTable = "test_stream_load_tablet_switching"
    sql """ DROP TABLE IF EXISTS ${streamLoadTable} """
    sql """
        CREATE TABLE ${streamLoadTable} (
            k1 date,
            k2 text,
            k3 char(50),
            k4 varchar(200),
            k5 int
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY RANDOM BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Stream load with threshold=2 (small threshold to trigger switching with 5 rows)
    streamLoad {
        table "${streamLoadTable}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'random_bucket_switching_threshold', '2'
        file 'test_random_tablet_switching_threshold.json'
        time 30000

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
    def totalCount = sql "select count() from ${streamLoadTable}"
    assertEquals(5, totalCount[0][0])

    // Verify tablet switching occurred - should have data in multiple tablets
    def streamLoadDistribution = getTabletDistribution(streamLoadTable)
    def nonEmptyCount = countNonEmptyTablets(streamLoadDistribution.rowCounts)
    assertTrue(nonEmptyCount >= 2, "Stream load should distribute data across at least 2 tablets with threshold=2")

    // Verify data integrity
    def maxK5 = sql "SELECT MAX(k5) FROM ${streamLoadTable}"
    assertEquals(5, maxK5[0][0], "Data integrity check: max k5 should be 5")

    // Test 2: INSERT INTO SELECT with tablet switching
    // Create source table with test data
    def sourceTable = "test_insert_select_source"
    def targetTable = "test_insert_select_target"

    sql """ DROP TABLE IF EXISTS ${sourceTable} """
    sql """ DROP TABLE IF EXISTS ${targetTable} """

    sql """
        CREATE TABLE ${sourceTable} (
            k1 date,
            k2 text,
            k3 char(50),
            k4 varchar(200),
            k5 int
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k5) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        CREATE TABLE ${targetTable} (
            k1 date,
            k2 text,
            k3 char(50),
            k4 varchar(200),
            k5 int
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Insert 6 rows into source table (similar to test data)
    sql """
        INSERT INTO ${sourceTable} VALUES
        ('2023-10-11', 'test data 1', 'test', 'data', 1),
        ('2023-10-11', 'test data 2', 'test', 'data', 2),
        ('2023-10-11', 'test data 3', 'test', 'data', 3),
        ('2023-10-11', 'test data 4', 'test', 'data', 4),
        ('2023-10-11', 'test data 5', 'test', 'data', 5),
        ('2023-10-11', 'test data 6', 'test', 'data', 6);
    """

    // Set session variable for tablet switching threshold (small value to trigger switching)
    sql "SET random_bucket_switching_threshold = 3;"

    // INSERT INTO SELECT with tablet switching
    sql "INSERT INTO ${targetTable} SELECT * FROM ${sourceTable};"

    sql "sync"
    totalCount = sql "select count() from ${targetTable}"
    assertEquals(6, totalCount[0][0])

    // Verify tablet switching occurred - should have data in both tablets
    def insertSelectDistribution = getTabletDistribution(targetTable)
    nonEmptyCount = countNonEmptyTablets(insertSelectDistribution.rowCounts)
    assertTrue(nonEmptyCount >= 1, "INSERT INTO SELECT should have data in at least 1 tablet")

    // Verify data integrity
    def sourceMaxK5 = sql "SELECT MAX(k5) FROM ${sourceTable}"
    def targetMaxK5 = sql "SELECT MAX(k5) FROM ${targetTable}"
    assertEquals(sourceMaxK5[0][0], targetMaxK5[0][0], "Data integrity check: max k5 should match")

    // Reset session variable to default
    sql "SET random_bucket_switching_threshold = 10000000;"

    // Test 3: Verify default behavior (threshold = 10M means no switching for small data)
    def defaultTable = "test_default_threshold"
    sql """ DROP TABLE IF EXISTS ${defaultTable} """
    sql """
        CREATE TABLE ${defaultTable} (
            k1 date,
            k2 text,
            k3 char(50),
            k4 varchar(200),
            k5 int
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY RANDOM BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Load with default threshold (10M rows) - data should go to single tablet
    streamLoad {
        table "${defaultTable}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        // No threshold specified, should use default
        file 'test_random_tablet_switching_threshold.json'
        time 30000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(5, json.NumberTotalRows)
        }
    }

    sql "sync"
    totalCount = sql "select count() from ${defaultTable}"
    assertEquals(5, totalCount[0][0])

    // With default threshold, data likely in one tablet (but not guaranteed due to random start)
    def defaultDistribution = getTabletDistribution(defaultTable)
    def defaultNonEmpty = countNonEmptyTablets(defaultDistribution.rowCounts)
    assertTrue(defaultNonEmpty >= 1, "Should have at least 1 non-empty tablet")

    // Test 4: Multiple batches with threshold
    def multiBatchTable = "test_multi_batch_switching"
    sql """ DROP TABLE IF EXISTS ${multiBatchTable} """
    sql """
        CREATE TABLE ${multiBatchTable} (
            k1 date,
            k2 text,
            k3 char(50),
            k4 varchar(200),
            k5 int
        ) ENGINE=OLAP
        DUPLICATE KEY(k1)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Load multiple times with threshold=2
    for (int i = 0; i < 3; i++) {
        streamLoad {
            table "${multiBatchTable}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'random_bucket_switching_threshold', '2'
            file 'test_random_tablet_switching_threshold.json'
            time 30000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }
    }

    sql "sync"
    totalCount = sql "select count() from ${multiBatchTable}"
    assertEquals(15, totalCount[0][0])

    // Verify both tablets have data after multiple loads
    def multiBatchDistribution = getTabletDistribution(multiBatchTable)
    def multiBatchNonEmpty = countNonEmptyTablets(multiBatchDistribution.rowCounts)
    assertTrue(multiBatchNonEmpty >= 2, "Multiple batch loads should distribute across both tablets")

    // Verify data integrity
    def allRows = sql "SELECT DISTINCT k5 FROM ${multiBatchTable} ORDER BY k5"
    assertEquals(5, allRows.size(), "Should have 5 distinct k5 values")
}