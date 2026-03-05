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

suite("test_partition_instance_query_cache") {
    def tableName = "test_partition_instance"
    def querySql = """
        SELECT
            url,
            SUM(cost) AS total_cost
        FROM ${tableName}
        WHERE dt >= '2026-01-01'
          AND dt < '2026-01-15'
        GROUP BY url
    """

    sql "set enable_nereids_planner=true"
    sql "set enable_nereids_distribute_planner=true"
    sql "set enable_query_cache=true"
    sql "set parallel_pipeline_task_num=3"
    sql "set enable_sql_cache=false"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            dt DATE,
            user_id INT,
            url STRING,
            cost BIGINT
        )
        ENGINE=OLAP
        DUPLICATE KEY(dt, user_id)
        PARTITION BY RANGE(dt)
        (
            PARTITION p20260101 VALUES LESS THAN ("2026-01-05"),
            PARTITION p20260105 VALUES LESS THAN ("2026-01-10"),
            PARTITION p20260110 VALUES LESS THAN ("2026-01-15")
        )
        DISTRIBUTED BY HASH(user_id) BUCKETS 3
        PROPERTIES
        (
            "replication_num" = "1"
        )
    """

    sql """
        INSERT INTO ${tableName} VALUES
        ('2026-01-01',1,'/a',10),
        ('2026-01-01',2,'/b',20),
        ('2026-01-02',3,'/c',30),
        ('2026-01-03',4,'/d',40),

        ('2026-01-06',1,'/a',15),
        ('2026-01-06',2,'/b',25),
        ('2026-01-07',3,'/c',35),
        ('2026-01-08',4,'/d',45),

        ('2026-01-11',1,'/a',50),
        ('2026-01-11',2,'/b',60),
        ('2026-01-12',3,'/c',70),
        ('2026-01-13',4,'/d',80)
    """

    order_qt_partition_instance_query_result """
        ${querySql}
        ORDER BY url
    """

    def normalize = { rows ->
        return rows.collect { row -> row.collect { col -> String.valueOf(col) }.join("|") }.sort()
    }

    def baseline = normalize(sql(querySql))
    for (int i = 0; i < 3; i++) {
        assertEquals(baseline, normalize(sql(querySql)))
    }

    explain {
        sql(querySql)
        contains("DIGEST")
    }

    def distributedRows = sql("EXPLAIN DISTRIBUTED PLAN ${querySql}")
    def distributedPlan = distributedRows.collect { it[0].toString() }.join("\n")
    assertTrue(distributedPlan.contains("UnassignedScanSingleOlapTableJob"))

    def partitionMatcher = (distributedPlan =~ /partitions=(\d+)\/(\d+)/)
    assertTrue(partitionMatcher.find())
    int partitionCount = Integer.parseInt(partitionMatcher.group(1))

    int scanFragmentBegin = distributedPlan.indexOf("fragmentJob: UnassignedScanSingleOlapTableJob")
    assertTrue(scanFragmentBegin > 0)
    def scanFragment = distributedPlan.substring(scanFragmentBegin)

    int scanInstanceCount = (scanFragment =~ /StaticAssignedJob\(/).count
    assertEquals(partitionCount, scanInstanceCount)

    def instanceToTablets = [:].withDefault { [] }
    String currentInstance = null
    scanFragment.eachLine { line ->
        def instanceMatcher = (line =~ /instanceId:\s*([0-9a-f\-]+)/)
        if (instanceMatcher.find()) {
            currentInstance = instanceMatcher.group(1)
            instanceToTablets[currentInstance] = []
        }

        def tabletMatcher = (line =~ /tablet\s+(\d+)/)
        if (tabletMatcher.find() && currentInstance != null) {
            instanceToTablets[currentInstance] << tabletMatcher.group(1)
        }
    }

    assertEquals(partitionCount, instanceToTablets.size())
    instanceToTablets.each { _, tablets ->
        assertTrue(tablets.size() > 0)
    }

    def tabletToInstance = [:]
    instanceToTablets.each { instanceId, tablets ->
        tablets.each { tabletId ->
            tabletToInstance[tabletId] = instanceId
        }
    }

    ["p20260101", "p20260105", "p20260110"].each { partitionName ->
        def partitionTabletRows = sql("SHOW TABLETS FROM ${tableName} PARTITION(${partitionName})")
        def partitionTabletIds = partitionTabletRows.collect { it[0].toString() }
        assertTrue(partitionTabletIds.size() > 0)

        partitionTabletIds.each { tabletId ->
            assertTrue(tabletToInstance.containsKey(tabletId))
        }

        def partitionInstanceIds = partitionTabletIds.collect { tabletId -> tabletToInstance[tabletId] }.toSet()
        assertEquals(1, partitionInstanceIds.size())
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
}
