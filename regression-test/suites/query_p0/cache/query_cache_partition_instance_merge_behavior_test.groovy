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

// ===================================================================================
// Regression test for merging scan instances by partition under Query Cache
//
// Core validation covers two things:
//   1. When total_tablets is clearly greater than parallel_pipeline_task_num * worker_num,
//      FE merges tablets from the same BE and same partition into one instance.
//   2. When the threshold is not met, planning falls back to the default tablet-based split,
//      where each instance scans only one tablet.
//
// Note:
//   - This no longer asserts "instance count = partition count". The real implementation is
//     "one partition on one BE maps to one instance"; the same partition on different BEs
//     may still map to multiple instances.
//   - Fallback for incomplete partition mapping is covered by FE unit tests; this regression only
//     covers the stable E2E positive path.
// ===================================================================================

suite("query_cache_partition_instance_merge_behavior_test") {
    // Set all session variables required by this case in one place.
    // This centralizes all switches that affect planner behavior, cache hits, and profile output,
    // so the two scenarios below only need to focus on the two key variables: parallelism and whether
    // query cache is enabled.
    def setSessionVariables = { int parallelPipelineTaskNum, boolean enableQueryCache ->
        // Force Nereids + distributed planner to avoid plan text changes after falling back to the legacy planner.
        sql "set enable_nereids_planner=true"
        sql "set enable_fallback_to_original_planner=false"
        sql "set enable_nereids_distribute_planner=true"
        // Explicitly disable SQL Cache to avoid mixing query cache behavior with SQL Cache.
        sql "set enable_sql_cache=false"
        sql "set enable_query_cache=${enableQueryCache}"
        // Allow query cache to reuse existing cache normally instead of force-refreshing every time.
        sql "set query_cache_force_refresh=false"
        // This parameter directly affects the scan instance split threshold and is the core control knob of this case.
        sql "set parallel_pipeline_task_num=${parallelPipelineTaskNum}"
        // Fix the exchange instance count to make explain / distributed plan more stable and easier to assert.
        sql "set parallel_exchange_instance_num=1"
        // Enable profile with a detailed enough level to expose fields such as HitCache / InsertCache.
        sql "set enable_profile=true"
        sql "set profile_level=2"
    }

    // First execution of a query with query cache:
    // It is expected to miss, but writes this result into cache.
    // This assertion verifies that cache can be built correctly.
    def assertMissAndInsertCache = { String sqlStr ->
        // Bind the profile with a unique tag to avoid fetching profile results from other statements
        // with the same query text.
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                // Profile writing is asynchronous, so waiting before reading reduces flaky failures.
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                if (!exception.is(null)) {
                    logger.error("Profile failed, profile result:\n${profileString}", exception)
                    throw exception
                }
                logger.info("profileString: " + profileString)
                // The first execution must miss and must not hit historical cache.
                assertTrue(profileString.contains("HitCache:  0"))
                assertFalse(profileString.contains("HitCache:  1"))
                // After the miss, the result should be written into cache.
                assertTrue(profileString.contains("InsertCache:  1"))
                // CacheTabletId in the profile means this query did enter query cache logic.
                assertTrue(profileString.contains("CacheTabletId"))
            }
        }
    }

    // Second execution of the same query:
    // It is expected to hit the cache just inserted, verifying that cache can be reused correctly.
    def assertHasCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                if (!exception.is(null)) {
                    logger.error("Profile failed, profile result:\n${profileString}", exception)
                    throw exception
                }
                logger.info("profileString: " + profileString)
                assertTrue(profileString.contains("HitCache:  1"))
                assertFalse(profileString.contains("HitCache:  0"))
                // On a hit, the profile still records the exact hit tablet information for later troubleshooting.
                assertTrue(profileString.contains("CacheTabletId"))
            }
        }
    }

    // Expose a combined assertion closer to business semantics:
    // first miss and write cache, then hit the same cache.
    def assertMissThenHit = { String sqlStr ->
        assertMissAndInsertCache(sqlStr)
        assertHasCache(sqlStr)
    }

    // Result comparison does not depend on row order; rows are converted to strings and sorted before comparison.
    def normalizeRows = { rows ->
        return rows.collect { row -> row.collect { col -> String.valueOf(col) }.join("|") }.sort()
    }

    // Query cache plans should contain DIGEST;
    // this is the key marker that query cache can perform query canonicalization.
    def assertExplainHasDigest = { String sqlStr ->
        explain {
            sql sqlStr
            contains("DIGEST")
        }
    }

    // EXPLAIN DISTRIBUTED PLAN returns a multi-row, multi-column list,
    // so join it into one string to make later slicing and regex parsing easier.
    def getDistributedPlan = { String sqlStr ->
        def distributedRows = sql("EXPLAIN DISTRIBUTED PLAN ${sqlStr}")
        return distributedRows.collect { it[0].toString() }.join("\n")
    }

    // The whole distributed plan may contain multiple fragmentJobs.
    // Only the scan fragment for the target table matters, so slice from
    // "UnassignedScanSingleOlapTableJob" until the next fragmentJob.
    def getScanFragment = { String distributedPlan ->
        int scanFragmentBegin = distributedPlan.indexOf("fragmentJob: UnassignedScanSingleOlapTableJob")
        assertTrue(scanFragmentBegin >= 0)

        String fragmentTail = distributedPlan.substring(scanFragmentBegin)
        def fragmentMatcher = (fragmentTail =~ /(?m)^\s*fragmentJob:/)
        assertTrue(fragmentMatcher.find())
        if (fragmentMatcher.find()) {
            return fragmentTail.substring(0, fragmentMatcher.start())
        }
        return fragmentTail
    }

    // Extract three kinds of information for each scan instance from the scan fragment:
    //   1. instanceId: the unique instance identifier assigned by the planner
    //   2. workerId: which BE this instance will run on
    //   3. tablets: which tablets this instance scans
    //
    // All later checks about whether merging happened are based on these three fields.
    def parseScanInstances = { String scanFragment ->
        def scanInstances = []
        def currentInstance = null
        scanFragment.eachLine { line ->
            String trimmed = line.trim()
            // Each StaticAssignedJob means a new scan instance starts being parsed.
            if (trimmed.startsWith("StaticAssignedJob(")) {
                if (currentInstance != null) {
                    scanInstances << currentInstance
                }
                currentInstance = [instanceId: null, workerId: null, tablets: []]
                return
            }
            if (currentInstance == null) {
                return
            }

            // The description for one instance may span multiple lines, so fill fields line by line.
            def instanceMatcher = (trimmed =~ /instanceId:\s*([0-9a-f\-]+)/)
            if (instanceMatcher.find()) {
                currentInstance.instanceId = instanceMatcher.group(1)
            }

            def workerMatcher = (trimmed =~ /worker:\s*BackendWorker\(id:\s*(\d+),/)
            if (workerMatcher.find()) {
                currentInstance.workerId = workerMatcher.group(1)
            }

            def tabletMatcher = (trimmed =~ /tablet\s+(\d+)/)
            if (tabletMatcher.find()) {
                currentInstance.tablets << tabletMatcher.group(1)
            }
        }
        if (currentInstance != null) {
            scanInstances << currentInstance
        }
        return scanInstances
    }

    // Read real metadata to build the ground-truth "partition -> tablet list" mapping.
    // Do not infer partitions from explain text; use SHOW TABLETS directly to get the authoritative mapping.
    def getPartitionToTablets = { String tableName, partitionNames ->
        def partitionToTablets = [:]
        partitionNames.each { partitionName ->
            def tabletRows = sql("SHOW TABLETS FROM ${tableName} PARTITION(${partitionName})")
            def tabletIds = tabletRows.collect { it[0].toString() }
            assertFalse(tabletIds.isEmpty())
            partitionToTablets[partitionName] = tabletIds
        }
        return partitionToTablets
    }

    // Reverse the mapping above into "tablet -> partition" so later checks can look up partitions by tablet.
    def buildTabletToPartition = { partitionToTablets ->
        def tabletToPartition = [:]
        partitionToTablets.each { partitionName, tabletIds ->
            tabletIds.each { tabletId ->
                assertFalse(tabletToPartition.containsKey(tabletId))
                tabletToPartition[tabletId] = partitionName
            }
        }
        return tabletToPartition
    }

    // First run generic validation:
    //   1. each scan instance has a valid instanceId / workerId;
    //   2. tablets in each instance come from only one partition;
    //   3. the tablet set assigned in the plan exactly matches the real metadata.
    //
    // This assertion applies to both merge and no-merge scenarios.
    def assertScanInstancesMatchTablets = { scanInstances, tabletToPartition ->
        assertFalse(scanInstances.isEmpty())

        def assignedTablets = [] as Set
        scanInstances.each { scanInstance ->
            assertTrue(scanInstance.instanceId != null)
            assertTrue(scanInstance.workerId != null)
            assertFalse(scanInstance.tablets.isEmpty())

            assignedTablets.addAll(scanInstance.tablets)
            def partitions = scanInstance.tablets.collect { tabletId ->
                assertTrue(tabletToPartition.containsKey(tabletId))
                tabletToPartition[tabletId]
            }.toSet()
            assertEquals(1, partitions.size())
        }

        assertEquals(tabletToPartition.keySet(), assignedTablets)
    }

    // This is the core extra assertion for the merge scenario.
    //
    // After generic validation passes, it additionally verifies:
    // for the same "worker + partition" combination, there can be only one final instance;
    // this is exactly what "tablets from the same BE and same partition are merged into one instance" means.
    def assertPartitionMergedByWorker = { scanInstances, tabletToPartition ->
        assertScanInstancesMatchTablets(scanInstances, tabletToPartition)

        def workerPartitionToInstances = [:].withDefault { [] }
        scanInstances.each { scanInstance ->
            def partitions = scanInstance.tablets.collect { tabletId ->
                tabletToPartition[tabletId]
            }.toSet()
            String partitionName = partitions.iterator().next()
            workerPartitionToInstances["${scanInstance.workerId}|${partitionName}"] << scanInstance.instanceId
        }

        workerPartitionToInstances.each { _, instanceIds ->
            assertEquals(1, instanceIds.toSet().size())
        }
    }

    // For the no-merge scenario, besides verifying that the final plan is still "one tablet per instance",
    // also prove that default planning really has "multiple tablets under the same worker and same partition"
    // so this scenario truly covers the branch where merge is possible but blocked by the threshold,
    // rather than a case with no physically mergeable objects at all.
    def assertHasMergeOpportunityByWorkerPartition = { scanInstances, tabletToPartition ->
        assertFalse(scanInstances.isEmpty())

        def workerPartitionToTabletCount = [:].withDefault { 0 }
        scanInstances.each { scanInstance ->
            assertTrue(scanInstance.workerId != null)
            assertFalse(scanInstance.tablets.isEmpty())

            scanInstance.tablets.each { tabletId ->
                assertTrue(tabletToPartition.containsKey(tabletId))
                String partitionName = tabletToPartition[tabletId]
                workerPartitionToTabletCount["${scanInstance.workerId}|${partitionName}"] += 1
            }
        }

        assertTrue(workerPartitionToTabletCount.values().any { it > 1 })
    }

    // Prevent FE from delaying query cache decisions because of the "latest version visibility wait window",
    // otherwise cache behavior right after inserts may be unstable because of the time window.
    withGlobalLock("cache_last_version_interval_second") {
        setFeConfigTemporary(["cache_last_version_interval_second": "0"], {

    // First count the number of alive BEs.
    // The merge trigger condition depends on worker count, so compute it dynamically to avoid
    // environment-sensitive tests.
    int aliveBeCount = sql_return_maparray("show backends")
            .stream()
            .filter { it["Alive"].toString().equalsIgnoreCase("true") }
            .count() as int
    // The trigger scenario needs total_tablets to be large enough:
    //   total_tablets > parallel_pipeline_task_num * worker_num
    // therefore the bucket count must exceed the BE count, with a lower bound of 12 to trigger
    // stably in most environments.
    int triggerBucketCount = Math.max(aliveBeCount + 1, 12)

    // ----------------------------------------------------------------------
    // Scenario 1: construct a table that triggers instance merge by partition.
    // Three partitions plus many buckets produce many tablets.
    // Then set parallel_pipeline_task_num small to lower the threshold and force the planner into merge logic.
    // ----------------------------------------------------------------------
    sql "DROP TABLE IF EXISTS query_cache_partition_instance_merge_trigger"
    sql """
        CREATE TABLE query_cache_partition_instance_merge_trigger (
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
        DISTRIBUTED BY HASH(user_id) BUCKETS ${triggerBucketCount}
        PROPERTIES
        (
            "replication_num" = "1"
        )
    """
    sql """
        INSERT INTO query_cache_partition_instance_merge_trigger VALUES
        ('2026-01-01', 1, '/a', 10),
        ('2026-01-01', 2, '/b', 20),
        ('2026-01-02', 3, '/c', 30),
        ('2026-01-03', 4, '/d', 40),
        ('2026-01-06', 5, '/a', 15),
        ('2026-01-06', 6, '/b', 25),
        ('2026-01-07', 7, '/c', 35),
        ('2026-01-08', 8, '/d', 45),
        ('2026-01-11', 9, '/a', 50),
        ('2026-01-11', 10, '/b', 60),
        ('2026-01-12', 11, '/c', 70),
        ('2026-01-13', 12, '/d', 80)
    """
    // Wait for the load to become visible so SHOW TABLETS, query execution, and explain all see
    // the same stable metadata.
    sql "sync"

    def triggerQuery = """
        SELECT
            url,
            SUM(cost) AS total_cost
        FROM query_cache_partition_instance_merge_trigger
        WHERE dt >= '2026-01-01'
          AND dt < '2026-01-15'
        GROUP BY url
    """

    // First run with query cache disabled to get the result baseline.
    // This ensures later checks still verify that query results themselves do not change even when
    // cache behavior is correct.
    setSessionVariables(1, false)
    def triggerBaseline = normalizeRows(sql(triggerQuery))

    // After enabling query cache, verify:
    //   1. explain contains DIGEST
    //   2. the first execution misses and inserts cache
    //   3. the next execution hits cache
    //   4. query results before and after cache hits exactly match the baseline
    setSessionVariables(1, true)
    assertExplainHasDigest(triggerQuery)
    assertMissThenHit(triggerQuery)
    assertEquals(triggerBaseline, normalizeRows(sql(triggerQuery)))

    // Now verify what the plan looks like, not just whether cache can hit.
    def triggerPlan = getDistributedPlan(triggerQuery)
    assertTrue(triggerPlan.contains("UnassignedScanSingleOlapTableJob"))

    def triggerScanInstances = parseScanInstances(getScanFragment(triggerPlan))
    def triggerTabletToPartition = buildTabletToPartition(getPartitionToTablets(
            "query_cache_partition_instance_merge_trigger",
            ["p20260101", "p20260105", "p20260110"]
    ))
    assertPartitionMergedByWorker(triggerScanInstances, triggerTabletToPartition)
    // At least one instance must scan multiple tablets to prove that merging really happened.
    assertTrue(triggerScanInstances.any { it.tablets.size() > 1 })
    // After merging, the instance count should be less than the tablet count; otherwise it is still
    // "one tablet per instance".
    assertTrue(triggerScanInstances.size() < triggerTabletToPartition.size())

    // ----------------------------------------------------------------------
    // Scenario 2: construct a table with merge opportunities that still does not trigger merging.
    // Each partition has multiple buckets, and the bucket count is deliberately greater than the alive BE count,
    // so there is at least one combination with multiple tablets under the same BE and same partition,
    // meaning it physically has merge opportunities.
    //
    // At the same time, raise parallel_pipeline_task_num to at least the total tablet count,
    // making total_tablets > parallel_pipeline_task_num * worker_num false,
    // so it stably takes the default no-merge split branch.
    // ----------------------------------------------------------------------
    int noTriggerBucketCount = Math.max(aliveBeCount + 1, 4)
    int noTriggerTotalTabletCount = noTriggerBucketCount * 2
    int noTriggerParallelPipelineTaskNum = noTriggerTotalTabletCount

    sql "DROP TABLE IF EXISTS query_cache_partition_instance_merge_no_trigger"
    sql """
        CREATE TABLE query_cache_partition_instance_merge_no_trigger (
            dt DATE,
            user_id INT,
            url STRING,
            cost BIGINT
        )
        ENGINE=OLAP
        DUPLICATE KEY(dt, user_id)
        PARTITION BY RANGE(dt)
        (
            PARTITION p20260201 VALUES LESS THAN ("2026-02-03"),
            PARTITION p20260203 VALUES LESS THAN ("2026-02-05")
        )
        DISTRIBUTED BY HASH(user_id) BUCKETS ${noTriggerBucketCount}
        PROPERTIES
        (
            "replication_num" = "1"
        )
    """
    sql """
        INSERT INTO query_cache_partition_instance_merge_no_trigger VALUES
        ('2026-02-01', 1, '/a', 10),
        ('2026-02-01', 2, '/b', 20),
        ('2026-02-03', 3, '/c', 30),
        ('2026-02-04', 4, '/d', 40)
    """
    sql "sync"

    // The query still covers all partitions, but this time even though each partition has multiple tablets,
    // instance merge should not be triggered.
    def noTriggerQuery = """
        SELECT
            url,
            SUM(cost) AS total_cost
        FROM query_cache_partition_instance_merge_no_trigger
        WHERE dt >= '2026-02-01'
          AND dt < '2026-02-05'
        GROUP BY url
    """

    // Get the baseline first, then enable query cache to verify miss/hit behavior.
    // Here parallel_pipeline_task_num is set to the total tablet count:
    //   threshold = parallel_pipeline_task_num * worker_num >= total_tablets
    // Because the trigger condition uses a strict greater-than comparison, regardless of whether
    // tablets land on one or multiple BEs,
    // this scenario stably stays on the no-merge side.
    setSessionVariables(noTriggerParallelPipelineTaskNum, false)
    def noTriggerBaseline = normalizeRows(sql(noTriggerQuery))

    setSessionVariables(noTriggerParallelPipelineTaskNum, true)
    assertExplainHasDigest(noTriggerQuery)
    assertMissThenHit(noTriggerQuery)
    assertEquals(noTriggerBaseline, normalizeRows(sql(noTriggerQuery)))

    def noTriggerPlan = getDistributedPlan(noTriggerQuery)
    assertTrue(noTriggerPlan.contains("UnassignedScanSingleOlapTableJob"))

    def noTriggerScanInstances = parseScanInstances(getScanFragment(noTriggerPlan))
    def noTriggerPartitionToTablets = getPartitionToTablets(
            "query_cache_partition_instance_merge_no_trigger",
            ["p20260201", "p20260203"]
    )
    def noTriggerTabletToPartition = buildTabletToPartition(noTriggerPartitionToTablets)
    // First confirm each partition has multiple tablets; otherwise merge is not meaningful in this scenario.
    noTriggerPartitionToTablets.each { _, tabletIds ->
        assertTrue(tabletIds.size() > 1)
    }
    assertScanInstancesMatchTablets(noTriggerScanInstances, noTriggerTabletToPartition)
    // Although no merge happens finally, the default plan must have at least one
    // "same worker + same partition with multiple tablets" case.
    assertHasMergeOpportunityByWorkerPartition(noTriggerScanInstances, noTriggerTabletToPartition)
    // When merge is not triggered, the instance count should equal the tablet count.
    assertEquals(noTriggerTabletToPartition.size(), noTriggerScanInstances.size())
    noTriggerScanInstances.each { scanInstance ->
        // Each instance should scan only one tablet, returning to the default split mode.
        assertEquals(1, scanInstance.tablets.size())
    }
        })
    }
}
