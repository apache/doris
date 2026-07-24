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

suite("insert_select_table_stats_bootstrap", "nonConcurrent") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"

    sql "set enable_nereids_planner = true"
    sql "set enable_fallback_to_original_planner = false"
    // Disable the distributed planner path so this case falls back to regular join-side selection.
    sql "set enable_nereids_distribute_planner = false"
    // Disable bucket shuffle join so the optimizer has to choose between broadcast and regular shuffle.
    sql "set enable_bucket_shuffle_join = false"
    sql "set runtime_filter_mode = OFF"
    sql "set broadcast_row_count_limit = 100"
    sql "set broadcast_hashtable_mem_limit_percentage = 1"

    sql "drop table if exists smallb"
    sql """
        create table smallb (
            k1 int,
            k2 int,
            v int
        )
        distributed by hash(v) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into smallb
        select number, 0, number
        from numbers("number" = "10")
    """
    // Analyze the known small table so the join-side decision can depend on biga bootstrap stats.
    sql "analyze table smallb with sync"

    def createBigATable = { boolean enableBootstrap ->
        sql "drop table if exists biga"
        sql "set enable_insert_select_table_stats_bootstrap = ${enableBootstrap}"
        sql """
            create table biga
            distributed by hash(k) buckets 1
            properties("replication_num" = "1")
            as
            select number % 10 as k, repeat('x', 64) as pad
            from numbers("number" = "262144")
        """
    }

    // Bootstrap disabled: show table stats should be empty and scan row count should be unknown.
    // Retry up to 10 times to tolerate rare cases where TabletStatMgr publishes BE row counts.
    boolean bootstrapOffPassed = false
    for (int retry = 0; retry < 10 && !bootstrapOffPassed; retry++) {
        createBigATable(false)
        try {
            def tableStatsWithoutBootstrap = sql "show table stats biga"
            assertEquals(1, tableStatsWithoutBootstrap.size())
            assertEquals("", tableStatsWithoutBootstrap[0][0])
            assertEquals("", tableStatsWithoutBootstrap[0][3])
            assertEquals("", tableStatsWithoutBootstrap[0][5])

            explain {
                sql """
                    physical plan
                    select a.k, b.v
                    from smallb b
                    join biga a
                    on cast(a.k as bigint) = cast(b.k1 + b.k2 as bigint)
                """
                contains("table=biga")
                // Without bootstrap, the optimizer should see the scan row count as 1 (unknown).
                contains("stats=1")
                contains("distributionSpec=DistributionSpecReplicated")
                check { explainStr ->
                    // biga (stats=1) should be the broadcast side because 1 < smallb's 10 rows.
                    assertTrue((explainStr =~ /DistributionSpecReplicated[\s\S]*table=biga/).find())
                }
            }
            bootstrapOffPassed = true
        } catch (Throwable t) {
            if (retry == 9) {
                throw t
            }
            logger.info("Bootstrap-off check attempt ${retry + 1} failed, retrying...", t)
        }
    }

    // Bootstrap enabled: show table stats should reflect the inserted row count,
    // and the optimizer should broadcast the known small table (smallb).
    createBigATable(true)

    def tableStatsWithBootstrap = sql "show table stats biga"
    assertEquals(1, tableStatsWithBootstrap.size())
    assertEquals("262144", tableStatsWithBootstrap[0][0])
    assertEquals("262144", tableStatsWithBootstrap[0][2])
    assertEquals("false", tableStatsWithBootstrap[0][7])

    explain {
        sql """
            physical plan
            select a.k, b.v
            from smallb b
            join biga a
            on cast(a.k as bigint) = cast(b.k1 + b.k2 as bigint)
        """
        contains("table=biga")
        contains("table=smallb")
        // With bootstrap stats, the scan row count for biga should reflect the inserted row count.
        contains("stats=262,144")
        contains("distributionSpec=DistributionSpecReplicated")
        check { explainStr ->
            // smallb should be the broadcast (build) side because it has fewer rows than biga.
            assertTrue((explainStr =~ /DistributionSpecReplicated[\s\S]*table=smallb/).find())
        }
    }

    // Verify that bootstrap stats do not interfere with subsequent manual analyze.
    // After analyze completes, column-level stats should be available and the plan should remain correct.
    sql "analyze table biga with sync"

    def tableStatsAfterAnalyze = sql "show table stats biga"
    assertEquals(1, tableStatsAfterAnalyze.size())
    // Analyze should produce column-level stats for the table; the columns field (index 4)
    // should list column names such as 'biga.k'.
    assertTrue(tableStatsAfterAnalyze[0][4].toString().contains("biga"))
    // trigger (index 5) should be populated (e.g. MANUAL).
    assertTrue(!tableStatsAfterAnalyze[0][5].toString().isEmpty())
    assertEquals("false", tableStatsAfterAnalyze[0][7])       // user_injected (index 7)

    // Ensure the optimizer still correctly uses the row count from full stats.
    explain {
        sql """
            physical plan
            select a.k, b.v
            from smallb b
            join biga a
            on cast(a.k as bigint) = cast(b.k1 + b.k2 as bigint)
        """
        contains("table=biga")
        contains("table=smallb")
        contains("stats=262,144")
        contains("distributionSpec=DistributionSpecReplicated")
        check { explainStr ->
            // smallb should still be the broadcast side.
            assertTrue((explainStr =~ /DistributionSpecReplicated[\s\S]*table=smallb/).find())
        }
    }
}
