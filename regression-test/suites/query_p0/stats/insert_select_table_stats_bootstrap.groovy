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
    // Analyze the known small table first so the join-side change mainly depends on biga bootstrap stats.
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

    createBigATable(false)

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
        contains("PhysicalOlapScan[biga]")
        contains("distributionSpec=DistributionSpecReplicated")
        check { explainStr ->
            assertTrue((explainStr =~ /PhysicalOlapScan\[biga\][^\n]*stats=1(?![,\d.])/).find())
            assertTrue((explainStr =~ /DistributionSpecReplicated[\s\S]*PhysicalOlapScan\[biga\]/).find())
        }
    }

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
        contains("PhysicalOlapScan[biga]")
        contains("PhysicalOlapScan[smallb]")
        contains("distributionSpec=DistributionSpecReplicated")
        check { explainStr ->
            assertTrue((explainStr =~ /PhysicalOlapScan\[biga\][^\n]*stats=262,144/).find())
            assertTrue((explainStr =~ /DistributionSpecReplicated[\s\S]*PhysicalOlapScan\[smallb\]/).find())
        }
    }
}
