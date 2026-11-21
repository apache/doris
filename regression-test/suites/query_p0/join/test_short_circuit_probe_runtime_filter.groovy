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

import org.apache.doris.regression.action.ProfileAction

// With a single build row, one of the two join instances has an empty build side and its probe
// short-circuits. That probe must not finish before the build sink publishes its runtime filters:
// otherwise the build task is woken up early, terminate() publishes its filters as DISABLED and
// the merged global filter is disabled for every probe-side scan.
//
// The debug point makes a sink whose probe short-circuits sleep at eos. When every build instance
// is empty, all probes short-circuit, so without the fix the probe pipeline closes during the sleep
// and the filters are disabled on every run.
suite("test_short_circuit_probe_runtime_filter", "nonConcurrent") {
    sql "set disable_join_reorder=true"
    sql "set parallel_pipeline_task_num=2"
    sql "set enable_runtime_filter_prune=false"
    sql "set runtime_filter_type='IN_OR_BLOOM_FILTER,MIN_MAX'"
    sql "set runtime_filter_max_in_num=1024"
    // A probe held back is released after the runtime filter wait time at the latest, so keep it
    // longer than the sleep of the debug point.
    sql "set runtime_filter_wait_time_ms=10000"
    sql "set enable_profile=true"
    sql "set profile_level=2"

    sql "drop table if exists sc_probe_rf_probe"
    sql "drop table if exists sc_probe_rf_build"
    sql """
        create table sc_probe_rf_probe (
            k int null,
            s varchar(32) null
        )
        duplicate key (k)
        distributed by hash(k) buckets 8
        properties("replication_num" = "1")
    """
    sql """
        create table sc_probe_rf_build (
            k int null,
            s varchar(32) null
        )
        duplicate key (k)
        distributed by hash(k) buckets 8
        properties("replication_num" = "1")
    """
    sql """
        insert into sc_probe_rf_probe
        select e, cast(e as varchar(32)) from (select 1 k) t lateral view explode_numbers(10000) tmp as e
    """
    sql "insert into sc_probe_rf_probe values (null, null)"
    sql "insert into sc_probe_rf_build values (7, '7')"

    def profileAction = new ProfileAction(context)
    // The spill operators may not merge the inner build sink's profile, so only wait for the
    // build sink's runtime filter info without spill.
    def requiredContents = []
    def check = { String tag, String joinType, String condition, String buildFilter ->
        // The token only marks the statement in the profile list, so it stays out of the result.
        def token = UUID.randomUUID().toString()
        "qt_${tag}"("""
            /* ${token} */ select count(*)
            from sc_probe_rf_probe p ${joinType} join [shuffle] sc_probe_rf_build b on ${condition}
            ${buildFilter}
        """)
        def profile = profileAction.getProfileBySql(token, requiredContents)
        assertTrue(profile != "", "Profile not found for ${token}")
        def description = "${joinType} join on ${condition} ${buildFilter}"
        assertFalse((profile =~ /SkipProcess:\s*True/).find(),
                "runtime filters of a finished build were disabled: ${description}")
        assertFalse(profile.contains("get disabled from remote"),
                "a probe-side scan got a disabled runtime filter: ${description}")
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs(
                "HashJoinBuildSinkOperatorX::sink_impl.short_circuit_eos_sleep", [sleep_ms: 2000])
        for (def enableSpill : [false, true]) {
            sql "set enable_spill=${enableSpill}"
            sql "set enable_force_spill=false"
            requiredContents = enableSpill ? [] : ["SkipProcess"]
            for (def syncSize : [true, false]) {
                sql "set enable_sync_runtime_filter_size=${syncSize}"
                def suffix = "_spill_${enableSpill}_sync_${syncSize}"
                check("inner_k${suffix}", "inner", "p.k = b.k", "")
                check("right_outer_k${suffix}", "right outer", "p.k = b.k", "")
                check("inner_s${suffix}", "inner", "p.s = b.s", "")
                check("right_outer_ks${suffix}", "right outer", "p.k = b.k and p.s = b.s", "")
                // Every build instance is empty.
                check("inner_all_empty${suffix}", "inner", "p.k = b.k", "where b.k < 0")
            }
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(
                "HashJoinBuildSinkOperatorX::sink_impl.short_circuit_eos_sleep")
    }
}
