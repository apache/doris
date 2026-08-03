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

// Reading fluss primary-key tables end to end. What separates this from the log
// suite is that the answer is not what the fixture wrote but what the writes add
// up to: a fluss primary-key table stores a change log holding every intermediate
// state, and the read has to merge a kv snapshot with the log that followed it.
// Returning the change log instead would show up here as extra rows that each look
// entirely plausible.
//
// The part only this suite can cover is where those snapshot files come from: the
// fluss container writes them and Doris BE reads them from the host filesystem, at
// the same absolute path on both sides. Startup does not report the environment
// ready until every primary-key fixture has a snapshot on disk (see the docker
// README), so these queries take that path rather than replaying the change log.
//
// Fixtures come from docker/thirdparties/docker-compose/fluss/sql/init.sql and are
// static - this suite never writes, so no polling gate is needed. Results are
// recorded into the .out baseline; what stays in the code is what is not a result:
// the EXPLAIN anchors saying how the scan was planned, and range-count bounds,
// which cannot be exact because key-to-bucket hashing is fluss's business.
suite("test_fluss_pk_table", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String coordinatorPort = context.config.otherConfigs.get("fluss_coordinator_port")
    String bootstrapServers = "${externalEnvIp}:${coordinatorPort}"
    String catalogName = "test_fluss_pk_table"

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}"
        );
    """
    sql """switch ${catalogName}"""
    sql """use fluss_test"""

    // The connector is wired into the v2 file scanner only; the legacy one answers
    // "Not supported create reader for table format: fluss". Fuzzy sessions randomize
    // this variable, so pinning it is what keeps the suite from failing on half the
    // CI runs for a reason that has nothing to do with fluss.
    sql """set enable_file_scanner_v2 = true"""
    // TIMESTAMP_LTZ renders through the session time zone, and the baseline below
    // records what it rendered as. Without pinning it, the recorded value would be
    // whatever the machine that generated the baseline happened to be set to.
    sql """set time_zone = 'Asia/Shanghai'"""

    def planOf = { String query ->
        def planRows = sql("""explain ${query}""")
        return planRows.collect { it[0].toString() }.join("\n")
    }
    def pkRangesOf = { String plan ->
        def matcher = (plan =~ /pkRanges=(\d+)/)
        assertTrue(matcher.find(), "plan has no flussScan line: ${plan}")
        return matcher.group(1) as int
    }

    // --- the merged view ----------------------------------------------------
    // The fixture inserts four rows, updates one and deletes one. Four writes, three
    // rows: id 2 carries its later value and id 3 is gone. Reading the change log
    // straight through would answer six rows here, all of them real records.
    order_qt_basic_all """select id, name, score from pk_basic"""

    // COUNT(*) projects no column at all. On a primary-key table the count still has
    // to be the merged one: counting change log records would report six.
    order_qt_basic_count """select count(*) from pk_basic"""

    // The deleted key must be absent, not merely superseded; the updated key must
    // appear once, not once per version, and never under its old value.
    order_qt_basic_deleted_key """select count(*) from pk_basic where id = 3"""
    order_qt_basic_updated_key """select count(*) from pk_basic where id = 2"""
    order_qt_basic_stale_value """select count(*) from pk_basic where name = 'k2'"""

    // --- projection and predicates -----------------------------------------
    // The merge needs the primary key even when the query does not ask for it, so the
    // reader adds it to what it fetches and projects it back out. A leaked key column
    // or a fetch-order projection shows up as the wrong values here.
    order_qt_basic_names """select name from pk_basic"""

    order_qt_basic_reordered """select score, name from pk_basic where id = 1"""

    order_qt_basic_filtered """select count(*) from pk_basic where score > 2.0"""

    // --- every mapped type, in the kv row format ---------------------------
    // Primary-key tables store rows in a different format from a log table's, so this
    // repeats the type coverage rather than trusting the log suite for it. Both rows
    // are recorded whole -- the populated one and the all-NULL one, where a null map
    // off by one column shifts every value after it.
    //
    // BINARY and BYTES go through hex(): they map to a Doris string, and a raw byte
    // in a recorded baseline is neither readable nor safely round-tripped.
    order_qt_types_all """
        select id, f_boolean, f_tinyint, f_smallint, f_int, f_bigint, f_float, f_double,
               f_decimal, f_char, f_string, hex(f_binary) as f_binary_hex,
               hex(f_bytes) as f_bytes_hex, f_date, f_timestamp, f_timestamp_ltz,
               f_array, f_map, f_row
        from pk_types
    """

    // The nested readers, asked for one element at a time: a struct that decoded into
    // the right shape but the wrong field order still renders plausibly above.
    order_qt_types_nested """
        select id, array_size(f_array), f_array[1], f_map['k1'], f_map['k2'],
               struct_element(f_row, 'r_int'), struct_element(f_row, 'r_string')
        from pk_types
    """

    // --- partitioned primary-key table --------------------------------------
    // A partitioned primary-key table is snapshotted per partition, so its snapshots
    // have to be asked for per partition too; asking at table level resumes the change
    // log at another partition's offset. The partition column itself is not read by the
    // scanner - FE declares it and BE fills it in from each range - so recording it
    // next to the row it belongs to is what catches a partition value on the wrong split.
    order_qt_part_all """select id, name, dt from pk_part"""

    // The delete landed in 20260102 and the update in 20260101: a merge that crossed
    // partitions would lose or resurrect one of them.
    order_qt_part_grouped """select dt, count(*) from pk_part group by dt"""

    order_qt_part_pruned """select id, name from pk_part where dt = '20260101'"""

    // --- planning is visible in the plan ------------------------------------
    def basicPlan = planOf("""select * from pk_basic""")
    assertTrue(basicPlan.contains("flussScan: unionRead=no"),
            "no fluss scan line: ${basicPlan}")
    assertTrue(basicPlan.contains("lakeSplits=0"), "unexpected lake splits: ${basicPlan}")
    assertTrue(basicPlan.contains("mode=auto"), "unexpected union read mode: ${basicPlan}")
    // A primary-key table produces primary-key ranges and no log ranges. Counting them
    // together would hide a table planned as a raw log read, which returns superseded
    // rows and otherwise looks like a working query.
    assertTrue(basicPlan.contains("logRanges=0"),
            "a primary-key table was planned as a log read: ${basicPlan}")
    // One range per bucket that holds anything. Which of the three buckets a key hashes
    // into is fluss's choice, so the count is bounded rather than fixed.
    def basicRanges = pkRangesOf(basicPlan)
    assertTrue(basicRanges >= 1 && basicRanges <= 3,
            "pk_basic planned ${basicRanges} ranges over 3 buckets")

    def fullPartPlan = planOf("""select * from pk_part""")
    assertTrue(fullPartPlan.contains("partition=2/2"),
            "both partitions should be scanned: ${fullPartPlan}")

    def prunedPlan = planOf("""select * from pk_part where dt = '20260101'""")
    assertTrue(prunedPlan.contains("partition=1/2"),
            "partition pruning did not reach the connector: ${prunedPlan}")
    // Pruning has to shrink the work, not just the plan line: a partition name rendered
    // one way for the listing and another way for the match would prune to nothing here
    // while still reporting 1/2.
    def prunedRanges = pkRangesOf(prunedPlan)
    assertTrue(prunedRanges >= 1 && prunedRanges <= 2,
            "one partition has 2 buckets, planned ${prunedRanges} ranges: ${prunedPlan}")
    assertTrue(prunedRanges <= pkRangesOf(fullPartPlan),
            "pruning planned more ranges than the full scan: ${prunedPlan}")

    sql """switch internal"""
    sql """drop catalog ${catalogName}"""
}
