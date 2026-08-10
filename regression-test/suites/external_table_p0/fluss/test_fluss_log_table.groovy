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

// Reading fluss log tables end to end: FE planning, the untyped payload it ships,
// the BE java scanner and the partition columns BE materializes from each range.
// Fixtures come from docker/thirdparties/docker-compose/fluss/sql/init.sql and are
// static - this suite never writes, so no polling gate is needed.
//
// Results are recorded into the .out baseline; only what is NOT a result stays as
// an assertion in the code -- the EXPLAIN anchors, which say how the scan was
// planned, and the bounds on range counts, which cannot be exact because a fluss
// writer chooses its own buckets.
suite("test_fluss_log_table", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String coordinatorPort = context.config.otherConfigs.get("fluss_coordinator_port")
    String bootstrapServers = "${externalEnvIp}:${coordinatorPort}"
    String catalogName = "test_fluss_log_table"

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
    def logRangesOf = { String plan ->
        def matcher = (plan =~ /logRanges=(\d+)/)
        assertTrue(matcher.find(), "plan has no flussScan line: ${plan}")
        return matcher.group(1) as int
    }

    // --- the whole table ----------------------------------------------------
    order_qt_basic_all """select id, name, price from log_basic"""

    // COUNT(*) projects no column at all: the scanner has to report how many rows it
    // read without returning one. A scanner that answered with an empty batch instead
    // would make an untouched-looking table out of a populated one.
    order_qt_basic_count """select count(*) from log_basic"""

    // --- projection and predicates -----------------------------------------
    order_qt_basic_names """select name from log_basic"""

    // Columns asked for in an order other than the table's: the scanner resolves the
    // projection by name, so a positional shortcut anywhere would swap these two.
    order_qt_basic_swapped """select name, id from log_basic where id = 2"""

    order_qt_basic_filtered """select id from log_basic where price > 15.00"""
    order_qt_basic_count_filtered """select count(*) from log_basic where name = 'bob'"""

    // --- every mapped type survives the round trip --------------------------
    // The three rows are recorded whole: the positive one, the negative one (a sign
    // lost in decoding shows up nowhere else) and the all-NULL one (read through a
    // null map that is off by one column, every value after it shifts). Recording
    // them pins the rendering of map, struct and decimal as well, which is more than
    // the decoding this suite is about -- a rendering change will land here and has
    // to be re-recorded deliberately.
    //
    // BINARY and BYTES go through hex(): they map to a Doris string, and a raw byte
    // in a recorded baseline is neither readable nor safely round-tripped.
    //
    // The MAP goes in as its sorted keys and sorted values rather than whole. The order
    // a map renders its entries in is neither the order they were written in nor stable
    // across runs -- two tables written by the same statement have rendered the same two
    // keys in different orders -- so a recorded whole-map cell pins something no reader
    // promises. Which value belongs to which key is pinned by the lookups below.
    order_qt_types_all """
        select id, f_boolean, f_tinyint, f_smallint, f_int, f_bigint, f_float, f_double,
               f_decimal, f_char, f_string, hex(f_binary) as f_binary_hex,
               hex(f_bytes) as f_bytes_hex, f_date, f_timestamp, f_timestamp_ltz,
               f_array, array_sort(map_keys(f_map)) as f_map_keys,
               array_sort(map_values(f_map)) as f_map_values, f_row
        from log_types
    """

    // The nested readers, asked for one element at a time: a struct that decoded into
    // the right shape but the wrong field order still renders plausibly above.
    order_qt_types_nested """
        select id, array_size(f_array), f_array[1], f_map['k1'], f_map['k2'],
               struct_element(f_row, 'r_int'), struct_element(f_row, 'r_string')
        from log_types
    """

    // --- partitioned table --------------------------------------------------
    // The partition column is not read by the scanner: FE declares it and BE fills it
    // in from each range. Recording it next to the row it belongs to is what catches a
    // partition value attached to the wrong split.
    order_qt_part_all """select id, name, dt from log_part"""

    // Projecting nothing but the partition column leaves the scanner with an empty
    // projection - it still has to report the row count for each range.
    order_qt_part_grouped """select dt, count(*) from log_part group by dt"""

    order_qt_part_pruned """select id from log_part where dt = '20260101'"""

    // --- planning is visible in the plan ------------------------------------
    def basicPlan = planOf("""select * from log_basic""")
    assertTrue(basicPlan.contains("flussScan: readMode=default, unionRead=no"),
            "no fluss scan line: ${basicPlan}")
    assertTrue(basicPlan.contains("lakeSplits=0"), "unexpected lake splits: ${basicPlan}")
    assertTrue(basicPlan.contains("mode=auto"), "unexpected union read mode: ${basicPlan}")
    // One range per bucket that holds data. Which of the three buckets a fluss log row
    // lands in is the writer's choice, so the count is bounded rather than fixed.
    def basicRanges = logRangesOf(basicPlan)
    assertTrue(basicRanges >= 1 && basicRanges <= 3,
            "log_basic planned ${basicRanges} ranges over 3 buckets")

    def fullPartPlan = planOf("""select * from log_part""")
    assertTrue(fullPartPlan.contains("partition=3/3"),
            "all three partitions should be scanned: ${fullPartPlan}")
    def fullPartRanges = logRangesOf(fullPartPlan)
    assertTrue(fullPartRanges >= 3, "every partition holds data: ${fullPartPlan}")

    def prunedPlan = planOf("""select * from log_part where dt = '20260101'""")
    assertTrue(prunedPlan.contains("partition=1/3"),
            "partition pruning did not reach the connector: ${prunedPlan}")
    // Pruning has to shrink the work, not just the plan line: a partition name rendered
    // one way for the listing and another way for the match would prune to nothing here
    // while still reporting 1/3.
    def prunedRanges = logRangesOf(prunedPlan)
    assertTrue(prunedRanges >= 1 && prunedRanges <= 2,
            "one partition has 2 buckets, planned ${prunedRanges} ranges: ${prunedPlan}")

    // --- a table that was never written to ----------------------------------
    // Its buckets stop at offset 0, so planning emits no range at all. The empty answer
    // has to come from that, not from a scanner opened on an empty bucket.
    order_qt_empty_count """select count(*) from log_empty"""
    order_qt_empty_rows """select id, name from log_empty"""
    def emptyPlan = planOf("""select * from log_empty""")
    // An engine that drops a split-less scan altogether is just as correct as one that
    // keeps the node, so what is pinned is that no range was planned either way.
    def emptyMatcher = (emptyPlan =~ /logRanges=(\d+)/)
    assertTrue(!emptyMatcher.find() || emptyMatcher.group(1) == "0",
            "a table that was never written to planned ranges: ${emptyPlan}")

    sql """switch internal"""
    sql """drop catalog ${catalogName}"""
}
