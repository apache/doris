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
// Everything is asserted explicitly rather than through qt_ and a .out file. The
// values are the fixture's own literals, so an expectation that drifts from the
// fixture is a diff in one file rather than a regenerated baseline nobody reads.
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

    def scalarOf = { String query -> sql(query)[0][0].toString() }
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
    def basicRows = sql """select id, name, price from log_basic order by id"""
    assertEquals(3, basicRows.size())
    assertEquals(["1", "alice", "10.10"], basicRows[0].collect { it.toString() })
    assertEquals(["2", "bob", "20.20"], basicRows[1].collect { it.toString() })
    assertEquals(["3", "carol", "30.30"], basicRows[2].collect { it.toString() })

    // COUNT(*) projects no column at all: the scanner has to report how many rows it
    // read without returning one. A scanner that answered with an empty batch instead
    // would make an untouched-looking table out of a populated one.
    assertEquals("3", scalarOf("""select count(*) from log_basic"""))

    // --- projection and predicates -----------------------------------------
    def names = sql """select name from log_basic order by name"""
    assertEquals(["alice", "bob", "carol"], names.collect { it[0].toString() })

    // Columns asked for in an order other than the table's: the scanner resolves the
    // projection by name, so a positional shortcut anywhere would swap these two.
    def swapped = sql """select name, id from log_basic where id = 2"""
    assertEquals(1, swapped.size())
    assertEquals(["bob", "2"], swapped[0].collect { it.toString() })

    def filtered = sql """select id from log_basic where price > 15.00 order by id"""
    assertEquals(["2", "3"], filtered.collect { it[0].toString() })
    assertEquals("1", scalarOf("""select count(*) from log_basic where name = 'bob'"""))

    // --- every mapped type survives the round trip --------------------------
    // Asserted through predicates rather than by comparing rendered values: the
    // rendering of a map or a struct is the display layer's business, while what this
    // suite is about is whether the bytes fluss returned decoded into the right value.
    assertEquals("3", scalarOf("""select count(*) from log_types"""))

    assertEquals("1", scalarOf("""
        select count(*) from log_types where id = 1
            and f_boolean = true
            and f_tinyint = 1
            and f_smallint = 2
            and f_int = 3
            and f_bigint = 4
            and f_float = cast(1.5 as float)
            and f_double = 2.5
            and f_decimal = 123.4567
    """))

    // Fluss BYTES and BINARY map to a Doris string by default, so their content is
    // compared as hex; a decoder that lost the length would return a prefix of this.
    assertEquals("1", scalarOf("""
        select count(*) from log_types where id = 1
            and f_char = 'char1'
            and f_string = 'string1'
            and hex(f_binary) = '010203'
            and hex(f_bytes) = '0A0B'
    """))

    // TIMESTAMP_LTZ is only checked for presence: its rendering depends on the session
    // time zone, which is not what this suite is pinning.
    assertEquals("1", scalarOf("""
        select count(*) from log_types where id = 1
            and f_date = '2026-01-01'
            and f_timestamp = '2026-01-01 01:02:03.456789'
            and f_timestamp_ltz is not null
    """))

    assertEquals("1", scalarOf("""
        select count(*) from log_types where id = 1
            and array_size(f_array) = 3
            and f_array[1] = 1
            and f_array[3] = 3
            and f_map['k1'] = 1
            and f_map['k2'] = 2
            and struct_element(f_row, 'r_int') = 1
            and struct_element(f_row, 'r_string') = 'nested1'
    """))

    // The second row is the negative-value one: a sign lost in decoding shows up here
    // and nowhere else.
    assertEquals("1", scalarOf("""
        select count(*) from log_types where id = 2
            and f_boolean = false
            and f_tinyint = -1
            and f_smallint = -2
            and f_int = -3
            and f_bigint = -4
            and f_float = cast(-1.5 as float)
            and f_double = -2.5
            and f_decimal = -123.4567
            and array_size(f_array) = 2
    """))

    // The all-NULL row. Read through a null map that is off by one column, every value
    // after it shifts, so this is checked column by column rather than by row count.
    assertEquals("1", scalarOf("""
        select count(*) from log_types where id = 3
            and f_boolean is null and f_tinyint is null and f_smallint is null
            and f_int is null and f_bigint is null and f_float is null
            and f_double is null and f_decimal is null and f_char is null
            and f_string is null and f_binary is null and f_bytes is null
            and f_date is null and f_timestamp is null and f_timestamp_ltz is null
            and f_array is null and f_map is null and f_row is null
    """))

    // --- partitioned table --------------------------------------------------
    // The partition column is not read by the scanner: FE declares it and BE fills it
    // in from each range. Checking it against the row it belongs to is what catches a
    // partition value attached to the wrong split.
    def partRows = sql """select id, name, dt from log_part order by id"""
    assertEquals(4, partRows.size())
    assertEquals(["1", "p1a", "20260101"], partRows[0].collect { it.toString() })
    assertEquals(["2", "p1b", "20260101"], partRows[1].collect { it.toString() })
    assertEquals(["3", "p2a", "20260102"], partRows[2].collect { it.toString() })
    assertEquals(["4", "p3a", "20260103"], partRows[3].collect { it.toString() })

    // Projecting nothing but the partition column leaves the scanner with an empty
    // projection - it still has to report the row count for each range.
    def perPartition = sql """select dt, count(*) from log_part group by dt order by dt"""
    assertEquals(3, perPartition.size())
    assertEquals(["20260101", "2"], perPartition[0].collect { it.toString() })
    assertEquals(["20260102", "1"], perPartition[1].collect { it.toString() })
    assertEquals(["20260103", "1"], perPartition[2].collect { it.toString() })

    def prunedRows = sql """select id from log_part where dt = '20260101' order by id"""
    assertEquals(["1", "2"], prunedRows.collect { it[0].toString() })

    // --- planning is visible in the plan ------------------------------------
    def basicPlan = planOf("""select * from log_basic""")
    assertTrue(basicPlan.contains("flussScan: unionRead=no"),
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
    assertEquals("0", scalarOf("""select count(*) from log_empty"""))
    def emptyRows = sql """select id, name from log_empty"""
    assertEquals(0, emptyRows.size())
    def emptyPlan = planOf("""select * from log_empty""")
    // An engine that drops a split-less scan altogether is just as correct as one that
    // keeps the node, so what is pinned is that no range was planned either way.
    def emptyMatcher = (emptyPlan =~ /logRanges=(\d+)/)
    assertTrue(!emptyMatcher.find() || emptyMatcher.group(1) == "0",
            "a table that was never written to planned ranges: ${emptyPlan}")

    sql """switch internal"""
    sql """drop catalog ${catalogName}"""
}
