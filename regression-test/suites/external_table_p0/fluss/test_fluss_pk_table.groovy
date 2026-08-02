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
// static - this suite never writes, so no polling gate is needed. Everything is
// asserted explicitly rather than through qt_ and a .out file: the values are the
// fixture's own literals, so an expectation that drifts from the fixture is a diff
// in one file rather than a regenerated baseline nobody reads.
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

    def scalarOf = { String query -> sql(query)[0][0].toString() }
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
    def basicRows = sql """select id, name, score from pk_basic order by id"""
    assertEquals(3, basicRows.size())
    assertEquals(["1", "k1", "1.5"], basicRows[0].collect { it.toString() })
    assertEquals(["2", "k2-updated", "22.5"], basicRows[1].collect { it.toString() })
    assertEquals(["4", "k4", "4.5"], basicRows[2].collect { it.toString() })

    // COUNT(*) projects no column at all. On a primary-key table the count still has
    // to be the merged one: counting change log records would report six.
    assertEquals("3", scalarOf("""select count(*) from pk_basic"""))

    // The deleted key must be absent, not merely superseded.
    assertEquals("0", scalarOf("""select count(*) from pk_basic where id = 3"""))
    // And the updated key must appear once, not once per version.
    assertEquals("1", scalarOf("""select count(*) from pk_basic where id = 2"""))
    assertEquals("0", scalarOf("""select count(*) from pk_basic where name = 'k2'"""))

    // --- projection and predicates -----------------------------------------
    // The merge needs the primary key even when the query does not ask for it, so the
    // reader adds it to what it fetches and projects it back out. A leaked key column
    // or a fetch-order projection shows up as the wrong values here.
    def names = sql """select name from pk_basic order by name"""
    assertEquals(["k1", "k2-updated", "k4"], names.collect { it[0].toString() })

    def reordered = sql """select score, name from pk_basic where id = 1"""
    assertEquals(["1.5", "k1"], reordered[0].collect { it.toString() })

    assertEquals("2", scalarOf("""select count(*) from pk_basic where score > 2.0"""))

    // --- every mapped type, in the kv row format ---------------------------
    // Primary-key tables store rows in a different format from a log table's, so this
    // repeats the type coverage rather than trusting the log suite for it. Asserted by
    // predicate, not by rendering: how a decimal or a map prints is the display layer's
    // business, and pinning it here would make this suite fail for the wrong reasons.
    assertEquals("2", scalarOf("""select count(*) from pk_types"""))

    assertEquals("1", scalarOf("""
        select count(*) from pk_types where id = 1
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
        select count(*) from pk_types where id = 1
            and f_char = 'char1'
            and f_string = 'string1'
            and hex(f_binary) = '010203'
            and hex(f_bytes) = '0A0B'
    """))

    // TIMESTAMP_LTZ is only checked for presence: its rendering depends on the session
    // time zone, which is not what this suite is pinning.
    assertEquals("1", scalarOf("""
        select count(*) from pk_types where id = 1
            and f_date = '2026-01-01'
            and f_timestamp = '2026-01-01 01:02:03.456789'
            and f_timestamp_ltz is not null
    """))

    assertEquals("1", scalarOf("""
        select count(*) from pk_types where id = 1
            and array_size(f_array) = 3
            and f_array[1] = 1
            and f_array[3] = 3
            and f_map['k1'] = 1
            and f_map['k2'] = 2
            and struct_element(f_row, 'r_int') = 1
            and struct_element(f_row, 'r_string') = 'nested1'
    """))

    // The all-NULL row. Read through a null map that is off by one column, every value
    // after it shifts, so this is checked column by column rather than by row count.
    assertEquals("1", scalarOf("""
        select count(*) from pk_types where id = 2
            and f_boolean is null and f_tinyint is null and f_smallint is null
            and f_int is null and f_bigint is null and f_float is null
            and f_double is null and f_decimal is null and f_char is null
            and f_string is null and f_binary is null and f_bytes is null
            and f_date is null and f_timestamp is null and f_timestamp_ltz is null
            and f_array is null and f_map is null and f_row is null
    """))

    // --- partitioned primary-key table --------------------------------------
    // A partitioned primary-key table is snapshotted per partition, so its snapshots
    // have to be asked for per partition too; asking at table level resumes the change
    // log at another partition's offset. The partition column itself is not read by the
    // scanner - FE declares it and BE fills it in from each range - so checking it
    // against the row it belongs to is what catches a partition value on the wrong split.
    def partRows = sql """select id, name, dt from pk_part order by dt, id"""
    assertEquals(3, partRows.size())
    assertEquals(["1", "q1a", "20260101"], partRows[0].collect { it.toString() })
    assertEquals(["2", "q1b-updated", "20260101"], partRows[1].collect { it.toString() })
    assertEquals(["3", "q2a", "20260102"], partRows[2].collect { it.toString() })

    // The delete landed in 20260102 and the update in 20260101: a merge that crossed
    // partitions would lose or resurrect one of them.
    def perPartition = sql """select dt, count(*) from pk_part group by dt order by dt"""
    assertEquals(2, perPartition.size())
    assertEquals(["20260101", "2"], perPartition[0].collect { it.toString() })
    assertEquals(["20260102", "1"], perPartition[1].collect { it.toString() })

    def prunedRows = sql """select id, name from pk_part where dt = '20260101' order by id"""
    assertEquals(2, prunedRows.size())
    assertEquals(["1", "q1a"], prunedRows[0].collect { it.toString() })
    assertEquals(["2", "q1b-updated"], prunedRows[1].collect { it.toString() })

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
