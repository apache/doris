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

// Reading a tiered log table as its lake plus the log written after it.
//
// The lake half is planned by the paimon connector on a snapshot the fluss
// coordinator pinned, the log half by fluss from the offsets that snapshot
// stopped at, and both halves end up as ranges of one scan node -- so BE builds
// a paimon reader and a fluss reader for the same query.
//
// The load-bearing assertion is the comparison between the two modes. `required`
// reads lake plus log; `disabled` replays the whole fluss log, which still holds
// everything because tiering copies rather than moves. Two entirely different
// readers over one table: if the seam between the halves is off by a row in
// either direction -- a row counted twice, a row skipped -- the two disagree.
// A single-mode suite could not tell a correct seam from a plausible one.
//
// Fixtures come from docker/thirdparties/docker-compose/fluss/sql/init.sql and
// are frozen: the tiering service is stopped before the log tail is written.
suite("test_fluss_union_log", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String coordinatorPort = context.config.otherConfigs.get("fluss_coordinator_port")
    String bootstrapServers = "${externalEnvIp}:${coordinatorPort}"
    String unionCatalog = "test_fluss_union_log"
    String flussOnlyCatalog = "test_fluss_union_log_off"

    // Two catalogs rather than one reconfigured between queries: the read mode is
    // a catalog property, and having both live at once is what lets the same query
    // be run down both paths and compared.
    sql """drop catalog if exists ${unionCatalog}"""
    sql """
        create catalog ${unionCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.union_read.mode" = "required"
        );
    """
    sql """drop catalog if exists ${flussOnlyCatalog}"""
    sql """
        create catalog ${flussOnlyCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.union_read.mode" = "disabled"
        );
    """

    sql """switch ${unionCatalog}"""
    sql """use fluss_test"""
    sql """set enable_file_scanner_v2 = true"""

    def scalarOf = { String query -> sql(query)[0][0].toString() }
    def rowsOf = { String query -> sql(query).collect { row -> row.collect { it.toString() } } }
    def planOf = { String query ->
        return sql("""explain ${query}""").collect { it[0].toString() }.join("\n")
    }
    def countIn = { String plan, String field ->
        def matcher = (plan =~ /${field}=(\d+)/)
        assertTrue(matcher.find(), "plan has no ${field}: ${plan}")
        return matcher.group(1) as int
    }

    // --- both halves are planned, and the plan says which is which -----------
    def logPlan = planOf("""select * from lake_log""")
    assertTrue(logPlan.contains("flussScan: unionRead=yes"), "not a union read: ${logPlan}")
    assertTrue(logPlan.contains("mode=required"), "unexpected mode: ${logPlan}")
    assertTrue(countIn(logPlan, "lakeSplits") >= 1, "no lake splits: ${logPlan}")
    // Four rows were tiered across three buckets and two written after, so at
    // least one bucket has caught up with the lake and must contribute nothing.
    // Which buckets the writer chose is its own business, hence a bound.
    def logRanges = countIn(logPlan, "logRanges")
    assertTrue(logRanges >= 1 && logRanges <= 2,
            "lake_log planned ${logRanges} log ranges over 3 buckets: ${logPlan}")
    assertEquals(0, countIn(logPlan, "pkRanges"))

    // --- the seam: same rows down both paths ---------------------------------
    def compareModes = { String query, int expectedRows ->
        def union = rowsOf("""${query}""")
        def flussOnly = rowsOf("""${query}""".replace("from ", "from ${flussOnlyCatalog}.fluss_test."))
        assertEquals(expectedRows, union.size(), "union read returned ${union.size()} rows for: ${query}")
        assertEquals(flussOnly, union,
                "lake+log and fluss-only disagree for: ${query}\nfluss-only=${flussOnly}\nunion=${union}")
        return union
    }

    // Four rows out of the lake and two out of the log, in one result.
    def logRows = compareModes("select id, name, price from lake_log order by id", 6)
    assertEquals(["1", "lake1", "1.10"], logRows[0])
    assertEquals(["4", "lake4", "4.40"], logRows[3])
    assertEquals(["5", "hot5", "5.50"], logRows[4])
    assertEquals(["6", "hot6", "6.60"], logRows[5])

    // Aggregates run over both halves; a half silently dropped shows up here even
    // when the ordered row list is not materialized.
    assertEquals("6", scalarOf("""select count(*) from lake_log"""))
    assertEquals("23.10", scalarOf("""select sum(price) from lake_log"""))

    // --- a table the lake already holds in full ------------------------------
    // Nothing was written after tiering caught up, so every bucket's log range is
    // empty and planning must emit none at all rather than ranges that read
    // nothing.
    def coldPlan = planOf("""select * from lake_cold""")
    assertTrue(coldPlan.contains("unionRead=yes"), "not a union read: ${coldPlan}")
    assertTrue(countIn(coldPlan, "lakeSplits") >= 1, "no lake splits: ${coldPlan}")
    assertEquals(0, countIn(coldPlan, "logRanges"), "cold table still has log ranges: ${coldPlan}")
    compareModes("select id, name from lake_cold order by id", 3)

    // --- every type crosses the seam -----------------------------------------
    // Row 1 comes back through paimon, row 2 through the fluss log: two decoders
    // feeding one result set, which is also what pins the mapping parity on real
    // values rather than on declared types.
    compareModes("select id from lake_types order by id", 2)
    assertEquals("1", scalarOf("""
        select count(*) from lake_types where id = 1
            and f_boolean = true
            and f_tinyint = 1 and f_smallint = 2 and f_int = 3 and f_bigint = 4
            and f_float = cast(1.5 as float) and f_double = 2.5
            and f_decimal = 123.4567
            and f_char = 'char1' and f_string = 'string1'
            and hex(f_binary) = '010203' and hex(f_bytes) = '0A0B'
            and f_date = '2026-01-01'
            -- Cast rather than compared as a timestamp: this row comes from the lake half, and an
            -- equality on a microsecond TIMESTAMP pushed into paimon matches nothing there. See the
            -- same note in test_fluss_lake_only: it is the paimon connector's behaviour, reproducible
            -- through a plain paimon catalog, and not something this suite should pin.
            and cast(f_timestamp as string) = '2026-01-01 01:02:03.456789'
            and f_timestamp_ltz is not null
            and array_size(f_array) = 3 and f_array[1] = 1 and f_array[3] = 3
            and f_map['k1'] = 1 and f_map['k2'] = 2
            and struct_element(f_row, 'r_int') = 1
            and struct_element(f_row, 'r_string') = 'nested1'
    """))
    // The all-NULL row arrived after tiering stopped, so it is the log half's.
    // Checked column by column: a null map read one column off shifts everything
    // after it, and a row count would not notice.
    assertEquals("1", scalarOf("""
        select count(*) from lake_types where id = 2
            and f_boolean is null and f_tinyint is null and f_smallint is null
            and f_int is null and f_bigint is null and f_float is null
            and f_double is null and f_decimal is null and f_char is null
            and f_string is null and f_binary is null and f_bytes is null
            and f_date is null and f_timestamp is null and f_timestamp_ltz is null
            and f_array is null and f_map is null and f_row is null
    """))

    // --- partitioning, where the two halves prune differently ----------------
    // The fluss half is given the partitions the engine pruned to; the paimon half
    // ignores that list and prunes on the pushed-down predicate instead. Both have
    // to land on the same partition, which is what these two assertions separate:
    // one partition has a log tail, the other is served entirely from the lake.
    def partRows = compareModes("select id, name, dt from lake_part order by id", 4)
    assertEquals(["1", "lp1a", "20260101"], partRows[0])
    assertEquals(["3", "lp2a", "20260102"], partRows[2])
    assertEquals(["4", "lp1c", "20260101"], partRows[3])

    def tieredPartPlan = planOf("""select * from lake_part where dt = '20260102'""")
    assertTrue(tieredPartPlan.contains("unionRead=yes"), "not a union read: ${tieredPartPlan}")
    assertEquals(0, countIn(tieredPartPlan, "logRanges"),
            "a fully tiered partition still has log ranges: ${tieredPartPlan}")
    assertEquals(["3"], rowsOf("""select id from lake_part where dt = '20260102'""").collect { it[0] })

    def tailPartPlan = planOf("""select * from lake_part where dt = '20260101'""")
    assertEquals(1, countIn(tailPartPlan, "logRanges"),
            "the partition with a tail lost its log range: ${tailPartPlan}")
    assertEquals(["1", "2", "4"],
            rowsOf("""select id from lake_part where dt = '20260101' order by id""").collect { it[0] })

    // --- what is not supported yet fails loudly ------------------------------
    // Merging a lake with a change log BY KEY is not implemented. The refusal has
    // to name the primary key and say what reading with the lake switched off
    // would and would not give, because that fallback returns a partial table for
    // a primary-key table rather than the whole one.
    test {
        sql """select * from lake_pk"""
        exception "primary-key"
    }

    // With the lake switched off the same table reads as the fluss-only merged
    // view: row 2's pre-tiering update, row 3's post-tiering one, row 1 deleted.
    def pkFlussOnly = rowsOf(
            """select id, name from ${flussOnlyCatalog}.fluss_test.lake_pk order by id""")
    assertEquals([["2", "lp2-lake"], ["3", "lp3-hot"]], pkFlussOnly)

    // --- required does not mean "every table has a lake" ---------------------
    // A table with no lake at all is not an error in required mode: there is
    // nothing to fall back FROM. Only a lake table whose snapshot cannot be read
    // is.
    def plainPlan = planOf("""select * from log_basic""")
    assertTrue(plainPlan.contains("flussScan: unionRead=no"), "unexpected union read: ${plainPlan}")
    assertEquals(0, countIn(plainPlan, "lakeSplits"))
    assertEquals("3", scalarOf("""select count(*) from log_basic"""))

    sql """switch internal"""
    sql """drop catalog if exists ${unionCatalog}"""
    sql """drop catalog if exists ${flussOnlyCatalog}"""
}
