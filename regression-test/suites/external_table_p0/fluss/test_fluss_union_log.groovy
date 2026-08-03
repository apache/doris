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
    // The comparison stays in the code even though the results are recorded: it is
    // not a hand-written expectation but the load-bearing check of this suite, and
    // it says something no pair of recorded blocks does -- that two entirely
    // different readers agreed on this table, row for row.
    def compareModes = { String query ->
        def union = rowsOf("""${query}""")
        def flussOnly = rowsOf("""${query}""".replace("from ", "from ${flussOnlyCatalog}.fluss_test."))
        assertEquals(flussOnly, union,
                "lake+log and fluss-only disagree for: ${query}\nfluss-only=${flussOnly}\nunion=${union}")
    }

    // Four rows out of the lake and two out of the log, in one result.
    order_qt_log_rows """select id, name, price from lake_log"""
    compareModes("select id, name, price from lake_log order by id")

    // Aggregates run over both halves; a half silently dropped shows up here even
    // when the ordered row list is not materialized.
    order_qt_log_count """select count(*) from lake_log"""
    order_qt_log_sum """select sum(price) from lake_log"""

    // --- a table the lake already holds in full ------------------------------
    // Nothing was written after tiering caught up, so every bucket's log range is
    // empty and planning must emit none at all rather than ranges that read
    // nothing.
    def coldPlan = planOf("""select * from lake_cold""")
    assertTrue(coldPlan.contains("unionRead=yes"), "not a union read: ${coldPlan}")
    assertTrue(countIn(coldPlan, "lakeSplits") >= 1, "no lake splits: ${coldPlan}")
    assertEquals(0, countIn(coldPlan, "logRanges"), "cold table still has log ranges: ${coldPlan}")
    order_qt_cold_rows """select id, name from lake_cold"""
    compareModes("select id, name from lake_cold order by id")

    // --- every type crosses the seam -----------------------------------------
    // Row 1 comes back through paimon, row 2 (the all-NULL one, written after
    // tiering stopped) through the fluss log: two decoders feeding one result set,
    // which is what pins the mapping parity on real values rather than on declared
    // types. A null map read one column off shifts every value after it, and only
    // recording the row column by column notices.
    //
    // BINARY and BYTES go through hex() as everywhere else. An equality predicate on
    // the microsecond TIMESTAMP would match nothing for the lake half -- pushed into
    // paimon, it finds no row, which is the paimon connector's own behaviour and not
    // something this suite should pin; recording the value sidesteps it.
    order_qt_types_all """
        select id, f_boolean, f_tinyint, f_smallint, f_int, f_bigint, f_float, f_double,
               f_decimal, f_char, f_string, hex(f_binary) as f_binary_hex,
               hex(f_bytes) as f_bytes_hex, f_date, f_timestamp, f_timestamp_ltz,
               f_array, f_map, f_row
        from lake_types
    """
    compareModes("select id from lake_types order by id")

    // --- partitioning, where the two halves prune differently ----------------
    // The fluss half is given the partitions the engine pruned to; the paimon half
    // ignores that list and prunes on the pushed-down predicate instead. Both have
    // to land on the same partition: one partition here has a log tail, the other is
    // served entirely from the lake.
    order_qt_part_rows """select id, name, dt from lake_part"""
    compareModes("select id, name, dt from lake_part order by id")

    def tieredPartPlan = planOf("""select * from lake_part where dt = '20260102'""")
    assertTrue(tieredPartPlan.contains("unionRead=yes"), "not a union read: ${tieredPartPlan}")
    assertEquals(0, countIn(tieredPartPlan, "logRanges"),
            "a fully tiered partition still has log ranges: ${tieredPartPlan}")
    order_qt_part_tiered_only """select id from lake_part where dt = '20260102'"""

    def tailPartPlan = planOf("""select * from lake_part where dt = '20260101'""")
    assertEquals(1, countIn(tailPartPlan, "logRanges"),
            "the partition with a tail lost its log range: ${tailPartPlan}")
    order_qt_part_with_tail """select id from lake_part where dt = '20260101'"""

    // --- a primary-key table is not a log table ------------------------------
    // Concatenating its halves the way this suite's tables are concatenated would
    // return superseded and deleted rows, so it is merged BY KEY instead -- a
    // different plan out of the same mode, asserted here only so that the two
    // shapes cannot quietly become one. What such a table reads as lives in
    // test_fluss_lake_pk and test_fluss_lake_pk_merge.
    def pkPlan = planOf("""select * from lake_pk""")
    assertTrue(pkPlan.contains("unionRead=yes"), "not a union read: ${pkPlan}")
    assertEquals(0, countIn(pkPlan, "logRanges"),
            "a primary-key table planned a plain log range: ${pkPlan}")
    assertTrue(countIn(pkPlan, "pkTailRanges") >= 1, "no tail replayed: ${pkPlan}")

    // --- required does not mean "every table has a lake" ---------------------
    // A table with no lake at all is not an error in required mode: there is
    // nothing to fall back FROM. Only a lake table whose snapshot cannot be read
    // is.
    def plainPlan = planOf("""select * from log_basic""")
    assertTrue(plainPlan.contains("flussScan: unionRead=no"), "unexpected union read: ${plainPlan}")
    assertEquals(0, countIn(plainPlan, "lakeSplits"))
    order_qt_plain_count """select count(*) from log_basic"""

    sql """switch internal"""
    sql """drop catalog if exists ${unionCatalog}"""
    sql """drop catalog if exists ${flussOnlyCatalog}"""
}
