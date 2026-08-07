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

// Merging a primary-key table with its log tail, in the shapes where the
// merge can be got wrong in ways one bucket cannot show.
//
// test_fluss_lake_pk covers what the merge IS, over a single-bucket table. The
// fixtures here cover WHERE each half comes from:
//   lake_pk_multi  three buckets, tail in some of them -- a lake split may only
//                  be filtered by the tail of its own bucket;
//   lake_pk_part   three partitions, one lake+tail, one lake only, one that the
//                  lake has never seen and that is therefore read whole from
//                  fluss inside the same scan;
//   lake_pk_cold   nothing left in the log, so nothing to merge and nothing to
//                  plan for.
//
// Every table is read twice: through the merge, and through the fluss-only read
// that serves a primary-key table in full. The two have no code in common past
// planning, so their agreeing is the argument -- the recorded blocks alone
// cannot say whether both are wrong the same way.
//
// Fixtures come from docker/thirdparties/docker-compose/fluss/sql/init.sql and
// init-lake-tail.sql, and are frozen: tiering is stopped before the tail is
// written, so the halves stay apart for as long as the environment lives.
suite("test_fluss_lake_pk_merge", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String coordinatorPort = context.config.otherConfigs.get("fluss_coordinator_port")
    String bootstrapServers = "${externalEnvIp}:${coordinatorPort}"
    String mergeCatalog = "test_fluss_lake_pk_merge"
    String flussOnlyCatalog = "test_fluss_lake_pk_merge_off"

    // required, not auto: a fallback here would read the right rows out of fluss
    // alone and every recorded block would still match, so the mode that cannot
    // fall back is the one that makes this suite about the merge.
    sql """drop catalog if exists ${mergeCatalog}"""
    sql """
        create catalog ${mergeCatalog} properties (
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

    sql """switch ${mergeCatalog}"""
    sql """use fluss_test"""
    // The C++ glue exists only for the v2 file scanner, and the session variable
    // that picks between them is randomised by the fuzzy mode this pipeline runs.
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
    // The load-bearing check: the merge and a reader that never touches the lake
    // agree row for row. It stays in the code rather than becoming a second
    // recorded block, because what is asserted is the agreement -- two identical
    // recordings only look alike to whoever reads them.
    def compareModes = { String query ->
        def merged = rowsOf("""${query}""")
        def flussOnly = rowsOf("""${query}""".replace("from ", "from ${flussOnlyCatalog}.fluss_test."))
        assertEquals(flussOnly, merged,
                "merge and fluss-only disagree for: ${query}\nfluss-only=${flussOnly}\nmerged=${merged}")
    }

    // --- three buckets, and a tail that reaches only some of them -------------
    // Which bucket a key lands in is fluss's hash of it, so the numbers below are
    // bounds rather than values. What they have to say is that the tail was split
    // across buckets and applied to those buckets only: some lake splits carry a
    // suppression set and some do not. Binding a tail to the wrong bucket
    // suppresses nothing at all -- a key lives in exactly one bucket -- and the
    // rows it was meant to replace come back beside their replacements.
    def multiPlan = planOf("""select * from lake_pk_multi""")
    assertTrue(multiPlan.contains("unionRead=yes"), "not a merge: ${multiPlan}")
    def multiLakeSplits = countIn(multiPlan, "lakeSplits")
    def multiSuppressed = countIn(multiPlan, "suppressedLakeSplits")
    def multiTailRanges = countIn(multiPlan, "pkTailRanges")
    assertTrue(multiLakeSplits >= 2,
            "fixture no longer spreads over buckets, so nothing here tests binding: ${multiPlan}")
    assertTrue(multiTailRanges >= 2,
            "fixture tail no longer spans buckets, so nothing here tests binding: ${multiPlan}")
    assertTrue(multiSuppressed >= 1 && multiSuppressed < multiLakeSplits,
            "suppression is not per bucket: ${multiSuppressed} of ${multiLakeSplits} splits: ${multiPlan}")
    // Every bucket of this table is in the lake, so none is read whole from fluss.
    assertEquals(0, countIn(multiPlan, "pkRanges"), "a bucket was read whole: ${multiPlan}")

    // The lake half on its own: nine keys as tiered, row 5 already updated.
    order_qt_multi_lake """select id, name from lake_pk_multi\$lake"""
    // The table: row 2 updated by the tail, row 7 deleted by it, row 10 added --
    // each in whatever bucket its key hashes to.
    order_qt_multi_rows """select id, name from lake_pk_multi"""
    compareModes("select id, name from lake_pk_multi order by id")
    order_qt_multi_count """select count(*) from lake_pk_multi"""
    // A predicate on a key the tail replaced, and one on a key it deleted: the
    // filtered path has to suppress exactly as the full scan does.
    order_qt_multi_updated """select name from lake_pk_multi where id = 2"""
    order_qt_multi_deleted """select name from lake_pk_multi where id = 7"""
    order_qt_multi_new """select name from lake_pk_multi where id = 10"""

    // --- partitions, each standing differently towards the lake ---------------
    // 20260101 is lake plus tail, 20260102 is lake alone, 20260103 was created
    // after tiering stopped and the lake has never heard of it. All three are read
    // by one scan, which therefore mixes suppressed lake splits, plain lake
    // splits, and buckets read whole out of fluss.
    def partPlan = planOf("""select * from lake_pk_part""")
    assertTrue(partPlan.contains("unionRead=yes"), "not a merge: ${partPlan}")
    assertTrue(countIn(partPlan, "lakeSplits") >= 2, "lake half lost a partition: ${partPlan}")
    assertTrue(countIn(partPlan, "suppressedLakeSplits") >= 1,
            "the partition with a tail was not suppressed: ${partPlan}")
    assertTrue(countIn(partPlan, "pkTailRanges") >= 1, "no tail replayed: ${partPlan}")
    assertTrue(countIn(partPlan, "pkRanges") >= 1,
            "the partition the lake never saw was not read from fluss: ${partPlan}")

    order_qt_part_rows """select id, name, dt from lake_pk_part"""
    compareModes("select id, name, dt from lake_pk_part order by dt, id")

    // Pruned to the partition the lake holds in full: lake splits, no tail, and
    // nothing suppressed. Pruning is per partition on the fluss half and by pushed
    // predicate on the paimon half, so a plan that keeps the tail here has bound
    // the two halves at table level.
    def tieredPlan = planOf("""select * from lake_pk_part where dt = '20260102'""")
    assertTrue(countIn(tieredPlan, "lakeSplits") >= 1, "lake half pruned away: ${tieredPlan}")
    assertEquals(0, countIn(tieredPlan, "suppressedLakeSplits"),
            "a partition with no tail was suppressed: ${tieredPlan}")
    assertEquals(0, countIn(tieredPlan, "pkTailRanges"), "tail read for nothing: ${tieredPlan}")
    order_qt_part_lake_only """select id, name from lake_pk_part where dt = '20260102'"""

    // Pruned to the partition the lake has never seen: no lake half at all, and
    // its buckets read whole from fluss.
    def flussOnlyPartPlan = planOf("""select * from lake_pk_part where dt = '20260103'""")
    assertEquals(0, countIn(flussOnlyPartPlan, "lakeSplits"),
            "a partition the lake never saw got lake splits: ${flussOnlyPartPlan}")
    assertTrue(countIn(flussOnlyPartPlan, "pkRanges") >= 1,
            "the partition was not read from fluss: ${flussOnlyPartPlan}")
    order_qt_part_fluss_only """select id, name from lake_pk_part where dt = '20260103'"""

    // Pruned to the partition that has a tail.
    def tailPartPlan = planOf("""select * from lake_pk_part where dt = '20260101'""")
    assertTrue(countIn(tailPartPlan, "suppressedLakeSplits") >= 1,
            "the tail of this partition was lost: ${tailPartPlan}")
    assertEquals(1, countIn(tailPartPlan, "pkTailRanges"), "unexpected tail count: ${tailPartPlan}")
    order_qt_part_with_tail """select id, name from lake_pk_part where dt = '20260101'"""

    // --- nothing left in the log ---------------------------------------------
    // The lake holds this table in full. Merging costs nothing when there is
    // nothing to merge, and only the plan can say so: a reader that built a
    // suppression set out of an empty tail would return these same rows while
    // opening a log scanner per bucket to do it.
    def coldPlan = planOf("""select * from lake_pk_cold""")
    assertTrue(coldPlan.contains("unionRead=yes"), "not a merge: ${coldPlan}")
    assertTrue(countIn(coldPlan, "lakeSplits") >= 1, "no lake splits: ${coldPlan}")
    assertEquals(0, countIn(coldPlan, "suppressedLakeSplits"),
            "a table with no tail still wraps splits: ${coldPlan}")
    assertEquals(0, countIn(coldPlan, "pkTailRanges"), "a tail was read for nothing: ${coldPlan}")
    assertEquals(0, countIn(coldPlan, "pkRanges"), "a bucket was read whole: ${coldPlan}")
    order_qt_cold_rows """select id, name from lake_pk_cold"""
    compareModes("select id, name from lake_pk_cold order by id")

    // --- the scanner the session asked for ------------------------------------
    // A merge plans BOTH range kinds onto one scan node -- the wrapped lake split
    // and the PK_TAIL that replays what it suppressed -- and neither is dispatched
    // anywhere but FileScannerV2. enable_file_scanner_v2 is a supported session
    // variable, so the merge has to survive a session that turned it off; the pin
    // at the top of this suite is there for the fuzzy mode, not to hide this. Kept
    // as a comparison rather than a recorded block for the same reason as
    // compareModes: what is asserted is the agreement between two settings.
    sql """set enable_file_scanner_v2 = false"""
    def mergedWithoutV2 = rowsOf("select id, name from lake_pk_multi order by id")
    sql """set enable_file_scanner_v2 = true"""
    def mergedWithV2 = rowsOf("select id, name from lake_pk_multi order by id")
    assertEquals(mergedWithV2, mergedWithoutV2,
            "enable_file_scanner_v2 changed the answer of a merge"
                    + "\non=${mergedWithV2}\noff=${mergedWithoutV2}")

    // Deletion vectors are not covered here and the reason is in init.sql: fluss
    // does create the paimon table with them on, but its tiering service writes
    // no deletion vector index, and paimon then reads such a table as empty. A
    // fixture that reads as empty asserts nothing. Deletion vectors under the
    // suppression filter stay covered by the BE unit tests.

    sql """switch internal"""
    sql """drop catalog if exists ${mergeCatalog}"""
    sql """drop catalog if exists ${flussOnlyCatalog}"""
}
