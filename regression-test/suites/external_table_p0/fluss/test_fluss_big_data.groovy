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

// The same reads, at a size where nothing can be eyeballed.
//
// Every other fluss suite here works on a handful of rows chosen so that a wrong
// answer is visible by reading it. That leaves a whole class of faults uncovered:
// a scan that loses the last partial batch, a stopping offset applied to the first
// bucket only, a suppression set built per split rather than per bucket, a cache
// keyed too coarsely. None of them can show up on nine rows -- with one batch and
// one bucket's worth of keys, wrong and right are the same arrangement.
//
// Two fixtures of 100000 rows, each read down both paths: through a union-read
// catalog they are lake plus log, through a disabled one they are the pure fluss
// read of the same rows -- a full log replay for big_log, a kv snapshot plus its
// log for big_pk. So one table answers the log, primary-key and union-read cases,
// and the two paths can be compared against each other rather than against a
// number someone wrote down.
//
// What IS written down is arithmetic: the fixtures derive every column from a
// sequence, so 1..101000 sums to 5100550500 and nothing here is a transcription of
// whatever the environment happened to produce.
//
// Fixtures: big_log and big_pk in
// docker/thirdparties/docker-compose/fluss/sql/init.sql and init-lake-tail.sql.
suite("test_fluss_big_data", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String coordinatorPort = context.config.otherConfigs.get("fluss_coordinator_port")
    // The lake sits in an object store. Fluss removes every lake option whose name holds
    // key, secret or password before it hands a table's properties to a client, and Doris
    // configures storage once per catalog rather than per table, so the whole of how to
    // reach that store is stated on the catalog instead of learned from the fluss cluster.
    String minioPort = context.config.otherConfigs.get("fluss_minio_port")
    String bootstrapServers = "${externalEnvIp}:${coordinatorPort}"
    String unionCatalog = "test_fluss_big_data"
    String flussOnlyCatalog = "test_fluss_big_data_off"

    sql """drop catalog if exists ${unionCatalog}"""
    sql """
        create catalog ${unionCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.lake.paimon.s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin",
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
    // The load-bearing check of this suite. At this size the recorded aggregates say a
    // number came back; only the comparison says the OTHER reader agreed with it, and
    // the two readers here share no code below the scan node.
    def compareModes = { String query ->
        def union = rowsOf("""${query}""")
        def flussOnly = rowsOf("""${query}""".replace("from ", "from ${flussOnlyCatalog}.fluss_test."))
        assertEquals(flussOnly, union,
                "lake+log and fluss-only disagree for: ${query}\nfluss-only=${flussOnly}\nunion=${union}")
    }

    // --- a log table of 101000 rows -------------------------------------------
    // 100000 tiered plus 1000 written after, with contiguous ids, so the count, the
    // sum and the bounds are all closed forms: 101000 rows, sum 5100550500, ids 1 to
    // 101000. A seam that dropped the tail, read it twice or stopped a bucket early
    // moves at least one of those.
    def logPlan = planOf("""select * from big_log""")
    assertTrue(logPlan.contains("unionRead=yes"), "not a union read: ${logPlan}")
    assertTrue(countIn(logPlan, "lakeSplits") >= 1, "no lake splits: ${logPlan}")
    def logRanges = countIn(logPlan, "logRanges")
    assertTrue(logRanges >= 1 && logRanges <= 3,
            "big_log planned ${logRanges} log ranges over 3 buckets: ${logPlan}")

    order_qt_log_summary """
        select count(*) as rows_read, sum(id) as id_sum, min(id) as id_min, max(id) as id_max,
               count(distinct grp) as groups, sum(price) as price_sum
        from big_log
    """
    compareModes("""select count(*), sum(id), min(id), max(id), sum(price) from big_log""")

    // Every id exactly once. count(distinct) over 101000 values is what says so, and it
    // is a different question from the count: a row read twice keeps the count wrong and
    // the distinct count right, and one lost key does the opposite.
    order_qt_log_distinct """select count(distinct id) from big_log"""

    // Per-group counts, which is the whole scan grouped rather than aggregated flat: a
    // half silently dropped shows up as one group short of its share, not as zero.
    order_qt_log_groups """select grp, count(*) from big_log group by grp order by grp"""
    compareModes("""select grp, count(*) from big_log group by grp order by grp""")

    // Rows from both halves by name, including the two ids either side of the seam.
    order_qt_log_seam """
        select id, name, price, grp from big_log
        where id in (1, 2, 50000, 99999, 100000, 100001, 100002, 100999, 101000)
    """

    // A range predicate that spans the seam: the rows it selects come from the lake up
    // to 100000 and from the log after it, in one result.
    order_qt_log_range """select count(*), sum(id) from big_log where id between 99990 and 100010"""
    order_qt_log_projection """select count(*) from big_log where name like 'hot%'"""
    order_qt_log_ordered_head """select id, name from big_log order by id limit 5"""
    order_qt_log_ordered_tail """select id, name from big_log order by id desc limit 5"""

    // --- a primary-key table of 100495 rows -----------------------------------
    // 100000 keys in the lake; the tail updates 500 of them, adds 500 more and deletes
    // five. So the suppression set that filters the lake half holds 1005 keys rather
    // than the two or three every other primary-key fixture has, and the rows it must
    // drop are scattered through every bucket instead of sitting at the front of one.
    //
    // 100000 - 5 + 500 = 100495 rows. Sum of the surviving ids is
    // 5000050000 - (7 + 19999 + 50000 + 77777 + 99999) + sum(100001..100500) =
    // 5000050000 - 247782 + 50125250 = 5049927468.
    def pkPlan = planOf("""select * from big_pk""")
    assertTrue(pkPlan.contains("unionRead=yes"), "not a union read: ${pkPlan}")
    assertEquals(0, countIn(pkPlan, "logRanges"),
            "a primary-key table planned a plain log range: ${pkPlan}")
    def lakeSplits = countIn(pkPlan, "lakeSplits")
    def suppressed = countIn(pkPlan, "suppressedLakeSplits")
    assertTrue(lakeSplits >= 3, "three buckets should each contribute a lake split: ${pkPlan}")
    // 500 updated keys hash across all three buckets, so every lake split has a tail to
    // be filtered by and none may be let through unfiltered.
    assertEquals(lakeSplits, suppressed,
            "every bucket has a tail, so every lake split must be suppressed: ${pkPlan}")
    assertEquals(3, countIn(pkPlan, "pkTailRanges"), "one tail per bucket: ${pkPlan}")
    assertEquals(0, countIn(pkPlan, "pkRanges"),
            "the lake holds every bucket, so none is read whole: ${pkPlan}")

    order_qt_pk_summary """
        select count(*) as rows_read, sum(id) as id_sum, min(id) as id_min, max(id) as id_max,
               count(distinct grp) as groups
        from big_pk
    """
    compareModes("""select count(*), sum(id), min(id), max(id) from big_pk""")
    order_qt_pk_distinct """select count(distinct id) from big_pk"""

    // What the merge had to get right, counted by which half the winning row came from:
    // 500 keys the tail replaced (less the one it then deleted), 500 it added, and the
    // rest untouched in the lake. A suppression set that missed keys leaves the 'p'
    // count too high and the total wrong by the same amount.
    order_qt_pk_by_origin """
        select
            sum(case when name like 'hot%' then 1 else 0 end) as replaced,
            sum(case when name like 'new%' then 1 else 0 end) as added,
            sum(case when name like 'p%' then 1 else 0 end) as untouched
        from big_pk
    """
    compareModes("""
        select
            sum(case when name like 'hot%' then 1 else 0 end),
            sum(case when name like 'new%' then 1 else 0 end),
            sum(case when name like 'p%' then 1 else 0 end)
        from big_pk
    """)

    // The five deleted keys, one at a time. A delete is the case a merge of surviving
    // rows cannot express: the key is absent from the tail's own result, and the lake
    // row it removes stays unless something drops it.
    order_qt_pk_deleted """
        select id, name from big_pk where id in (7, 19999, 50000, 77777, 99999)
    """
    // And the keys either side of them, so "returns nothing" cannot be how the rows
    // above came to be missing.
    order_qt_pk_neighbours """
        select id, name from big_pk where id in (6, 8, 19998, 20000, 49999, 50001, 99998, 100000)
    """

    // Updated, added and untouched keys by name.
    order_qt_pk_sample """
        select id, name, grp from big_pk
        where id in (1, 500, 501, 99999, 100000, 100001, 100500)
    """
    order_qt_pk_groups """select grp, count(*) from big_pk group by grp order by grp"""
    order_qt_pk_ordered_head """select id, name from big_pk order by id limit 5"""

    // --- the same tables with the lake switched off ---------------------------
    // A full change-log replay of 101000 records for the log table, and a kv snapshot
    // plus its log for the primary-key one. These are the reads the other suites only
    // ever ask for a few rows of.
    sql """switch ${flussOnlyCatalog}"""
    sql """use fluss_test"""
    sql """set enable_file_scanner_v2 = true"""

    def flussOnlyLogPlan = planOf("""select * from big_log""")
    assertTrue(flussOnlyLogPlan.contains("unionRead=no"), "the lake should be off: ${flussOnlyLogPlan}")
    assertEquals(0, countIn(flussOnlyLogPlan, "lakeSplits"), "no lake half: ${flussOnlyLogPlan}")
    order_qt_fluss_only_log """
        select count(*) as rows_read, sum(id) as id_sum, max(id) as id_max from big_log
    """

    def flussOnlyPkPlan = planOf("""select * from big_pk""")
    assertTrue(flussOnlyPkPlan.contains("unionRead=no"), "the lake should be off: ${flussOnlyPkPlan}")
    assertEquals(3, countIn(flussOnlyPkPlan, "pkRanges"), "one range per bucket: ${flussOnlyPkPlan}")
    order_qt_fluss_only_pk """
        select count(*) as rows_read, sum(id) as id_sum, max(id) as id_max from big_pk
    """

    sql """switch internal"""
    sql """drop catalog if exists ${unionCatalog}"""
    sql """drop catalog if exists ${flussOnlyCatalog}"""
}
