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

// Partition columns of every type, and the line between the ones that can be read
// and the ones that cannot.
//
// A fluss partition carries its value nowhere but in its own name, and fluss allows
// only ASCII letters, digits, '_' and '-' there -- so a value holding anything else
// is rewritten on the way in and cannot be recovered. Which types survive that is
// not a matter of taste, and every verdict below was established against this
// cluster: STRING, CHAR, BOOLEAN, the integer family, DATE and (as text) BINARY come
// back as written; FLOAT, DOUBLE, TIME and the timestamps do not.
//
// The other suites partition by STRING only, which is also the one type that cannot
// tell a rendering bug from a working one -- a number, a date or a padded CHAR is
// where the two sides of that rendering could disagree.
//
// Fixtures: part_types, part_ts, lake_part_int, lake_pk_part_int in
// docker/thirdparties/docker-compose/fluss/sql/init.sql.
suite("test_fluss_partition_types", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String coordinatorPort = context.config.otherConfigs.get("fluss_coordinator_port")
    String bootstrapServers = "${externalEnvIp}:${coordinatorPort}"
    String catalogName = "test_fluss_partition_types"
    String varbinaryCatalog = "test_fluss_partition_types_vb"
    String unionCatalog = "test_fluss_partition_types_union"
    String flussOnlyCatalog = "test_fluss_partition_types_off"

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}"
        );
    """
    sql """switch ${catalogName}"""
    sql """use fluss_test"""
    sql """set enable_file_scanner_v2 = true"""

    def planOf = { String query ->
        return sql("""explain ${query}""").collect { it[0].toString() }.join("\n")
    }
    def countIn = { String plan, String field ->
        def matcher = (plan =~ /${field}=(\d+)/)
        assertTrue(matcher.find(), "plan has no ${field}: ${plan}")
        return matcher.group(1) as int
    }
    def rowsOf = { String query -> sql(query).collect { row -> row.collect { it.toString() } } }

    // --- every readable partition type, in one table --------------------------
    // Recorded unsorted, because the column ORDER is what fe-core zips the partition
    // values against positionally: a listing whose values are right but in another
    // order assigns each partition someone else's value, silently.
    qt_desc_part_types """desc part_types"""

    // The partition names as this connector renders them. Nothing else pins the
    // rendering of a boolean, a date or a byte string into a partition name, and both
    // the listing and split planning read this same rendering -- so a change here that
    // reached only one of them would prune to nothing while still looking planned.
    order_qt_part_names """show partitions from part_types"""

    // The values themselves, next to the row they belong to. The partition columns are
    // not read by the scanner at all: FE declares them and BE fills each one in from
    // the range it came with, so a value on the wrong split shows up only here.
    //
    // p_bin records as the hex of "0102" rather than of the bytes 0x01 0x02, and that is
    // the truth of a partitioned BINARY rather than a defect: what fluss kept is the
    // NAME it gave the partition, which is the hex text, and the value in the row is
    // that text. A non-partitioned BINARY column, whose bytes are stored in the row
    // itself, reads back as the bytes -- covered in the type suites.
    order_qt_part_rows """
        select id, name, p_str, p_char, p_bool, p_tiny, p_small, p_int, p_big, p_date,
               p_bin, hex(p_bin) as p_bin_hex
        from part_types
    """

    // --- pruning, one partition column at a time ------------------------------
    // Each column takes a different value in each of the two partitions, so a predicate
    // on any one of them must leave exactly one. Pruning has to shrink the WORK and not
    // only the plan line: a name rendered one way for the listing and another for the
    // match reports 1/2 here while scanning nothing at all, which is why the recorded
    // rows sit beside the plan assertion rather than instead of it.
    def prunesToOne = { String column, String predicate ->
        def plan = planOf("""select * from part_types where ${predicate}""")
        assertTrue(plan.contains("partition=1/2"),
                "pruning on ${column} did not reach the connector: ${plan}")
        assertTrue(countIn(plan, "logRanges") >= 1,
                "pruning on ${column} left no range to read: ${plan}")
    }
    prunesToOne("p_str", "p_str = 'cn'")
    prunesToOne("p_char", "p_char = 'c1'")
    prunesToOne("p_bool", "p_bool = true")
    prunesToOne("p_tiny", "p_tiny = 1")
    prunesToOne("p_small", "p_small = 10")
    prunesToOne("p_int", "p_int = 100")
    prunesToOne("p_big", "p_big = 1000")
    prunesToOne("p_date", "p_date = '2026-01-01'")

    order_qt_pruned_str """select id, name from part_types where p_str = 'cn'"""
    order_qt_pruned_int """select id, name from part_types where p_int = 200"""
    order_qt_pruned_date """select id, name from part_types where p_date = '2026-01-02'"""
    order_qt_pruned_bool """select id, name from part_types where p_bool = false"""
    order_qt_pruned_tiny """select id, name from part_types where p_tiny = 2"""

    // Several partition columns at once, which is also the ordinary case for this
    // table: they have to agree rather than each prune on its own.
    order_qt_pruned_multi """
        select id, name from part_types where p_str = 'cn' and p_int = 100 and p_bool = true
    """
    // A combination no partition holds. Pruning to nothing is a correct answer and not
    // an error, and it must not be reached by scanning everything and filtering after.
    def emptyPlan = planOf("""select * from part_types where p_str = 'cn' and p_int = 200""")
    assertTrue(emptyPlan.contains("partition=0/2"),
            "an impossible combination should prune to nothing: ${emptyPlan}")
    order_qt_pruned_impossible """
        select count(*) from part_types where p_str = 'cn' and p_int = 200
    """
    order_qt_pruned_absent """select count(*) from part_types where p_int = 999"""

    // --- the type whose value fluss cannot store ------------------------------
    // Fluss creates such a table happily and names the partition of
    // 2026-01-01 01:02:03.0 as 2026-01-01-01-02-03_0 -- every character it may not
    // hold rewritten, many-to-one. Left alone this reaches fe-core's partition parser,
    // which fails with the mangled name and nothing else: no column, no type, no fluss.
    // The refusal therefore belongs to the connector, and has to name all three.
    test {
        sql """select * from part_ts"""
        exception "its partition column 'p_ts' has fluss type TIMESTAMP(3)"
    }
    test {
        sql """show partitions from part_ts"""
        exception "cannot be read back"
    }
    // DESC still works: the table is describable, it is only unreadable, and the column
    // its user has to change is the one this shows.
    qt_desc_part_ts """desc part_ts"""

    // --- the type whose verdict the catalog decides ---------------------------
    // Fluss names a BINARY partition with the hex text of the bytes. Read as text that
    // is exactly what was written; asked for as a VARBINARY it is not a literal of
    // anything, so the same table is readable through one catalog and not the other.
    sql """drop catalog if exists ${varbinaryCatalog}"""
    sql """
        create catalog ${varbinaryCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "enable.mapping.varbinary" = "true"
        );
    """
    test {
        sql """select id from ${varbinaryCatalog}.fluss_test.part_types"""
        exception "enable.mapping.varbinary"
    }

    // --- a non-STRING partition under a union read ----------------------------
    // Concatenating a lake with the log written after it needs no partition value
    // matched across the halves: each half prunes on its own. So an INT partition works
    // here, and this is what says the rule that stops the primary-key merge below was
    // not quietly applied to everything partitioned.
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

    def intPartPlan = planOf("""select * from lake_part_int""")
    assertTrue(intPartPlan.contains("unionRead=yes"),
            "an INT-partitioned log table should still be read as lake plus log: ${intPartPlan}")
    assertTrue(countIn(intPartPlan, "lakeSplits") >= 1, "no lake splits: ${intPartPlan}")
    assertEquals(1, countIn(intPartPlan, "logRanges"),
            "one partition has a tail and the other does not: ${intPartPlan}")
    order_qt_lake_part_int """select id, name, p_int from lake_part_int"""

    // Both halves must land on the same partition: one of these has a tail, the other
    // is served entirely from the lake.
    def tailPlan = planOf("""select * from lake_part_int where p_int = 1""")
    assertTrue(tailPlan.contains("partition=1/2"), "pruning did not reach the connector: ${tailPlan}")
    assertEquals(1, countIn(tailPlan, "logRanges"), "the partition with a tail lost it: ${tailPlan}")
    order_qt_lake_part_int_tail """select id, name from lake_part_int where p_int = 1"""

    def tieredPlan = planOf("""select * from lake_part_int where p_int = 2""")
    assertEquals(0, countIn(tieredPlan, "logRanges"),
            "a fully tiered partition still has log ranges: ${tieredPlan}")
    order_qt_lake_part_int_tiered """select id, name from lake_part_int where p_int = 2"""

    def unionIntRows = rowsOf("""select id, name, p_int from lake_part_int order by id""")
    def flussOnlyIntRows = rowsOf(
            """select id, name, p_int from ${flussOnlyCatalog}.fluss_test.lake_part_int order by id""")
    assertEquals(flussOnlyIntRows, unionIntRows,
            "lake+log and fluss-only disagree\nfluss-only=${flussOnlyIntRows}\nunion=${unionIntRows}")

    // --- a non-STRING partition under a merge BY KEY --------------------------
    // Merging the halves of a primary-key table means deciding which fluss partition a
    // paimon split belongs to, by comparing the text each side renders that value as --
    // and only STRING is guaranteed to render alike on both. So this table is not
    // merged. Under `required` that is an error naming the reason; the fluss-only read
    // it would otherwise fall back to returns every row anyway, so nothing is lost but
    // the lake's speed.
    test {
        sql """select * from lake_pk_part_int"""
        exception "cannot be read as its lake plus its change log"
    }

    // Under `auto` it falls back instead, and says so in the plan. Without the anchor
    // the fallback and a working merge look identical from the rows alone -- which is
    // exactly the point of the fallback.
    sql """switch ${catalogName}"""
    sql """use fluss_test"""
    sql """set enable_file_scanner_v2 = true"""
    def degradedPlan = planOf("""select * from lake_pk_part_int""")
    assertTrue(degradedPlan.contains("unionRead=no"),
            "the halves should not have been merged: ${degradedPlan}")
    assertTrue(degradedPlan.contains("degraded=partition-type"),
            "the plan should say why it fell back: ${degradedPlan}")
    assertEquals(0, countIn(degradedPlan, "lakeSplits"), "no lake half is read: ${degradedPlan}")
    assertTrue(countIn(degradedPlan, "pkRanges") >= 1, "the whole table comes from fluss: ${degradedPlan}")

    // And the rows are still all of them -- a primary-key table's fluss-only read is the
    // whole table, not the part the lake has not taken yet.
    order_qt_lake_pk_part_int """select id, name, p_int from lake_pk_part_int"""
    order_qt_lake_pk_part_int_pruned """select id, name from lake_pk_part_int where p_int = 1"""

    sql """switch internal"""
    sql """drop catalog if exists ${catalogName}"""
    sql """drop catalog if exists ${varbinaryCatalog}"""
    sql """drop catalog if exists ${unionCatalog}"""
    sql """drop catalog if exists ${flussOnlyCatalog}"""
}
