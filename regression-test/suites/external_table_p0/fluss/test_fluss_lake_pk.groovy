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

// Reading a PRIMARY-KEY table that is tiered into a lake.
//
// Its two halves cannot be concatenated the way a log table's are: the log
// carries updates and deletes of rows the lake already holds, so reading both
// and adding them up returns superseded and deleted rows. They are merged BY
// KEY instead -- the lake half is read column by column and a lake row is
// dropped when the log tail that follows it touched its key at all, while the
// tail contributes the state it ended in.
//
// The results below were recorded before that merge existed, off a read that
// went to fluss alone. That read is the WHOLE table rather than a part of it,
// because fluss keeps a primary-key table's state in its own kv store and
// tiering copies rows into the lake rather than moving them out -- so it is a
// second, independent answer to every query here, and the merge has to
// reproduce it row for row. Nothing in the RESULTS says which one ran, which is
// exactly what makes them a baseline; the plan says it, and this suite asserts
// both. When these recorded blocks change, the merge has broken -- that is what
// recording them is for.
//
// Fixtures come from docker/thirdparties/docker-compose/fluss/sql/init.sql and
// init-lake-tail.sql, and are frozen -- the tiering service is stopped before
// the tail is written, so the two halves stay apart for as long as the
// environment lives.
suite("test_fluss_lake_pk", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String coordinatorPort = context.config.otherConfigs.get("fluss_coordinator_port")
    String bootstrapServers = "${externalEnvIp}:${coordinatorPort}"
    String autoCatalog = "test_fluss_lake_pk"
    String flussOnlyCatalog = "test_fluss_lake_pk_off"
    String requiredCatalog = "test_fluss_lake_pk_required"

    // Three catalogs, one per read mode, all live at once: the mode is a catalog
    // property, and comparing what the modes return is most of the point here.
    sql """drop catalog if exists ${autoCatalog}"""
    sql """
        create catalog ${autoCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}"
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
    sql """drop catalog if exists ${requiredCatalog}"""
    sql """
        create catalog ${requiredCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.union_read.mode" = "required"
        );
    """

    sql """switch ${autoCatalog}"""
    sql """use fluss_test"""
    // The C++ glue exists only for the v2 file scanner, and the session variable
    // that picks between them is randomised by the fuzzy mode this pipeline runs.
    sql """set enable_file_scanner_v2 = true"""

    def rowsOf = { String query -> sql(query).collect { row -> row.collect { it.toString() } } }
    def planOf = { String query ->
        return sql("""explain ${query}""").collect { it[0].toString() }.join("\n")
    }

    // --- the two halves, and what the table IS -------------------------------
    // The lake was frozen mid-history: it holds row 2 already updated (that
    // update happened before tiering) and rows 1 and 3 as first written.
    order_qt_lake_side """select id, name from lake_pk\$lake"""

    // The log tail that follows disagrees with the lake in all three possible
    // ways: row 3 updated, row 1 deleted, row 4 added. The table is therefore
    // neither half, and no concatenation of the two produces it -- adding them up
    // would return the deleted row 1, two versions of row 3, and row 4.
    order_qt_front_door """select id, name from lake_pk"""

    // --- the merge lands on what a fluss-only read returns --------------------
    // The same rows out of an entirely different reader: this block and the one
    // above have to stay identical to each other. That is the whole argument for
    // the merge being right, because fluss serves a primary-key table in full and
    // has no lake half to get wrong.
    order_qt_fluss_only """select id, name from ${flussOnlyCatalog}.fluss_test.lake_pk"""

    // --- the plan says the lake really was read ------------------------------
    // Without this the suite would still pass if the merge quietly stopped
    // happening and the read fell back to fluss alone -- the rows would be the
    // same ones. One bucket: one lake split, suppressed by the tail of that
    // bucket, and one range replaying the tail itself.
    def plan = planOf("""select * from lake_pk""")
    assertTrue(
            plan.contains("flussScan: unionRead=yes, lakeSplits=1, suppressedLakeSplits=1, "
                    + "logRanges=0, pkRanges=0, pkTailRanges=1, mode=auto"),
            "not planned as a lake+tail merge of one bucket: ${plan}")

    // --- required merges rather than refusing --------------------------------
    // That mode exists so a union-read test cannot pass by falling back. It used
    // to refuse a primary-key table outright; now it has to produce the merge,
    // and produce the same rows as the two modes that may fall back.
    def requiredPlan = planOf("""select * from ${requiredCatalog}.fluss_test.lake_pk""")
    assertTrue(requiredPlan.contains("unionRead=yes"), "required did not merge: ${requiredPlan}")
    assertTrue(requiredPlan.contains("mode=required"), "unexpected mode: ${requiredPlan}")

    // Three modes, three catalogs, one answer -- compared here rather than by
    // three recorded blocks, because what is being asserted is that they AGREE.
    // Two identical recordings only look alike to a reader.
    def query = { String catalog -> "select id, name from ${catalog}.fluss_test.lake_pk order by id" }
    def merged = rowsOf(query(autoCatalog))
    assertEquals(rowsOf(query(flussOnlyCatalog)), merged,
            "auto and disabled disagree on lake_pk")
    assertEquals(rowsOf(query(requiredCatalog)), merged,
            "required and auto disagree on lake_pk")

    // --- the ordinary things still work on this path -------------------------
    // Projection, predicates and count all run over the merged pair. The
    // projection one carries a second load: the merge needs the key column to
    // suppress by, and the engine keeps it in the scan for that reason alone --
    // so a single-column result here is also the assertion that the kept column
    // stays out of the answer.
    order_qt_count """select count(*) from lake_pk"""
    order_qt_projection """select name from lake_pk where id = 3"""
    // The deleted row stays gone under a predicate too, not only in a full scan:
    // the delete is a log record, and applying it on one path but not the other is
    // how a "row that only shows up sometimes" bug looks.
    order_qt_deleted_row """select name from lake_pk where id = 1"""
    order_qt_log_only_key """select id, name from lake_pk where id > 3"""

    sql """switch internal"""
    sql """drop catalog if exists ${autoCatalog}"""
    sql """drop catalog if exists ${flussOnlyCatalog}"""
    sql """drop catalog if exists ${requiredCatalog}"""
}
