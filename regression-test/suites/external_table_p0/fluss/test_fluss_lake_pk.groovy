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
// and adding them up returns superseded and deleted rows. Merging them BY KEY is
// not implemented, so such a table is read from fluss alone -- and that read is
// the WHOLE table, not a part of it, because fluss keeps a primary-key table's
// state in its own kv store and tiering copies rows into the lake rather than
// moving them out. (A log table has no such guarantee, which is why IT is read
// as a union and refused when the lake cannot be reached.)
//
// This suite is therefore two things at once. It checks today's read, and it
// pins the answer a future lake+log merge will have to reproduce row for row:
// the same queries, over a fixture whose lake and log deliberately disagree in
// every way they can. When that merge lands, these recorded results must not
// change -- that is what recording them is for.
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

    // --- auto falls back to exactly what disabled asks for -------------------
    // The same rows out of the same code path, not "something equivalent": this
    // result and the one above have to stay identical to each other, and a
    // fallback that quietly read something else would break one of them.
    order_qt_fluss_only """select id, name from ${flussOnlyCatalog}.fluss_test.lake_pk"""

    // --- the plan says the lake was not read ---------------------------------
    // Nothing in the RESULT distinguishes this read from a correct merge -- that
    // is exactly what makes it a baseline -- so only the plan can say which one
    // ran. One bucket, one primary-key range, no lake splits.
    def plan = planOf("""select * from lake_pk""")
    assertTrue(
            plan.contains("flussScan: unionRead=no, lakeSplits=0, logRanges=0, pkRanges=1, mode=auto"),
            "not planned as a fluss-only primary-key read of one bucket: ${plan}")

    // --- required refuses rather than falling back ---------------------------
    // That mode exists so a union-read test cannot pass without a union read. No
    // primary-key table can satisfy it yet, and the refusal has to say so --
    // "wait for tiering to commit" would send the reader down a dead end.
    test {
        sql """select * from ${requiredCatalog}.fluss_test.lake_pk"""
        exception "not implemented yet"
    }

    // --- the ordinary things still work on this path -------------------------
    // Projection, predicates and count all go through the kv snapshot merged with
    // the change log, which is a different reader from the log tables' and is
    // exercised here for a lake table.
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
