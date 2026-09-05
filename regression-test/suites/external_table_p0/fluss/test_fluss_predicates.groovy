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

// Predicates that are more than one comparison.
//
// The other suites filter with a single predicate, which is the shape that cannot
// go wrong in an interesting way. Combinations can: a disjunction over a partition
// column has to keep every partition it names rather than the last one; a
// conjunction of a partition predicate and a data predicate must prune on the first
// without dropping rows the second would have kept; a NOT around a partition
// predicate must not prune at all. And for a table read as lake plus log, a
// predicate that reached only one half returns an answer that is short by exactly
// the other half's matching rows -- a plausible number, always.
//
// This suite pins WHAT the answers are; where a predicate is evaluated is not
// asserted, because it is the optimizer's choice and pinning it would fail on the
// day the optimizer gets better rather than worse. The union-read blocks compare
// the two read paths instead, which is the property that has to hold however the
// predicate was routed.
//
// Fixtures: docker/thirdparties/docker-compose/fluss/sql/init.sql.
suite("test_fluss_predicates", "p0,external") {
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
    String catalogName = "test_fluss_predicates"
    String flussOnlyCatalog = "test_fluss_predicates_off"

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
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
    sql """switch ${catalogName}"""
    sql """use fluss_test"""
    sql """set enable_file_scanner_v2 = true"""
    // The timestamp columns below are compared against literals, and TIMESTAMP_LTZ
    // renders through the session time zone.
    sql """set time_zone = 'Asia/Shanghai'"""

    def rowsOf = { String query -> sql(query).collect { row -> row.collect { it.toString() } } }
    def planOf = { String query ->
        return sql("""explain ${query}""").collect { it[0].toString() }.join("\n")
    }
    def compareModes = { String query ->
        def union = rowsOf("""${query}""")
        def flussOnly = rowsOf("""${query}""".replace("from ", "from ${flussOnlyCatalog}.fluss_test."))
        assertEquals(flussOnly, union,
                "lake+log and fluss-only disagree for: ${query}\nfluss-only=${flussOnly}\nunion=${union}")
    }

    // --- conjunction, disjunction, negation on a log table --------------------
    order_qt_and """select id, name from log_basic where id > 1 and price < 25.00"""
    order_qt_or """select id, name from log_basic where id = 1 or price > 25.00"""
    // The parenthesised mixture: a reader that flattened it into a chain of ANDs or
    // ORs answers with everything or nothing, and both look like working code.
    order_qt_and_or """
        select id, name from log_basic
        where (id = 1 and price < 15.00) or (id = 3 and price > 25.00)
    """
    order_qt_or_and """
        select id, name from log_basic
        where (id = 1 or id = 3) and price > 25.00
    """
    order_qt_not """select id, name from log_basic where not (id = 1 or price > 25.00)"""
    order_qt_not_between """select id from log_basic where id not between 2 and 3"""
    order_qt_in """select id, name from log_basic where id in (1, 3) and name <> 'carol'"""
    order_qt_not_in """select id from log_basic where id not in (1, 2)"""
    order_qt_like """select id from log_basic where name like '%o%' and name not like 'c%'"""
    // A predicate that selects nothing, and one that selects everything: the two ends
    // of the range, where an inverted condition is easiest to mistake for a working one.
    order_qt_always_false """select count(*) from log_basic where id > 0 and id < 0"""
    order_qt_always_true """select count(*) from log_basic where id > 0 or name is not null"""

    // --- NULLs, which no comparison is true of --------------------------------
    // The all-NULL row is row 3 of log_types. `f_int <> 3` must not return it, `is null`
    // must, and `not (f_int = 3)` must not either -- three-valued logic is exactly what
    // a filter pushed down and re-implemented gets wrong.
    order_qt_is_null """select id from log_types where f_int is null"""
    order_qt_is_not_null """select id from log_types where f_int is not null"""
    order_qt_ne_excludes_null """select id from log_types where f_int <> 3"""
    order_qt_not_eq_excludes_null """select id from log_types where not (f_int = 3)"""
    order_qt_null_or """select id from log_types where f_int is null or f_int > 0"""
    order_qt_null_and """select id from log_types where f_string is null and f_boolean is null"""

    // Across types, in one condition. A predicate list that stopped at the first column
    // it could not push would return the rows the rest were meant to exclude.
    order_qt_mixed_types """
        select id from log_types
        where f_boolean = true and f_tinyint > 0 and f_decimal > 0 and f_char = 'char1'
              and f_date = '2026-01-01' and f_string like 'string%'
    """
    order_qt_mixed_types_or """
        select id from log_types
        where f_bigint = 4 or f_double < 0 or (f_float is null and f_smallint is null)
    """
    // Reaching into a complex value from a predicate: the element has to be decoded
    // before it can be compared, so this cannot have been answered by the scanner.
    order_qt_nested_elements """
        select id from log_types
        where f_array[1] = 1 and f_map['k1'] = 1 and struct_element(f_row, 'r_int') = 1
    """

    // --- a partition column in a compound predicate ---------------------------
    // Pruning takes the partitions the predicate leaves possible, so a disjunction has
    // to keep both of the partitions it names and a negation has to keep every one it
    // does not exclude. The plan line is asserted alongside the rows because a
    // conservative pruner returning too many partitions still gives the right rows.
    def partOrPlan = planOf("""select * from log_part where dt = '20260101' or dt = '20260103'""")
    assertTrue(partOrPlan.contains("partition=2/3"),
            "a disjunction over partitions should keep both: ${partOrPlan}")
    order_qt_part_or """select id, name, dt from log_part where dt = '20260101' or dt = '20260103'"""

    def partInPlan = planOf("""select * from log_part where dt in ('20260101', '20260102')""")
    assertTrue(partInPlan.contains("partition=2/3"), "IN over partitions: ${partInPlan}")
    order_qt_part_in """select id, dt from log_part where dt in ('20260101', '20260102') and id > 1"""

    def partNotPlan = planOf("""select * from log_part where dt <> '20260101'""")
    assertTrue(partNotPlan.contains("partition=2/3"), "a negated partition predicate: ${partNotPlan}")
    order_qt_part_ne """select id, dt from log_part where dt <> '20260101'"""

    // A partition predicate and a data predicate together: the first prunes, the second
    // must still be applied to what is left rather than assumed satisfied.
    def partAndDataPlan = planOf("""select * from log_part where dt = '20260101' and id = 2""")
    assertTrue(partAndDataPlan.contains("partition=1/3"), "pruning still applies: ${partAndDataPlan}")
    order_qt_part_and_data """select id, name from log_part where dt = '20260101' and id = 2"""

    // A disjunction that mixes the two cannot prune at all: the row it selects lives in
    // a partition the partition predicate excludes.
    def partOrDataPlan = planOf("""select * from log_part where dt = '20260101' or id = 4""")
    assertTrue(partOrDataPlan.contains("partition=3/3"),
            "a disjunction with a data predicate must not prune: ${partOrDataPlan}")
    order_qt_part_or_data """select id, name, dt from log_part where dt = '20260101' or id = 4"""

    order_qt_part_absent """select count(*) from log_part where dt = '20261231'"""

    // --- a primary-key table, where a predicate is applied after the merge ----
    // The change log holds superseded and deleted rows; a predicate evaluated against
    // it rather than against the merged view brings them back. 'k2' is the value key 2
    // used to have, and key 3 was deleted.
    order_qt_pk_and """select id, name from pk_basic where id >= 2 and score > 3.0"""
    order_qt_pk_or """select id, name from pk_basic where name = 'k1' or score > 20.0"""
    order_qt_pk_stale_value """select count(*) from pk_basic where name = 'k2' or id = 3"""
    order_qt_pk_not """select id, name from pk_basic where not (id = 1) and name is not null"""
    order_qt_pk_in """select id, score from pk_basic where id in (1, 2, 3) and score is not null"""

    // --- a table read as lake plus log ----------------------------------------
    // Whichever half the optimizer pushed the predicate to, both halves have to end up
    // filtered by it. The comparison is the check: a predicate that reached only the
    // lake returns the log's matching rows unfiltered, and one that reached only the log
    // returns the lake's -- either way a plausible count.
    order_qt_union_and """select id, name from lake_log where id > 2 and price < 6.00"""
    compareModes("""select id, name from lake_log where id > 2 and price < 6.00 order by id""")

    order_qt_union_or """select id, name from lake_log where id = 1 or id = 6"""
    compareModes("""select id, name from lake_log where id = 1 or id = 6 order by id""")

    order_qt_union_not """select id from lake_log where not (id between 2 and 5)"""
    compareModes("""select id from lake_log where not (id between 2 and 5) order by id""")

    order_qt_union_like """select id, name from lake_log where name like 'hot%' or name like '%1'"""
    compareModes("""select id, name from lake_log where name like 'hot%' or name like '%1' order by id""")

    // A predicate that only the lake half can satisfy, and one that only the log half
    // can: each is the case where returning the other half unfiltered is invisible in
    // the row count of the first.
    order_qt_union_lake_only """select id, name from lake_log where id <= 4 and price < 5.00"""
    order_qt_union_log_only """select id, name from lake_log where id >= 5"""
    order_qt_union_neither """select count(*) from lake_log where id > 100"""

    // The same over a partitioned lake table, where pruning and the seam interact: one
    // partition has a tail and the other does not.
    order_qt_union_part_or """
        select id, name, dt from lake_part where dt = '20260101' or dt = '20260102'
    """
    compareModes("""
        select id, name, dt from lake_part where dt = '20260101' or dt = '20260102' order by id
    """)
    order_qt_union_part_and """select id from lake_part where dt = '20260101' and id > 2"""
    compareModes("""select id from lake_part where dt = '20260101' and id > 2 order by id""")

    // And over a primary-key lake table, where the predicate is applied to the merged
    // view of both halves: key 1 was deleted by the tail and key 3 updated by it.
    order_qt_union_pk_and """select id, name from lake_pk where id >= 2 and name like '%hot'"""
    compareModes("""select id, name from lake_pk where id >= 2 and name like '%hot' order by id""")
    order_qt_union_pk_or """select id, name from lake_pk where id = 1 or id = 4"""
    compareModes("""select id, name from lake_pk where id = 1 or id = 4 order by id""")

    sql """switch internal"""
    sql """drop catalog if exists ${catalogName}"""
    sql """drop catalog if exists ${flussOnlyCatalog}"""
}
