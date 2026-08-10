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

// Choosing what a statement reads of a tiered table, and how.
//
// Two ways, and they answer different questions. The `$log` suffix names a
// SEGMENT of the table -- the part that is still in the fluss log, the
// complement of `$lake` -- and reads it whatever the catalog is configured to
// do. The session variable names a PATH: it says whether this statement's read
// of the whole table is allowed, or required, to combine the lake with the log,
// and it changes no row that comes back.
//
// So the two are orthogonal, and most of this suite is about keeping them that
// way: a suffix that started listening to the mode would return a different
// number of rows, and a mode that started rewriting the suffix would too.
//
// Fixtures come from docker/thirdparties/docker-compose/fluss/sql/init.sql and
// init-lake-tail.sql, and are frozen: the tiering service is stopped before the
// tail is written, so which rows are in the lake and which are in the log stays
// put for the life of the environment.
suite("test_fluss_read_modes", "p0,external") {
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
    String autoCatalog = "test_fluss_read_modes"
    String offCatalog = "test_fluss_read_modes_off"
    String requiredCatalog = "test_fluss_read_modes_req"
    String cappedCatalog = "test_fluss_read_modes_capped"

    // Four catalogs, because the mode is a catalog property and the point of several of
    // the cases below is what a session variable does ON TOP of each setting of it.
    sql """drop catalog if exists ${autoCatalog}"""
    sql """
        create catalog ${autoCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.lake.paimon.s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin"
        );
    """
    sql """drop catalog if exists ${offCatalog}"""
    sql """
        create catalog ${offCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.lake.paimon.s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin",
            "fluss.union_read.mode" = "disabled"
        );
    """
    sql """drop catalog if exists ${requiredCatalog}"""
    sql """
        create catalog ${requiredCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.lake.paimon.s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin",
            "fluss.union_read.mode" = "required"
        );
    """
    // A ceiling of one record is not a configuration anyone would run; it is how a log
    // tail is made to overflow on a fixture small enough to reason about. See the last
    // group of this suite for what it is for.
    sql """drop catalog if exists ${cappedCatalog}"""
    sql """
        create catalog ${cappedCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.lake.paimon.s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "fluss.lake.paimon.s3.access-key" = "minioadmin",
            "fluss.lake.paimon.s3.secret-key" = "minioadmin",
            "fluss.union_read.max_tail_rows" = "1"
        );
    """

    sql """switch ${autoCatalog}"""
    sql """use fluss_test"""
    // The connector is wired into the v2 file scanner only, and fuzzy sessions randomize
    // this variable.
    sql """set enable_file_scanner_v2 = true"""
    // TIMESTAMP_LTZ renders through the session time zone, and the baseline below records
    // what it rendered as.
    sql """set time_zone = 'Asia/Shanghai'"""
    // Explicitly unset rather than assumed: a session variable set by an earlier suite in
    // the same connection would decide what half of this one reads.
    sql """set fluss_union_read_mode = ''"""

    def colOf = { String query -> sql(query).collect { it[0].toString() } }
    def planOf = { String query ->
        return sql("""explain ${query}""").collect { it[0].toString() }.join("\n")
    }
    def countIn = { String plan, String field ->
        def matcher = (plan =~ /${field}=(\d+)/)
        assertTrue(matcher.find(), "plan has no ${field}: ${plan}")
        return matcher.group(1) as int
    }

    // --- which segment of the table `$log` names -----------------------------
    // Four rows were tiered and two written after, so this is the two.
    order_qt_log_rows """select id, name, price from lake_log\$log"""

    // The property the name promises: `$lake` and `$log` PARTITION the table. Both
    // directions, because the two ways to break a seam are opposite -- a row in neither
    // half (the halves overlap at the boundary and one gets skipped) and a row in both
    // (the boundary is off the other way) -- and a size check alone would let one of each
    // cancel the other out.
    def lakeIds = colOf("""select id from lake_log\$lake order by id""")
    def logIds = colOf("""select id from lake_log\$log order by id""")
    def wholeIds = colOf("""select id from lake_log order by id""")
    assertTrue(lakeIds.intersect(logIds).isEmpty(),
            "a row is in both halves: lake=${lakeIds} log=${logIds}")
    assertEquals(wholeIds.sort { it as int },
            (lakeIds + logIds).sort { it as int },
            "the halves do not add up to the table: lake=${lakeIds} log=${logIds} whole=${wholeIds}")

    // A table the lake has caught up with has an empty complement, and planning says so
    // rather than emitting ranges that read nothing.
    def coldPlan = planOf("""select * from lake_cold\$log""")
    assertEquals(0, countIn(coldPlan, "logRanges"), "a caught-up table still has log ranges: ${coldPlan}")
    order_qt_cold_log_count """select count(*) from lake_cold\$log"""

    // Partitioned: only the partition that got a tail has anything in the log. A reader
    // that took one partition's starting offsets for another's would return rows here
    // from the partition that has none.
    order_qt_part_log_rows """select id, name, dt from lake_part\$log"""

    // A hundred thousand rows in the lake and a thousand after it, ids contiguous across
    // the two, so what the complement holds has a closed form: 100001..101000, whose sum
    // is (100001 + 101000) * 1000 / 2. Nothing here was read off a screen -- a boundary
    // computed per bucket instead of per table lands on a different sum whatever the
    // buckets happen to be.
    order_qt_big_log_closed_form """
        select count(*) as tail_rows, min(id) as first_id, max(id) as last_id, sum(id) as id_sum
        from big_log\$log
    """
    order_qt_big_log_halves """
        select (select count(*) from big_log\$lake) as lake_rows,
               (select count(*) from big_log\$log) as log_rows,
               (select count(*) from big_log) as whole_rows
    """

    // Every type, read out of the log half alone. The row the log holds for this fixture
    // is the all-NULL one, which is the row a decoder off by one column gets wrong.
    //
    // The MAP goes in as its sorted keys and sorted values rather than whole: the order a
    // map renders its entries in is neither the order they were written in nor stable
    // across runs.
    order_qt_types_log """
        select id, f_boolean, f_tinyint, f_smallint, f_int, f_bigint, f_float, f_double,
               f_decimal, f_char, f_string, hex(f_binary) as f_binary_hex,
               hex(f_bytes) as f_bytes_hex, f_date, f_timestamp, f_timestamp_ltz,
               f_array, array_sort(map_keys(f_map)) as f_map_keys,
               array_sort(map_values(f_map)) as f_map_values, f_row
        from lake_types\$log
    """

    // --- `$log` is the same table, not a table of its own ---------------------
    // Same columns, same types, same order as the base table: it is the base table read
    // from a starting point. Compared column by column rather than recorded, because two
    // recorded blocks drifting together would still match each other. Only the first
    // three fields are compared -- DESC renders a sys table's Extra column differently
    // from a base table's, which is fe-core's business and not this connector's.
    def columnsOf = { String table ->
        return sql("""desc ${table}""").collect { row -> [row[0], row[1], row[2]] }
    }
    assertEquals(columnsOf("lake_log"), columnsOf("lake_log\$log"),
            "the log half of a table does not describe as the table")

    // The lake half does not: paimon holds three columns fluss adds on the way in, and
    // that difference is what says `$log` was not implemented as "the lake table again".
    assertEquals(columnsOf("lake_log").size() + 3, columnsOf("lake_log\$lake").size(),
            "the lake half no longer carries the three fluss system columns")
    order_qt_desc_log_half """desc lake_log\$log"""

    // --- the plan anchor -----------------------------------------------------
    // The one failure `$log` has that changes nothing observable: reading the whole log
    // instead of its tail. Same plan shape, same range count, same columns -- only the
    // starting offsets differ, and those come from the lake snapshot. So the anchor is
    // that a lake snapshot was resolved at all (unionRead=yes) while no lake range was
    // planned (lakeSplits=0). Reading `readMode=log` alone would not notice.
    def logPlan = planOf("""select * from lake_log\$log""")
    assertTrue(logPlan.contains("flussScan: readMode=log, unionRead=yes, lakeSplits=0"),
            "the log half lost its lake boundary: ${logPlan}")
    // Two rows over three buckets, and which bucket a row lands in is the writer's
    // business, so this is a bound rather than a number.
    def logRanges = countIn(logPlan, "logRanges")
    assertTrue(logRanges >= 1 && logRanges <= 3, "unexpected log range count: ${logPlan}")

    // The base table under the same catalog still plans as itself: a suffix that leaked
    // into the base table's plan would read a fraction of it.
    assertTrue(planOf("""select * from lake_log""").contains("flussScan: readMode=default"),
            "the base table was planned as a half")

    // --- what `$log` refuses -------------------------------------------------
    // A primary-key table's log past the lake snapshot is a change stream -- inserts,
    // updates and deletes against keys the lake already holds -- so "the part not in the
    // lake" is not a set of rows it can return. Answering with the change records would
    // return superseded rows as if they were current.
    test {
        sql """select * from lake_pk\$log"""
        exception "has a primary key"
    }

    // No lake snapshot means no point to start from. The whole table is in the log, and
    // that is what the base table already reads, so answering with it would make `$log`
    // mean something different for this table than for every other one.
    test {
        sql """select * from lake_empty\$log"""
        exception "has no readable lake snapshot"
    }

    // A table with no lake at all announces no halves, so the name resolves to nothing.
    // The message is the engine's, which is the honest one: nothing offered this name.
    test {
        sql """select * from log_basic\$log"""
        exception "Unknown sys table"
    }

    // The other half of the same table, on a lake that exists and holds nothing: the
    // fluss coordinator creates the paimon table when the fluss table is created, so
    // `$lake` reads an empty table rather than failing. That the two halves answer
    // differently here is right -- fluss has no readable snapshot yet, while the lake
    // table genuinely holds no rows -- and this pins the pair so that neither answer
    // quietly becomes the other's.
    order_qt_empty_lake_count """select count(*) from lake_empty\$lake"""
    test {
        sql """select * from ${requiredCatalog}.fluss_test.lake_empty"""
        exception "no readable lake snapshot"
    }

    // --- the suffix and the mode do not listen to each other ------------------
    // `$log` names a segment; the mode names a path to the whole table. A `$log` scan
    // under a catalog that forbids combining the halves still reads the same two rows,
    // because it is not combining anything.
    order_qt_log_rows_mode_disabled """select id, name from ${offCatalog}.fluss_test.lake_log\$log"""
    sql """set fluss_union_read_mode = 'disabled'"""
    order_qt_log_rows_session_disabled """select id, name from lake_log\$log"""

    // A value the connector cannot make sense of is refused wherever it is read, `$log`
    // included: a statement whose setting is a typo is a statement nobody can serve, and
    // reporting it only sometimes would make the same typo look intermittent.
    sql """set fluss_union_read_mode = 'sometimes'"""
    test {
        sql """select * from lake_log\$log"""
        exception "fluss_union_read_mode"
    }
    sql """set fluss_union_read_mode = ''"""

    // --- the mode changes the path and not the rows ---------------------------
    // The same table down all three settings of the session variable, under a catalog
    // that states none of its own. The anchors differ; the rows must not. This is the
    // whole claim the setting makes -- it picks how the table is read, not what the
    // table is -- and a setting that silently dropped a half would pass every other
    // assertion in this suite.
    def rowsUnderMode = { String mode, String query ->
        sql """set fluss_union_read_mode = '${mode}'"""
        return sql(query).collect { row -> row.collect { it.toString() } }
    }
    def followsCatalog = rowsUnderMode("", "select id, name, price from lake_log order by id")
    def viaFlussOnly = rowsUnderMode("disabled", "select id, name, price from lake_log order by id")
    def viaLakePlusLog = rowsUnderMode("required", "select id, name, price from lake_log order by id")
    assertEquals(followsCatalog, viaFlussOnly, "the fluss-only path returned different rows")
    assertEquals(followsCatalog, viaLakePlusLog, "the lake-plus-log path returned different rows")

    sql """set fluss_union_read_mode = ''"""
    def unsetPlan = planOf("""select * from lake_log""")
    assertTrue(unsetPlan.contains("mode=auto"), "a blank session value did not fall back: ${unsetPlan}")
    sql """set fluss_union_read_mode = 'disabled'"""
    def offPlan = planOf("""select * from lake_log""")
    assertTrue(offPlan.contains("unionRead=no"), "the session did not turn the union read off: ${offPlan}")
    assertTrue(offPlan.contains("mode=disabled(session)"),
            "the plan does not say where the mode came from: ${offPlan}")
    sql """set fluss_union_read_mode = 'required'"""
    def onPlan = planOf("""select * from lake_log""")
    assertTrue(onPlan.contains("unionRead=yes"), "the session did not turn the union read on: ${onPlan}")

    // Either direction: the statement's setting wins over the catalog's, and a blank one
    // hands the decision back. Blank is the only way to take an override back -- there is
    // no "unset" for a session variable that has been set -- so it has to mean "not set"
    // rather than "auto".
    sql """set fluss_union_read_mode = 'required'"""
    def onOverOffPlan = planOf("""select * from ${offCatalog}.fluss_test.lake_log""")
    assertTrue(onOverOffPlan.contains("unionRead=yes"),
            "the session did not override a catalog that says disabled: ${onOverOffPlan}")
    sql """set fluss_union_read_mode = 'disabled'"""
    def offOverOnPlan = planOf("""select * from ${requiredCatalog}.fluss_test.lake_log""")
    assertTrue(offOverOnPlan.contains("unionRead=no"),
            "the session did not override a catalog that says required: ${offOverOnPlan}")
    sql """set fluss_union_read_mode = ''"""
    def backToCatalogPlan = planOf("""select * from ${requiredCatalog}.fluss_test.lake_log""")
    assertTrue(backToCatalogPlan.contains("unionRead=yes"),
            "blanking the session did not hand the decision back: ${backToCatalogPlan}")
    assertTrue(backToCatalogPlan.contains("mode=required")
            && !backToCatalogPlan.contains("(session)"),
            "the plan still credits the session: ${backToCatalogPlan}")

    // The name in the message has to be the name the user typed. Nothing checks at
    // compile time that the connector spells this variable the way fe-core declares it --
    // the connector looks it up by name in a map -- so a message naming something else
    // would be the first sign, and only if someone is reading it.
    sql """set fluss_union_read_mode = 'sometimes'"""
    test {
        sql """select * from lake_log"""
        exception "session variable 'fluss_union_read_mode'"
    }
    sql """set fluss_union_read_mode = ''"""

    // --- the decision has to hold for the whole of planning -------------------
    // Planning a primary-key union read asks fluss for the log tails AFTER it has decided
    // to attempt one, and what it does when a tail is too large depends on the mode
    // again. A second reader of the mode that consulted the catalog instead of this
    // statement would degrade to a fluss-only read here -- which returns the right rows,
    // in a plan whose only difference is a word -- and `required` would have been
    // silently abandoned half way through.
    //
    // So: the same catalog, whose own mode is auto, twice. Once it degrades; once the
    // statement says required and it must fail instead. And the failure has to name the
    // setting that is actually in force.
    sql """set fluss_union_read_mode = ''"""
    def degradedPlan = planOf("""select * from ${cappedCatalog}.fluss_test.lake_pk""")
    assertTrue(degradedPlan.contains("degraded=tail-too-large"),
            "the tail ceiling did not bite: ${degradedPlan}")
    assertTrue(degradedPlan.contains("unionRead=no"), "degraded but still a union read: ${degradedPlan}")
    order_qt_capped_degraded_rows """select id, name from ${cappedCatalog}.fluss_test.lake_pk"""

    sql """set fluss_union_read_mode = 'required'"""
    test {
        sql """select * from ${cappedCatalog}.fluss_test.lake_pk"""
        exception "session variable 'fluss_union_read_mode'"
    }
    sql """set fluss_union_read_mode = ''"""

    sql """switch internal"""
    sql """drop catalog if exists ${autoCatalog}"""
    sql """drop catalog if exists ${offCatalog}"""
    sql """drop catalog if exists ${requiredCatalog}"""
    sql """drop catalog if exists ${cappedCatalog}"""
}
