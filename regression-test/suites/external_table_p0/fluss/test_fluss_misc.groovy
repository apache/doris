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

// The rest of what a catalog has to answer for: tables with nothing in them, the
// ways a query over one is supposed to fail, and the ordinary SQL a user reaches
// for once the table reads at all.
//
// The suites next to this one each follow one path in depth. This one is the
// breadth: a fluss table has to behave like a table -- joinable, insertable-from,
// a subquery, one side of a union -- and it has to fail in a way that says what to
// do, on the handful of things it genuinely cannot do.
//
// Fixtures: docker/thirdparties/docker-compose/fluss/sql/init.sql.
suite("test_fluss_misc", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String coordinatorPort = context.config.otherConfigs.get("fluss_coordinator_port")
    String bootstrapServers = "${externalEnvIp}:${coordinatorPort}"
    String catalogName = "test_fluss_misc"
    String requiredCatalog = "test_fluss_misc_required"
    String internalDb = "test_fluss_misc_internal"

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

    // --- tables with nothing in them ------------------------------------------
    // A primary-key table that was never written to is not the same shape as an empty
    // log table: its read starts from a kv snapshot, and there is none, so the code that
    // has to notice is not the code that reads no log records.
    order_qt_pk_empty_count """select count(*) from pk_empty"""
    order_qt_pk_empty_rows """select id, name from pk_empty"""
    def pkEmptyPlan = planOf("""select * from pk_empty""")
    // An engine that drops a split-less scan altogether is as correct as one that keeps
    // the node, so what is pinned is that no range was planned either way.
    def pkEmptyMatcher = (pkEmptyPlan =~ /pkRanges=(\d+)/)
    assertTrue(!pkEmptyMatcher.find() || pkEmptyMatcher.group(1) == "0",
            "an empty primary-key table planned ranges: ${pkEmptyPlan}")

    // Aggregates over an empty table, where COUNT and the rest disagree by design:
    // count is 0 and every other aggregate is NULL, and a scanner that reported an
    // empty batch as "no rows read" rather than "zero rows" gets one of them wrong.
    order_qt_empty_aggregates """
        select count(*), count(id), sum(id), min(id), max(id), avg(id) from pk_empty
    """
    order_qt_log_empty_aggregates """
        select count(*), count(id), sum(id), min(id), max(id) from log_empty
    """
    order_qt_empty_join """
        select count(*) from log_empty a join log_basic b on a.id = b.id
    """

    // A lake table tiering has never committed anything for. Under auto there is
    // nothing to read the lake half from, so the fluss-only read is the whole answer --
    // which for a table nobody wrote to is no rows.
    def lakeEmptyPlan = planOf("""select * from lake_empty""")
    assertTrue(lakeEmptyPlan.contains("unionRead=no"),
            "there is no lake snapshot to read: ${lakeEmptyPlan}")
    assertTrue(lakeEmptyPlan.contains("lakeSplits=0"), "no lake half: ${lakeEmptyPlan}")
    order_qt_lake_empty_count """select count(*) from lake_empty"""

    // Under required the same table is an error: falling back is precisely what that
    // mode forbids, and "wait for the tiering service" is the thing its user needs to
    // be told rather than left to infer from an empty result.
    sql """drop catalog if exists ${requiredCatalog}"""
    sql """
        create catalog ${requiredCatalog} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}",
            "fluss.union_read.mode" = "required"
        );
    """
    test {
        sql """select * from ${requiredCatalog}.fluss_test.lake_empty"""
        exception "has no readable lake snapshot yet"
    }

    // --- the one fluss type Doris has nowhere to put --------------------------
    // TIME is marked unsupported rather than mapped to a string or to elapsed millis:
    // either of those hands back a value whose meaning differs from the source, and
    // nothing downstream would ever reveal it. So the column is describable and
    // unreadable, and the rest of the table is unaffected -- which is what makes the
    // marker useful instead of merely honest.
    qt_desc_log_time """desc log_time"""
    order_qt_time_other_columns """select id, name from log_time"""
    order_qt_time_count """select count(*) from log_time"""
    test {
        sql """select f_time from log_time"""
        exception "UNSUPPORTED"
    }
    test {
        sql """select * from log_time"""
        exception "UNSUPPORTED"
    }

    // --- things that are simply not there -------------------------------------
    test {
        sql """select * from no_such_table"""
        exception "no_such_table"
    }
    test {
        sql """select no_such_column from log_basic"""
        exception "no_such_column"
    }
    test {
        sql """select * from no_such_db.log_basic"""
        exception "no_such_db"
    }

    // --- a fluss table is a table ---------------------------------------------
    // Two fluss tables joined to each other. The scan of each is the same one the other
    // suites make; what is new is two of them in one fragment, which is where a scanner
    // holding state on something shared between nodes would show up.
    order_qt_join_fluss """
        select b.id, b.name, t.f_string
        from log_basic b join log_types t on b.id = t.id
    """
    order_qt_join_pk """
        select p.id, p.name, l.name as log_name
        from pk_basic p join log_basic l on p.id = l.id
    """
    // A join over the seam of a table read as lake plus log: rows from both halves have
    // to reach the join operator, not just the half that was planned first.
    order_qt_join_lake """
        select l.id, l.name, c.name as cold_name
        from lake_log l left join lake_cold c on l.id = c.id
    """
    order_qt_join_self """
        select a.id, a.name, b.name
        from log_part a join log_part b on a.dt = b.dt and a.id < b.id
    """

    // Subqueries, correlated and not.
    order_qt_subquery_in """
        select id, name from log_basic where id in (select id from pk_basic where score > 2.0)
    """
    order_qt_subquery_exists """
        select id from log_basic b where exists (select 1 from log_types t where t.id = b.id)
    """
    order_qt_subquery_scalar """
        select id, name from log_basic where price > (select min(price) from log_basic)
    """
    order_qt_cte """
        with hot as (select id, name from lake_log where id >= 5)
        select count(*), min(id) from hot
    """

    // Set operations, where a fluss scan is one arm of several.
    order_qt_union_all """
        select id, name from log_basic union all select id, name from lake_cold
    """
    order_qt_union_distinct """
        select id from log_basic union select id from lake_cold
    """
    order_qt_except """
        select id from lake_log except select id from lake_cold
    """
    order_qt_intersect """
        select id from lake_log intersect select id from lake_cold
    """

    // Ordinary aggregation and window shapes over a fluss scan.
    order_qt_aggregates """
        select count(*), count(distinct name), min(price), max(price), sum(price), avg(id)
        from log_basic
    """
    order_qt_group_having """
        select dt, count(*) as c from log_part group by dt having count(*) > 1
    """
    order_qt_order_limit """select id, name from log_basic order by price desc limit 2"""
    order_qt_limit_offset """select id from log_basic order by id limit 1 offset 1"""
    order_qt_star """select * from log_basic"""
    order_qt_expression """select id * 2 as doubled, upper(name) from log_basic where id < 3"""
    order_qt_case """
        select id, case when price > 20 then 'high' else 'low' end as band from log_basic
    """

    // --- reading a fluss table into an internal one ---------------------------
    // The whole point of the connector for most users, and the one path where the
    // scanned rows leave the query that scanned them: they are typed, materialized and
    // written. A type that reads correctly but describes itself wrongly fails here and
    // nowhere else.
    sql """switch internal"""
    sql """drop database if exists ${internalDb}"""
    sql """create database ${internalDb}"""
    sql """use ${internalDb}"""

    sql """
        create table copied (
            id int,
            name varchar(64),
            price decimalv3(10, 2)
        ) distributed by hash(id) buckets 1 properties ("replication_num" = "1")
    """
    sql """
        insert into copied select id, name, price from ${catalogName}.fluss_test.log_basic
    """
    order_qt_copied """select id, name, price from copied"""

    // CTAS, which takes the column types from the connector rather than from a table
    // someone wrote by hand -- so this is where the declared Doris type of every mapped
    // fluss type has to be a type Doris can actually create a column of.
    sql """
        create table ctas_types properties ("replication_num" = "1") as
        select id, f_boolean, f_tinyint, f_smallint, f_int, f_bigint, f_float, f_double,
               f_decimal, f_char, f_string, f_date, f_timestamp, f_timestamp_ltz,
               f_array, f_map, f_row
        from ${catalogName}.fluss_test.log_types
    """
    qt_desc_ctas """desc ctas_types"""
    order_qt_ctas_count """select count(*), count(f_string), count(f_array) from ctas_types"""

    sql """
        create table ctas_nested properties ("replication_num" = "1") as
        select id, f_arr_arr, f_map_row, f_row_deep
        from ${catalogName}.fluss_test.log_nested where id = 1
    """
    qt_desc_ctas_nested """desc ctas_nested"""
    order_qt_ctas_nested_rows """select id, f_arr_arr, f_map_row, f_row_deep from ctas_nested"""

    // A join between an internal table and a fluss one, which is the shape a user
    // actually writes: the two scans are entirely different node types in one plan.
    order_qt_join_internal """
        select c.id, c.name, t.f_string
        from copied c join ${catalogName}.fluss_test.log_types t on c.id = t.id
    """

    // --- the scanner the session asked for is not the scanner fluss needs ------
    // Every fluss reader exists in FileScannerV2 alone, and enable_file_scanner_v2 is
    // a session variable a user may legitimately turn off (the fuzzy mode this pipeline
    // runs turns it off at random, which is why every suite here pins it on). A scan
    // that honoured it would reach the legacy scanner, whose JNI dispatch has no fluss
    // branch, and die with "Not supported create reader for table format" -- so what is
    // asserted is that the variable cannot change the answer, on each range kind a plain
    // fluss catalog plans. This stays in the code rather than becoming a recorded block:
    // what is under test is the agreement between two settings, and two identical
    // recordings only look alike to whoever reads them.
    def rowsOf = { String query -> sql(query).collect { row -> row.collect { it.toString() } } }
    def sameWithScannerV2Off = { String query ->
        sql """set enable_file_scanner_v2 = true"""
        def withV2 = rowsOf(query)
        sql """set enable_file_scanner_v2 = false"""
        def withoutV2 = rowsOf(query)
        sql """set enable_file_scanner_v2 = true"""
        assertEquals(withV2, withoutV2,
                "enable_file_scanner_v2 changed the answer for: ${query}"
                        + "\non=${withV2}\noff=${withoutV2}")
    }

    // LOG, PK_FULL, and a partitioned log table -- the three shapes whose ranges only
    // the v2 scanner can read.
    sameWithScannerV2Off """
        select id, name, price from ${catalogName}.fluss_test.log_basic order by id
    """
    sameWithScannerV2Off """
        select id, name from ${catalogName}.fluss_test.pk_basic order by id
    """
    sameWithScannerV2Off """
        select count(*) from ${catalogName}.fluss_test.log_part
    """

    sql """drop database if exists ${internalDb} force"""
    sql """switch internal"""
    sql """drop catalog if exists ${catalogName}"""
    sql """drop catalog if exists ${requiredCatalog}"""
}
