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

suite("test_mtmv_base_partition_read_scope") {
    String dbName = context.config.getDbNameByFile(context.file)
    // A refresh reads the base partitions the MV partition is recorded with, and no others. The window of
    // partition_sync_limit below keeps the last two days, so the day before them is recorded nowhere: it
    // must not be read either, or its rows would sit in the MV partition -- whose key range does cover
    // them -- while the snapshot says the MV does not hold that partition. A base partition dropped after
    // such a read would then be invisible to the sync check, and the transparent rewrite would serve the
    // rows of a partition the base table no longer has.
    //
    // The MV partition is a year and the base table's are days, so the range it is read through is wider
    // than what it is recorded with. Days are taken relative to today, and the two sides are asserted
    // separately rather than as a total, so that where the window's edge falls does not decide the case.
    // The days are taken in the FE's own calendar, which is the one partition_sync_limit cuts its window
    // from: the runner's local date can be a day off it, and a fixture built in the wrong one leaves the
    // retained day outside the window.
    def today = java.time.LocalDate.parse(sql("select curdate()").get(0).get(0).toString())
    def expiredDay = today.minusDays(5)
    def keptDay = today.minusDays(1)

    // The expired day is read by a refresh only while it falls inside a retained MV partition's range, and
    // both days are inside the same year only from the sixth of January on. The part below is left out
    // where they are not, rather than asserted on days the change cannot be told apart on -- and rather
    // than through an assumption, which this runner records as a failure of the suite, taking the calendar
    // independent parts of it down with it.
    if (expiredDay.getYear() == today.getYear()) {
        sql """drop materialized view if exists mv_read_scope"""
        sql """drop table if exists base_read_scope"""
        sql """
            create table base_read_scope (
                k1 date not null,
                value int not null
            ) duplicate key(k1)
            partition by range(k1) (
                partition p_expired values [("${expiredDay}"), ("${expiredDay.plusDays(1)}")),
                partition p_kept values [("${keptDay}"), ("${keptDay.plusDays(1)}")),
                partition p_today values [("${today}"), ("${today.plusDays(1)}"))
            )
            distributed by hash(k1) buckets 1
            properties("replication_num" = "1")
        """
        // The kept day carries enough rows that the view pre-aggregates to a single one: the optimizer then
        // prefers the view plus the expired day over scanning the base table, so the split is the plan that
        // is answered and not only one CBO leaves unused.
        sql """insert into base_read_scope select '${keptDay}', number from numbers("number" = "1000")"""
        sql """insert into base_read_scope values ("${expiredDay}", 1), ("${today}", 3)"""

        sql """
            create materialized view mv_read_scope
            build immediate refresh complete on manual
            partition by (date_trunc(k1,'year'))
            distributed by random buckets 1
            properties(
                "replication_num" = "1",
                "partition_sync_limit" = "2",
                "partition_sync_time_unit" = "DAY"
            )
            as select k1, sum(value) as total from base_read_scope group by k1
        """
        sql """refresh materialized view mv_read_scope complete"""
        waitingMTMVTaskFinishedByMvName("mv_read_scope")
        // The expectations below are read from the base table, so they must not be answered from the MV.
        sql """set enable_materialized_view_rewrite = false"""

        // The data the refresh had to work with, so that the count below is not read as an empty base table.
        order_qt_base_rows """select count(*) from base_read_scope"""

        // The day the window left out is the one missing from the MV, and the days it kept are there: read
        // separately rather than as a total, so that the case does not turn on where the window's edge falls.
        order_qt_scope_expired_day "select count(*) from mv_read_scope where k1 = '${expiredDay}'"
        order_qt_scope_kept_days "select count(*) from mv_read_scope where k1 >= '${keptDay}'"

        // Grouping by the view's own transform is answered from the view only while each retained partition
        // holds the whole transform bucket. The window cut this year: the view's year partition holds keptDay
        // alone, and expiredDay -- compensated from the base table -- falls in the same year. Joining the two
        // branches would emit one year group each instead of one row carrying both days' total.
        sql """analyze table base_read_scope with sync"""
        sql """analyze table mv_read_scope with sync"""
        sql """set enable_materialized_view_rewrite = true"""
        explain {
            sql("select date_trunc(k1, 'year') as y, sum(value) from base_read_scope "
                    + "where k1 in ('${expiredDay}', '${keptDay}') group by date_trunc(k1, 'year')")
            notContains "UNION"
        }
        order_qt_scope_window_cut_group """select year(date_trunc(k1, 'year'))
            - year(cast('${expiredDay}' as date)) as year_offset, sum(value)
            from base_read_scope where k1 in ('${expiredDay}', '${keptDay}')
            group by date_trunc(k1, 'year')"""
        sql """set enable_materialized_view_rewrite = false"""
    }

    // A partition of a list partitioned table holds one key per partition column, and a read pinned to one
    // of those columns reaches the partitions that differ in the others. The expired partition below shares
    // its `d` with a kept one, so a read pinned to `d` alone reads it while the snapshot names only the kept
    // partition: a later drop of it would leave its rows in the MV unaccounted for.
    sql """drop materialized view if exists list_scope_mv"""
    sql """drop table if exists list_scope_base"""
    sql """
        CREATE TABLE list_scope_base (d DATE NOT NULL, region VARCHAR(10) NOT NULL, amount BIGINT)
        DUPLICATE KEY(d, region)
        PARTITION BY LIST(d, region) (
            PARTITION p_expired VALUES IN ((\"2020-01-01\", \"US\")),
            PARTITION p_kept VALUES IN ((\"2020-01-01\", \"EU\"), (\"2038-01-01\", \"EU\"))
        )
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
    """
    sql """INSERT INTO list_scope_base VALUES
        (\"2020-01-01\", \"US\", 1), (\"2020-01-01\", \"EU\", 2), (\"2038-01-01\", \"EU\", 3)"""
    sql """
        CREATE MATERIALIZED VIEW list_scope_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        PARTITION BY (d)
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\",
            \"partition_sync_limit\" = \"2\", \"partition_sync_time_unit\" = \"YEAR\")
        AS SELECT d, region, SUM(amount) AS total FROM list_scope_base GROUP BY d, region
    """
    waitingMTMVTaskFinishedByMvName("list_scope_mv")
    order_qt_list_scope "SELECT d, region, total FROM list_scope_mv"

    // Two shapes cannot be pinned to what the base partitions hold, and both are read the way the MV
    // partition's own key range reads them -- a reading that can be seen to be too wide, rather than one
    // that leaves rows of the MV partition unread.
    //
    // The first is a list partitioned table's default partition, which takes the rows no other partition of
    // it claims: the row below is in it while its `d` puts it in the MV partition the explicit partition is
    // read through, so pinning the read to that partition's key would lose it.
    sql """drop materialized view if exists list_default_mv"""
    sql """drop table if exists list_default_base"""
    sql """
        CREATE TABLE list_default_base (d DATE NOT NULL, k INT NOT NULL, amount BIGINT)
        DUPLICATE KEY(d, k)
        PARTITION BY LIST(d, k) (
            PARTITION p_explicit VALUES IN ((\"2020-01-01\", 2)),
            PARTITION p_default
        )
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
    """
    sql """INSERT INTO list_default_base VALUES (\"2020-01-01\", 2, 2), (\"2020-01-01\", 3, 1)"""
    sql """
        CREATE MATERIALIZED VIEW list_default_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        PARTITION BY (d)
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
        AS SELECT d, k, SUM(amount) AS total FROM list_default_base GROUP BY d, k
    """
    waitingMTMVTaskFinishedByMvName("list_default_mv")
    order_qt_list_default "SELECT d, k, total FROM list_default_mv"
    // And the rows of that partition are read for every MV partition they belong to, so the MV partition is
    // recorded with the partition they come from: a row inserted into it afterwards is a change the MV
    // compares, rather than one it calls itself synchronized through.
    sql """INSERT INTO list_default_base VALUES (\"2020-01-01\", 4, 7)"""
    // A refresh of the MV has to read the rows of it that belong to this partition again: the partition is
    // one of the ones this MV partition is recorded with, so the insert is a change it compares, and a
    // refresh that judged the partition synchronized would leave the new row out. The MV's own sync flag is
    // not the check -- the sentinel MV partition is out of sync either way -- so the row is.
    sql """REFRESH MATERIALIZED VIEW list_default_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("list_default_mv")
    order_qt_list_default_tracked "SELECT d, k, total FROM list_default_mv"

    // The same table under a partition_sync_limit window: it is not windowed. The default partition takes
    // the rows no other partition of the table claims, and ADD PARTITION makes an empty partition for a key
    // without moving the rows the default partition already holds under it -- so which side of the window a
    // row is on is not something a read can filter by the partition columns, and the read is left whole. The
    // MV partition keeps every partition its key range covers, which is what it is recorded with.
    sql """drop materialized view if exists list_default_scope_mv"""
    sql """drop table if exists list_default_scope_base"""
    sql """
        CREATE TABLE list_default_scope_base (d DATE NOT NULL, k INT NOT NULL, amount BIGINT)
        DUPLICATE KEY(d, k)
        PARTITION BY LIST(d, k) (
            PARTITION p_kept VALUES IN ((\"2020-01-01\", 2), (\"2038-01-01\", 2)),
            PARTITION p_default
        )
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
    """
    // Placed in the default partition: no explicit partition claims its key yet.
    sql """INSERT INTO list_default_scope_base VALUES (\"2020-01-01\", 1, 1)"""
    // And an explicit partition for that key, added afterwards: it is empty, and the row above stays in the
    // default partition. It is an expired partition for the window below.
    sql """ALTER TABLE list_default_scope_base ADD PARTITION p_expired VALUES IN ((\"2020-01-01\", 1))"""
    sql """INSERT INTO list_default_scope_base VALUES
        (\"2020-01-01\", 2, 2), (\"2038-01-01\", 2, 3), (\"2020-01-01\", 3, 4)"""
    sql """
        CREATE MATERIALIZED VIEW list_default_scope_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        PARTITION BY (d)
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\",
            \"partition_sync_limit\" = \"2\", \"partition_sync_time_unit\" = \"YEAR\")
        AS SELECT d, k, SUM(amount) AS total FROM list_default_scope_base GROUP BY d, k
    """
    waitingMTMVTaskFinishedByMvName("list_default_scope_mv")
    // The row whose key a later partition claimed is one the MV partition holds: what it is read through is
    // the key range, not the partition a row sits in.
    order_qt_list_default_scope "SELECT d, k, total FROM list_default_scope_mv"

    // The added partition is one the MV partition is recorded with -- the key range covers it -- so dropping
    // it is a change the MV partition compares rather than one it stays synchronized through.
    sql """ALTER TABLE list_default_scope_base DROP PARTITION p_expired"""
    order_qt_list_default_scope_dropped """SELECT PartitionName, SyncWithBaseTables FROM
        partitions('catalog'='internal','database'='${dbName}','table'='list_default_scope_mv')
        ORDER BY PartitionName"""

    // A table's default partition belongs to every MV partition that reads the table, not only to the one
    // its own key maps to: here the join's other table has a partition for key 2 and a default partition
    // holding key 1, and the MV partition for key 1 is named by the first table. Reading the second table
    // as "no row" for that partition -- which is what its mapping says on its own -- empties the join.
    sql """drop materialized view if exists two_pct_default_mv"""
    sql """drop table if exists two_pct_left"""
    sql """drop table if exists two_pct_right"""
    sql """
        CREATE TABLE two_pct_left (k INT, v INT) DUPLICATE KEY(k)
        PARTITION BY LIST(k) (PARTITION l1 VALUES IN ((1)))
        DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
    """
    sql """
        CREATE TABLE two_pct_right (k INT, w INT) DUPLICATE KEY(k)
        PARTITION BY LIST(k) (PARTITION r2 VALUES IN ((2)), PARTITION r_default)
        DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
    """
    sql """INSERT INTO two_pct_left VALUES (1, 10)"""
    sql """INSERT INTO two_pct_right VALUES (1, 100)"""
    sql """
        CREATE MATERIALIZED VIEW two_pct_default_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        PARTITION BY (k)
        DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
        AS SELECT l.k AS k, l.v AS v, r.w AS w FROM two_pct_left l JOIN two_pct_right r ON l.k = r.k
    """
    waitingMTMVTaskFinishedByMvName("two_pct_default_mv")
    order_qt_two_pct_default "SELECT k, v, w FROM two_pct_default_mv"

    // The same read at the partition column's own type: a key written with milliseconds is compared at that
    // scale, so the partition of the fractional key keeps its row even though the table has a default
    // partition, which sends this table through the MV partition's key range rather than through the key.
    sql """drop materialized view if exists fractional_default_mv"""
    sql """drop table if exists fractional_default_base"""
    sql """
        CREATE TABLE fractional_default_base (ts DATETIME(3) NOT NULL, amount BIGINT)
        DUPLICATE KEY(ts)
        PARTITION BY LIST(ts) (
            PARTITION p_fraction VALUES IN ((\"2024-02-01 00:00:00.123\")),
            PARTITION p_default
        )
        DISTRIBUTED BY HASH(ts) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
    """
    sql """INSERT INTO fractional_default_base VALUES (\"2024-02-01 00:00:00.123\", 5)"""
    sql """
        CREATE MATERIALIZED VIEW fractional_default_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        PARTITION BY (ts)
        DISTRIBUTED BY HASH(ts) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
        AS SELECT ts, SUM(amount) AS total FROM fractional_default_base GROUP BY ts
    """
    waitingMTMVTaskFinishedByMvName("fractional_default_mv")
    order_qt_fractional_default "SELECT ts, total FROM fractional_default_mv"

    // The second is a key whose value carries a scale: what a partition of a DATETIME(3) partitioned table
    // holds is the value written with its milliseconds, and a read pinned to that value at a coarser scale
    // is one no row of the partition compares equal to.
    sql """drop materialized view if exists fractional_key_mv"""
    sql """drop table if exists fractional_key_base"""
    sql """
        CREATE TABLE fractional_key_base (ts DATETIME(3) NOT NULL, amount BIGINT)
        DUPLICATE KEY(ts)
        PARTITION BY LIST(ts) (
            PARTITION p_fraction VALUES IN ((\"2024-02-01 00:00:00.123\")),
            PARTITION p_whole VALUES IN ((\"2024-03-01 00:00:00\"))
        )
        DISTRIBUTED BY HASH(ts) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
    """
    sql """INSERT INTO fractional_key_base VALUES
        (\"2024-02-01 00:00:00.123\", 5), (\"2024-03-01 00:00:00\", 6)"""
    sql """
        CREATE MATERIALIZED VIEW fractional_key_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        PARTITION BY (ts)
        DISTRIBUTED BY HASH(ts) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
        AS SELECT ts, SUM(amount) AS total FROM fractional_key_base GROUP BY ts
    """
    waitingMTMVTaskFinishedByMvName("fractional_key_mv")
    order_qt_fractional_key "SELECT ts, total FROM fractional_key_mv"
    // A key that only the default partition holds has no MV partition to be read into -- the MV's partitions
    // are the keys the explicit partitions project to -- so the MV does not hold its rows. A query that may
    // read them is answered from the base table rather than from the MV: with the rewrite on, an MV over a
    // base table whose list partitions have a default partition is not offered as a candidate at all, since
    // which keys are in the default partition is not in the metadata. Without this the query below was
    // rewritten to the view and lost the k = 2 row (2 is in p_default, and the view holds key 1 alone).
    sql """drop materialized view if exists list_default_only_mv"""
    sql """drop table if exists list_default_only_base"""
    sql """
        CREATE TABLE list_default_only_base (k INT NOT NULL, amount BIGINT)
        DUPLICATE KEY(k)
        PARTITION BY LIST(k) (PARTITION p1 VALUES IN ((1)), PARTITION p_default)
        DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
    """
    sql """INSERT INTO list_default_only_base VALUES (1, 10), (2, 20)"""
    sql """
        CREATE MATERIALIZED VIEW list_default_only_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        PARTITION BY (k)
        DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
        AS SELECT k, SUM(amount) AS total FROM list_default_only_base GROUP BY k
    """
    waitingMTMVTaskFinishedByMvName("list_default_only_mv")
    // What the view holds: key 1 alone, the key the default partition holds having no view partition.
    order_qt_list_default_only_mv "SELECT k, total FROM list_default_only_mv ORDER BY k"

    // And the query over the base table with the rewrite on is answered from the base table, whole.
    sql """set enable_materialized_view_rewrite = true"""
    order_qt_list_default_only_query "SELECT k, SUM(amount) AS total FROM list_default_only_base GROUP BY k"
    // A base partition the sync window has since left out can already have been materialized in an MV partition
    // the query uses: the mapping stops naming it, but the rows stay where an earlier refresh put them. The
    // query has to be answered from the base table for it with that MV partition taken out, or the rows are
    // counted twice -- which is what the union of the MV's own branch and the base's did before.
    sql """drop materialized view if exists sync_window_narrowed_mv"""
    sql """drop table if exists sync_window_narrowed_base"""
    sql """
        CREATE TABLE sync_window_narrowed_base (d DATE NOT NULL, region VARCHAR(10) NOT NULL, v INT NOT NULL)
        DUPLICATE KEY(d, region)
        PARTITION BY LIST(d, region) (
            PARTITION p_old VALUES IN ((\"2020-01-01\", \"US\")),
            PARTITION p_kept VALUES IN ((\"2020-01-01\", \"EU\"), (\"2120-01-01\", \"EU\")))
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
    """
    sql """INSERT INTO sync_window_narrowed_base VALUES
        (\"2020-01-01\", \"US\", 1), (\"2020-01-01\", \"EU\", 2), (\"2120-01-01\", \"EU\", 3)"""
    // No window at all, so the refresh records both partitions of 2020-01-01.
    sql """
        CREATE MATERIALIZED VIEW sync_window_narrowed_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL PARTITION BY (d)
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\", \"grace_period\" = \"31536000\")
        AS SELECT d, region, SUM(v) AS s FROM sync_window_narrowed_base GROUP BY d, region
    """
    waitingMTMVTaskFinishedByMvName("sync_window_narrowed_mv")
    // And then the window narrows past p_old: it is no longer named by any MV partition's mapping, while the
    // MV partition still holds the rows the earlier refresh read from it.
    sql """ALTER MATERIALIZED VIEW sync_window_narrowed_mv SET
        (\"partition_sync_limit\" = \"2\", \"partition_sync_time_unit\" = \"YEAR\",
         \"partition_date_format\" = \"yyyy-MM-dd\")"""
    sql """set enable_materialized_view_rewrite = true"""
    // The answer below is also what a plan over the base table alone returns, so the plan is what pins the
    // path: the view's branch unioned with the base table's for the partition the window left out.
    explain {
        sql(" select d, region, sum(v) as s from sync_window_narrowed_base group by d, region")
        check { result -> result.contains("UNION") && result.contains("sync_window_narrowed_mv") }
    }
    order_qt_sync_window_narrowed "SELECT d, region, SUM(v) AS s FROM sync_window_narrowed_base GROUP BY d, region ORDER BY d, region"

    // A partition of a range partitioned base table can end past the MV partition it is rolled up into: the
    // roll-up reads the partition's upper bound with the last second taken off it, so a partition ending at
    // 2025-01-01 00:00:00.123 is the year 2024's. The row at 2025-01-01 00:00:00.100 is inside that
    // partition and belongs to the year 2025, for which the MV has no partition at all -- so a read that
    // takes it has nowhere to write it, the sink filters it, and the refresh fails on the filtered row
    // instead of rebuilding the partition. Read inside the MV partition's own bound, it is left where it
    // belongs and the partition is rebuilt.
    sql """drop materialized view if exists range_ms_boundary_mv"""
    sql """drop table if exists range_ms_boundary_base"""
    sql """
        CREATE TABLE range_ms_boundary_base (ts DATETIME(3) NOT NULL, amount BIGINT)
        DUPLICATE KEY(ts)
        PARTITION BY RANGE(ts) (
            PARTITION p_2024 VALUES [("2024-01-01 00:00:00.123"), ("2025-01-01 00:00:00.123"))
        )
        DISTRIBUTED BY HASH(ts) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO range_ms_boundary_base VALUES
        ("2024-12-31 23:59:59.999", 1), ("2025-01-01 00:00:00.100", 2)"""
    sql """
        CREATE MATERIALIZED VIEW range_ms_boundary_mv
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL PARTITION BY (date_trunc(ts, 'year'))
        DISTRIBUTED BY HASH(ts) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        AS SELECT ts, SUM(amount) AS total FROM range_ms_boundary_base GROUP BY ts
    """
    sql """REFRESH MATERIALIZED VIEW range_ms_boundary_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("range_ms_boundary_mv")
    order_qt_range_ms_boundary "SELECT ts, total FROM range_ms_boundary_mv"

    // The same partition under a DIRECT range MV -- one partitioned by the base table's own column -- where
    // the MV partition's bounds ARE the base partition's, written with its milliseconds. The read writes the
    // MV partition's own bound too, and a partition key carries only its primitive type: written from the key
    // the upper bound comes out as the bare second, the read ends 123 milliseconds early, and the rows of
    // that tail -- rows this refresh is to write -- are left out of the rebuilt partition. Nothing reports
    // them: the snapshot records the partition either way.
    sql """drop materialized view if exists range_ms_direct_mv"""
    sql """drop table if exists range_ms_direct_base"""
    sql """
        CREATE TABLE range_ms_direct_base (ts DATETIME(3) NOT NULL, amount BIGINT)
        DUPLICATE KEY(ts)
        PARTITION BY RANGE(ts) (
            PARTITION p_frac VALUES [("2024-01-01 00:00:00.123"), ("2024-02-01 00:00:00.123"))
        )
        DISTRIBUTED BY HASH(ts) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO range_ms_direct_base VALUES
        ("2024-01-01 00:00:00.123", 1), ("2024-02-01 00:00:00.100", 2)"""
    sql """
        CREATE MATERIALIZED VIEW range_ms_direct_mv
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL PARTITION BY (ts)
        DISTRIBUTED BY HASH(ts) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        AS SELECT ts, SUM(amount) AS total FROM range_ms_direct_base GROUP BY ts
    """
    sql """REFRESH MATERIALIZED VIEW range_ms_direct_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("range_ms_direct_mv")
    order_qt_range_ms_direct "SELECT ts, total FROM range_ms_direct_mv"

    // A table every query-used partition of which the mapping does not name -- the sync window left the one
    // the query reads out of it, the query's filtering then dropped the table from the mapping whole -- is
    // read for the query's own partitions from the base table. The MV partitions the plan uses answer the
    // partitions the mapping names, and for this table those are none: without this the MV alone answered for
    // it and the committed join row of the partition the query reads was lost. Grace is what admits the MV
    // partition here without comparing it, so the compensation is the only thing that reads those rows.
    sql """drop materialized view if exists uncovered_pct_mv"""
    sql """drop table if exists uncovered_pct_a"""
    sql """drop table if exists uncovered_pct_b"""
    sql """
        CREATE TABLE uncovered_pct_a (d DATE NOT NULL, k INT NOT NULL, v INT NOT NULL)
        DUPLICATE KEY(d, k) PARTITION BY LIST(d, k) (
            PARTITION a_kept VALUES IN (("2020-01-01", 1), ("2038-01-01", 1)))
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE uncovered_pct_b (d DATE NOT NULL, k INT NOT NULL, v INT NOT NULL)
        DUPLICATE KEY(d, k) PARTITION BY LIST(d, k) (
            PARTITION b_kept VALUES IN (("2020-01-01", 1), ("2038-01-01", 1)),
            PARTITION b_old VALUES IN (("2020-01-01", 2)))
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO uncovered_pct_a VALUES ("2020-01-01", 1, 1)"""
    sql """INSERT INTO uncovered_pct_b VALUES ("2020-01-01", 1, 10), ("2020-01-01", 2, 20)"""
    sql """
        CREATE MATERIALIZED VIEW uncovered_pct_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL PARTITION BY (a_d)
        DISTRIBUTED BY HASH(a_d) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "partition_sync_limit" = "2", "partition_sync_time_unit" = "YEAR",
                    "grace_period" = "600")
        AS SELECT a.d AS a_d, b.k AS b_k, COUNT(*) AS c
           FROM uncovered_pct_a a JOIN uncovered_pct_b b ON a.d = b.d
           GROUP BY a.d, b.k
    """
    waitingMTMVTaskFinishedByMvName("uncovered_pct_mv")
    sql """set enable_materialized_view_rewrite = true"""
    // b.k = 2 is b_old's, which the window left out: the answer needs that join row, and the MV does not
    // hold it.
    order_qt_uncovered_pct """SELECT a.d AS a_d, b.k AS b_k, COUNT(*) AS c FROM uncovered_pct_a a
        JOIN uncovered_pct_b b ON a.d = b.d WHERE a.d = "2020-01-01" AND b.k = 2 GROUP BY a.d, b.k"""

    // A merged LIST component is recorded with every partition it was built from, and the query's partitions
    // decide only which component is read: a query reading one of a component's partitions finds the MV
    // partition freshly refreshed, since the freshness check compares those names to the recorded ones. A
    // name set narrowed to what the query reads would never match it and the MV would answer nothing.
    sql """drop materialized view if exists merged_component_mv"""
    sql """drop table if exists merged_component_base"""
    sql """
        CREATE TABLE merged_component_base (d INT NOT NULL, region VARCHAR(10) NOT NULL, v INT NOT NULL)
        DUPLICATE KEY(d, region) PARTITION BY LIST(d, region) (
            PARTITION p_single VALUES IN ((1, "EU")),
            PARTITION p_double VALUES IN ((1, "US"), (2, "US")))
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO merged_component_base VALUES (1, "EU", 1), (1, "US", 2), (2, "US", 3)"""
    sql """
        CREATE MATERIALIZED VIEW merged_component_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL PARTITION BY (d)
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        AS SELECT d, region, SUM(v) AS s FROM merged_component_base GROUP BY d, region
    """
    waitingMTMVTaskFinishedByMvName("merged_component_mv")
    sql """set enable_materialized_view_rewrite = true"""
    mv_rewrite_success(
        "select d, region, sum(v) as s from merged_component_base where d = 2 group by d, region",
        "merged_component_mv",
        is_partition_statistics_ready(dbName, ["merged_component_base", "merged_component_mv"]))
    order_qt_merged_component """SELECT d, region, SUM(v) AS s FROM merged_component_base
        WHERE d = 2 GROUP BY d, region"""

    // The base branch of a union compensation reads a stale partition back through that partition's own
    // key, and that key has to be written at the partition column's type. A DATETIME(3) key rounded to the
    // second is a value no row of the partition compares equal to, so the branch would read none of it and
    // the rows the stale MV partition is missing would be lost again.
    sql """drop materialized view if exists stale_fractional_union_mv"""
    sql """drop table if exists stale_fractional_union_base"""
    sql """
        CREATE TABLE stale_fractional_union_base (ts DATETIME(3) NOT NULL, amount BIGINT)
        DUPLICATE KEY(ts) PARTITION BY LIST(ts) (
            PARTITION p_ms VALUES IN (("2024-02-01 00:00:00.123")),
            PARTITION p_kept VALUES IN (("2024-02-02 00:00:00.456")))
        DISTRIBUTED BY HASH(ts) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO stale_fractional_union_base VALUES
        ("2024-02-01 00:00:00.123", 1), ("2024-02-02 00:00:00.456", 2)"""
    // The kept partition is filled before the refresh, so that after one partition goes stale the MV is the
    // cheaper side and the union is chosen rather than dropped by the cost model on an empty table.
    sql """INSERT INTO stale_fractional_union_base
        SELECT CAST("2024-02-02 00:00:00.456" AS DATETIME(3)), 0 FROM numbers("number" = "10000")"""
    sql """
        CREATE MATERIALIZED VIEW stale_fractional_union_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL PARTITION BY (ts)
        DISTRIBUTED BY HASH(ts) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        AS SELECT ts, SUM(amount) AS total FROM stale_fractional_union_base GROUP BY ts
    """
    waitingMTMVTaskFinishedByMvName("stale_fractional_union_mv")
    // The stale partition's row is inserted after the refresh, so the row can only come from the base
    // branch of the union.
    sql """INSERT INTO stale_fractional_union_base VALUES ("2024-02-01 00:00:00.123", 3)"""
    sql """ANALYZE TABLE stale_fractional_union_base WITH SYNC"""
    sql """set enable_materialized_view_rewrite = true"""
    explain {
        sql("select ts, sum(amount) as total from stale_fractional_union_base group by ts")
        check { result -> result.contains("UNION") && result.contains("stale_fractional_union_mv") }
    }
    order_qt_stale_fractional_union """SELECT ts, SUM(amount) AS total FROM stale_fractional_union_base
        GROUP BY ts"""

    // A merged LIST component's MV partition holds a key an uncovered partition also carries: kept has
    // (2020-01-01,1) and bridge (2038-01-01,2) meet in one component, whose MV partition holds d=2020;
    // old (2020-01-01,2) is outside the window and uncovered. Reading old beside the MV partition and
    // unioning the two answers gives two rows for d=2020 rather than one, because the union sits below the
    // aggregate. The query is answered from the base table instead.
    sql """drop materialized view if exists group_split_mv"""
    sql """drop table if exists group_split_base"""
    sql """
        CREATE TABLE group_split_base (d DATE NOT NULL, k INT NOT NULL, v INT NOT NULL)
        DUPLICATE KEY(d, k) PARTITION BY LIST(d, k) (
            PARTITION p_kept VALUES IN (("2020-01-01", 1), ("2038-01-01", 1)),
            PARTITION p_bridge VALUES IN (("2038-01-01", 2)),
            PARTITION p_old VALUES IN (("2020-01-01", 2)))
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO group_split_base VALUES
        ("2020-01-01", 1, 2), ("2020-01-01", 2, 1), ("2038-01-01", 1, 5), ("2038-01-01", 2, 7)"""
    // Fill a kept partition before the refresh, so that without the fallback the union would be chosen
    // rather than dropped by the cost model on an empty table.
    sql """INSERT INTO group_split_base
        SELECT CAST("2038-01-01" AS DATE), 1, 0 FROM numbers("number" = "10000")"""
    sql """
        CREATE MATERIALIZED VIEW group_split_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL PARTITION BY (d)
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES ("replication_num" = "1",
            "partition_sync_limit" = "2", "partition_sync_time_unit" = "YEAR")
        AS SELECT d, SUM(v) AS s FROM group_split_base GROUP BY d
    """
    waitingMTMVTaskFinishedByMvName("group_split_mv")
    sql """ANALYZE TABLE group_split_base WITH SYNC"""
    sql """set enable_materialized_view_rewrite = true"""
    explain {
        sql("select d, sum(v) from group_split_base where d = '2020-01-01' group by d")
        notContains "UNION"
    }
    order_qt_group_split """SELECT d, SUM(v) FROM group_split_base WHERE d = '2020-01-01' GROUP BY d"""

    // A many-to-one group expression has the same effect even when the keys differ: kept holds January 2
    // and old January 1, which are different keys, but date_trunc(d,'year') puts both in the year 2020
    // group. The union of the MV's year-2020 aggregate and the base branch's is two rows rather than one.
    sql """drop materialized view if exists group_split_expr_mv"""
    sql """drop table if exists group_split_expr_base"""
    sql """
        CREATE TABLE group_split_expr_base (d DATE NOT NULL, k INT NOT NULL, v INT NOT NULL)
        DUPLICATE KEY(d, k) PARTITION BY LIST(d, k) (
            PARTITION p_kept VALUES IN (("2020-01-02", 1), ("2038-01-01", 1)),
            PARTITION p_bridge VALUES IN (("2038-01-01", 2)),
            PARTITION p_old VALUES IN (("2020-01-01", 2)))
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO group_split_expr_base VALUES
        ("2020-01-02", 1, 2), ("2020-01-01", 2, 1), ("2038-01-01", 1, 5), ("2038-01-01", 2, 7)"""
    sql """INSERT INTO group_split_expr_base
        SELECT CAST("2038-01-01" AS DATE), 1, 0 FROM numbers("number" = "10000")"""
    sql """
        CREATE MATERIALIZED VIEW group_split_expr_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL PARTITION BY (d)
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES ("replication_num" = "1",
            "partition_sync_limit" = "2", "partition_sync_time_unit" = "YEAR")
        AS SELECT d, SUM(v) AS s FROM group_split_expr_base GROUP BY d
    """
    waitingMTMVTaskFinishedByMvName("group_split_expr_mv")
    sql """ANALYZE TABLE group_split_expr_base WITH SYNC"""
    sql """set enable_materialized_view_rewrite = true"""
    explain {
        sql("select date_trunc(d, 'year') as y, sum(v) from group_split_expr_base "
                + "where d in ('2020-01-01', '2020-01-02') group by date_trunc(d, 'year')")
        notContains "UNION"
    }
    order_qt_group_split_expr """SELECT DATE_TRUNC(d, 'year') AS y, SUM(v) FROM group_split_expr_base
        WHERE d IN ('2020-01-01', '2020-01-02') GROUP BY DATE_TRUNC(d, 'year')"""

    // A grouping set that leaves the partition column out has the same effect with neither a group expression
    // nor a removed partition: GROUPING SETS ((d),()) holds Jan 2 in one set and nothing in the grand total,
    // so the total of the MV's Jan 2 and the compensation branch's Jan 1 is two rows rather than one. The
    // normalized group-by list is only the slots d and grouping_id, which the check above skips, so each
    // grouping set of the query's Repeat has to be looked at.
    explain {
        sql("select d, sum(v) from group_split_expr_base where d in ('2020-01-01', '2020-01-02') "
                + "group by grouping sets ((d), ())")
        notContains "UNION"
    }
    order_qt_group_split_sets """SELECT d, SUM(v) FROM group_split_expr_base
        WHERE d IN ('2020-01-01', '2020-01-02') GROUP BY GROUPING SETS ((d), ())"""

    // Removing a stale MV partition does not remove every partition of the MV, so it does not make the
    // computed group safe: here the 2037 partition alone is stale while the merged Jan 2 component stays
    // fresh, and the compensation reads p_old beside it. date_trunc(d,'year') puts both of the year-2020 days
    // in one group, so the MV branch and the base branch each emit a 2020 row. The query is answered from the
    // base table instead.
    sql """drop materialized view if exists group_split_stale_expr_mv"""
    sql """drop table if exists group_split_stale_expr_base"""
    sql """
        CREATE TABLE group_split_stale_expr_base (d DATE NOT NULL, k INT NOT NULL, v INT NOT NULL)
        DUPLICATE KEY(d, k) PARTITION BY LIST(d, k) (
            PARTITION p_kept VALUES IN (("2020-01-02", 1), ("2038-01-01", 1)),
            PARTITION p_bridge VALUES IN (("2038-01-01", 2)),
            PARTITION p_stale VALUES IN (("2037-01-01", 3)),
            PARTITION p_old VALUES IN (("2020-01-01", 2)))
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO group_split_stale_expr_base VALUES
        ("2020-01-02", 1, 2), ("2020-01-01", 2, 1), ("2038-01-01", 1, 5), ("2038-01-01", 2, 7),
        ("2037-01-01", 3, 4)"""
    sql """INSERT INTO group_split_stale_expr_base
        SELECT CAST("2038-01-01" AS DATE), 1, 0 FROM numbers("number" = "10000")"""
    sql """
        CREATE MATERIALIZED VIEW group_split_stale_expr_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL PARTITION BY (d)
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES ("replication_num" = "1",
            "partition_sync_limit" = "2", "partition_sync_time_unit" = "YEAR")
        AS SELECT d, SUM(v) AS s FROM group_split_stale_expr_base GROUP BY d
    """
    waitingMTMVTaskFinishedByMvName("group_split_stale_expr_mv")
    // Written after the refresh, so only the 2037 partition of the MV goes stale; the merged component's
    // stays usable and is the side the union would take the year-2020 group from.
    sql """INSERT INTO group_split_stale_expr_base VALUES ("2037-01-01", 3, 100)"""
    sql """ANALYZE TABLE group_split_stale_expr_base WITH SYNC"""
    sql """set enable_materialized_view_rewrite = true"""
    explain {
        sql("select date_trunc(d, 'year') as y, sum(v) from group_split_stale_expr_base "
                + "where d in ('2020-01-01', '2020-01-02', '2037-01-01') group by date_trunc(d, 'year')")
        notContains "UNION"
    }
    order_qt_group_split_stale_expr """SELECT DATE_TRUNC(d, 'year') AS y, SUM(v) FROM group_split_stale_expr_base
        WHERE d IN ('2020-01-01', '2020-01-02', '2037-01-01') GROUP BY DATE_TRUNC(d, 'year')"""

    // But an uncovered partition that overlaps only an MV partition which the staleness pass has already
    // scheduled for removal overlaps nothing the plan keeps: the removed partition's rows come through the
    // base branch too, so the two branches' groups stay apart and the union rewrite is the right answer.
    // Here the merged {2020, 2037} component goes stale (p_stale is written after the refresh) while the
    // 2038 partition stays fresh, and p_old's Jan 1 is the uncovered partition the query reads beside it.
    sql """drop materialized view if exists partial_stale_mv"""
    sql """drop table if exists partial_stale_base"""
    sql """
        CREATE TABLE partial_stale_base (d DATE NOT NULL, k INT NOT NULL, v INT NOT NULL)
        DUPLICATE KEY(d, k) PARTITION BY LIST(d, k) (
            PARTITION p_stale VALUES IN (("2020-01-01", 1), ("2037-01-01", 1)),
            PARTITION p_bridge VALUES IN (("2037-01-01", 2)),
            PARTITION p_old VALUES IN (("2020-01-01", 2)),
            PARTITION p_fresh VALUES IN (("2038-01-01", 1)))
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO partial_stale_base VALUES
        ("2020-01-01", 1, 2), ("2037-01-01", 1, 5), ("2037-01-01", 2, 7), ("2020-01-01", 2, 1)"""
    sql """INSERT INTO partial_stale_base
        SELECT CAST("2038-01-01" AS DATE), 1, 0 FROM numbers("number" = "10000")"""
    sql """
        CREATE MATERIALIZED VIEW partial_stale_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL PARTITION BY (d)
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES ("replication_num" = "1",
            "partition_sync_limit" = "2", "partition_sync_time_unit" = "YEAR")
        AS SELECT d, SUM(v) AS s FROM partial_stale_base GROUP BY d
    """
    waitingMTMVTaskFinishedByMvName("partial_stale_mv")
    sql """INSERT INTO partial_stale_base VALUES ("2020-01-01", 1, 100)"""
    sql """ANALYZE TABLE partial_stale_base WITH SYNC"""
    sql """set enable_materialized_view_rewrite = true"""
    explain {
        sql("select d, sum(v) from partial_stale_base where d in ('2020-01-01', '2038-01-01') group by d")
        check { result -> result.contains("UNION") && result.contains("partial_stale_mv") }
    }
    order_qt_partial_stale """SELECT d, SUM(v) FROM partial_stale_base
        WHERE d IN ('2020-01-01', '2038-01-01') GROUP BY d"""

    // A view partitioned on the column two tables are joined by tracks one partition dimension through both
    // of them: the refresh follows the join's equality, so it records A.d and B.d both as columns its
    // partition key derives from. Grouping the query by either side reaches that dimension all the same while
    // the query keeps the two sides equal, and the view's partitions are disjoint in it, so the compensation's
    // two branches answer different days -- the view the days it still holds, the base tables the day written
    // after the refresh. Asking for every recorded column by name instead throws the view away and re-reads and
    // re-aggregates the days the view answers.
    def joinOldDay = today.minusDays(5)
    def joinKeptDay = today.minusDays(4)
    def joinThirdDay = today.minusDays(3)
    sql """drop materialized view if exists pct_join_mv"""
    sql """drop table if exists pct_join_a"""
    sql """drop table if exists pct_join_b"""
    sql """
        CREATE TABLE pct_join_a (d DATE NOT NULL, v INT NOT NULL)
        DUPLICATE KEY(d) PARTITION BY RANGE(d) (
            PARTITION p_old VALUES [("${joinOldDay}"), ("${joinKeptDay}")),
            PARTITION p_kept VALUES [("${joinKeptDay}"), ("${joinThirdDay}")),
            PARTITION p_third VALUES [("${joinThirdDay}"), ("${joinThirdDay.plusDays(1)}"))
        )
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE pct_join_b (d DATE NOT NULL, v INT NOT NULL)
        DUPLICATE KEY(d) PARTITION BY RANGE(d) (
            PARTITION p_old VALUES [("${joinOldDay}"), ("${joinKeptDay}")),
            PARTITION p_kept VALUES [("${joinKeptDay}"), ("${joinThirdDay}")),
            PARTITION p_third VALUES [("${joinThirdDay}"), ("${joinThirdDay.plusDays(1)}"))
        )
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO pct_join_a VALUES ("${joinOldDay}", 1), ("${joinKeptDay}", 10), ("${joinThirdDay}", 100)"""
    // One row per day on the other side, so the join does not multiply what the sum over the first side holds.
    sql """INSERT INTO pct_join_b VALUES ("${joinOldDay}", 1), ("${joinKeptDay}", 1), ("${joinThirdDay}", 1)"""
    sql """
        CREATE MATERIALIZED VIEW pct_join_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL PARTITION BY (a_d)
        DISTRIBUTED BY HASH(a_d) BUCKETS 1 PROPERTIES ("replication_num" = "1",
            "partition_sync_limit" = "7", "partition_sync_time_unit" = "DAY")
        AS SELECT a.d AS a_d, SUM(a.v) AS s FROM pct_join_a a JOIN pct_join_b b ON a.d = b.d GROUP BY a.d
    """
    waitingMTMVTaskFinishedByMvName("pct_join_mv")
    // The day the view holds for p_old is the only one written after the refresh, so it is the only partition
    // the compensation re-reads -- from both tables, since the join needs each row's pair on the other side.
    sql """INSERT INTO pct_join_a VALUES ("${joinOldDay}", 1000)"""
    sql """ANALYZE TABLE pct_join_a WITH SYNC"""
    sql """ANALYZE TABLE pct_join_b WITH SYNC"""
    sql """set enable_materialized_view_rewrite = true"""
    explain {
        sql("select a.d, sum(a.v) from pct_join_a a join pct_join_b b on a.d = b.d "
                + "where a.d in ('${joinOldDay}', '${joinKeptDay}') group by a.d")
        check { result -> result.contains("UNION") && result.contains("pct_join_mv") }
    }
    // The day is reported as an offset from the first one read, so that the expected rows hold on any date the
    // suite runs on: the days themselves come from today.
    order_qt_pct_join_one_side """SELECT DATEDIFF(a.d, CAST('${joinOldDay}' AS DATE)) AS day_offset, SUM(a.v)
        FROM pct_join_a a JOIN pct_join_b b ON a.d = b.d
        WHERE a.d IN ('${joinOldDay}', '${joinKeptDay}') GROUP BY a.d"""

    // The equality the refresh follows need not be a single predicate. Over A JOIN C JOIN B with a.d = c.x and
    // c.x = b.d, the view partitioned on a_d is tracked by A's and B's columns as much as a direct a.d = b.d
    // would make it, because the chain ties those two columns in every row the join returns. Closing the
    // grouping over the whole chain rather than over direct pairs only keeps such a view in the rewrite;
    // requiring each tracked column to be grouped or tied to a grouped one by itself stops at c.x -- which the
    // view is not partitioned on -- and so never reaches b.d, answering the query from the base tables.
    sql """drop materialized view if exists pct_join_mv3"""
    sql """drop table if exists pct_join_c"""
    sql """
        CREATE TABLE pct_join_c (x DATE NOT NULL, w INT NOT NULL)
        DUPLICATE KEY(x) DISTRIBUTED BY HASH(x) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO pct_join_c VALUES ("${joinOldDay}", 1), ("${joinKeptDay}", 1), ("${joinThirdDay}", 1)"""
    sql """
        CREATE MATERIALIZED VIEW pct_join_mv3
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL PARTITION BY (a_d)
        DISTRIBUTED BY HASH(a_d) BUCKETS 1 PROPERTIES ("replication_num" = "1",
            "partition_sync_limit" = "7", "partition_sync_time_unit" = "DAY")
        AS SELECT a.d AS a_d, SUM(a.v) AS s FROM pct_join_a a JOIN pct_join_c c ON a.d = c.x
           JOIN pct_join_b b ON c.x = b.d GROUP BY a.d
    """
    waitingMTMVTaskFinishedByMvName("pct_join_mv3")
    // Written after this view's refresh, so p_old is the day its compensation re-reads and the days it keeps
    // stay on the view's side of the union.
    sql """INSERT INTO pct_join_a VALUES ("${joinOldDay}", 10000)"""
    sql """ANALYZE TABLE pct_join_a WITH SYNC"""
    sql """ANALYZE TABLE pct_join_b WITH SYNC"""
    sql """ANALYZE TABLE pct_join_c WITH SYNC"""
    sql """set enable_materialized_view_rewrite = true"""
    explain {
        sql("select a.d, sum(a.v) from pct_join_a a join pct_join_c c on a.d = c.x join pct_join_b b on c.x = b.d "
                + "where a.d in ('${joinOldDay}', '${joinKeptDay}') group by a.d")
        check { result -> result.contains("UNION") && result.contains("pct_join_mv3") }
    }
    order_qt_pct_join_transitive """SELECT DATEDIFF(a.d, CAST('${joinOldDay}' AS DATE)) AS day_offset, SUM(a.v)
        FROM pct_join_a a JOIN pct_join_c c ON a.d = c.x JOIN pct_join_b b ON c.x = b.d
        WHERE a.d IN ('${joinOldDay}', '${joinKeptDay}') GROUP BY a.d"""

    // The view need not partition a base column by itself. Partitioned by date_trunc(d, 'month'), its
    // partitions are the months of d, and a query grouping by that same expression puts each group inside one
    // of them -- the view answers the month it kept, the base table the month written after the refresh, and
    // the union's two rows stay apart. The group by names the transform rather than d, so the shuttled
    // expression is a date_trunc over the column instead of the slot itself; it reaches the partition column
    // exactly when it is the view's own transform, since a coarser truncation rolls several of the view's
    // partitions into one group that both branches answer.
    def thisMonth = today.withDayOfMonth(1)
    def lastMonth = thisMonth.minusMonths(1)
    sql """drop materialized view if exists pct_month_mv"""
    sql """drop table if exists pct_month_base"""
    sql """
        CREATE TABLE pct_month_base (d DATE NOT NULL, v INT NOT NULL)
        DUPLICATE KEY(d) PARTITION BY RANGE(d) (
            PARTITION p_last VALUES [("${lastMonth}"), ("${lastMonth.plusDays(1)}")),
            PARTITION p_this VALUES [("${thisMonth}"), ("${thisMonth.plusDays(1)}"))
        )
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO pct_month_base VALUES ("${lastMonth}", 1), ("${thisMonth}", 10)"""
    // The kept month is filled before the refresh, so that once the other month is written to after it the
    // view's branch is the cheaper side and the union is chosen rather than dropped by the cost model on a
    // table whose every month is one row.
    sql """INSERT INTO pct_month_base
        SELECT CAST("${lastMonth}" AS DATE), 0 FROM numbers("number" = "10000")"""
    sql """
        CREATE MATERIALIZED VIEW pct_month_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL PARTITION BY (date_trunc(d, 'month'))
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES ("replication_num" = "1",
            "partition_sync_limit" = "100", "partition_sync_time_unit" = "DAY")
        AS SELECT d, SUM(v) AS s FROM pct_month_base GROUP BY d
    """
    waitingMTMVTaskFinishedByMvName("pct_month_mv")
    // Written after the refresh, so the month of p_this is the one the compensation re-reads; the view keeps
    // the month before it.
    sql """INSERT INTO pct_month_base VALUES ("${thisMonth}", 1000)"""
    sql """ANALYZE TABLE pct_month_base WITH SYNC"""
    sql """set enable_materialized_view_rewrite = true"""
    explain {
        sql("select date_trunc(d, 'month') as m, sum(v) from pct_month_base "
                + "where d in ('${lastMonth}', '${thisMonth}') group by date_trunc(d, 'month')")
        check { result -> result.contains("UNION") && result.contains("pct_month_mv") }
    }
    // Months are reported as offsets from the first one read, so the expected rows hold on any date the suite
    // runs on: the months themselves come from today.
    order_qt_pct_month """SELECT MONTHS_DIFF(date_trunc(d, 'month'), CAST('${lastMonth}' AS DATE)) AS month_offset,
        SUM(v) FROM pct_month_base WHERE d IN ('${lastMonth}', '${thisMonth}')
        GROUP BY date_trunc(d, 'month')"""
    // The same column under a coarser truncation is not the view's transform: a year group can roll several
    // of the view's months into one, so the view's kept month and the base table's written month can both
    // fall in it, and such a group is not one the compensation can split between the branches.
    explain {
        sql("select date_trunc(d, 'year') as y, sum(v) from pct_month_base "
                + "where d in ('${lastMonth}', '${thisMonth}') group by date_trunc(d, 'year')")
        notContains "UNION"
    }

    // A compensated partition can reach into a month a retained partition answers even when no retained-side
    // overrun shows it. The roll-up reads the upper bound with the last second off it, so p_prev ending at
    // 2024-02-01 00:00:00.123 is January's, while its rows in [February 1 00:00:00.000, .123) are February's.
    // The refresh clips that fraction, so the view never holds it; the next base partition starts after a gap,
    // at February 5, so the view's February partition is fresh and no recorded upper bound runs past it. The
    // compensated partition itself reaches past the month its roll-up names, so the query is answered from
    // the base table rather than by a union whose branches each emit a February row.
    sql """drop materialized view if exists range_spill_mv"""
    sql """drop table if exists range_spill_base"""
    sql """
        CREATE TABLE range_spill_base (ts DATETIME(3) NOT NULL, amount BIGINT)
        DUPLICATE KEY(ts) PARTITION BY RANGE(ts) (
            PARTITION p_prev VALUES [("2024-01-01 00:00:00.123"), ("2024-02-01 00:00:00.123")),
            PARTITION p_kept VALUES [("2024-02-05 00:00:00.000"), ("2024-03-01 00:00:00.000")))
        DISTRIBUTED BY HASH(ts) BUCKETS 1 PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO range_spill_base VALUES
        ("2024-01-15 00:00:00.000", 10), ("2024-02-01 00:00:00.100", 1)"""
    // The kept month carries enough rows that the view pre-aggregates to one, so that the union would be the
    // cheaper side once p_prev goes stale rather than dropped by the cost model on an empty table.
    sql """INSERT INTO range_spill_base
        SELECT CAST("2024-02-15 00:00:00.000" AS DATETIME(3)), 1 FROM numbers("number" = "1000")"""
    sql """
        CREATE MATERIALIZED VIEW range_spill_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL PARTITION BY (date_trunc(ts, 'month'))
        DISTRIBUTED BY HASH(ts) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        AS SELECT ts, SUM(amount) AS total FROM range_spill_base GROUP BY ts
    """
    waitingMTMVTaskFinishedByMvName("range_spill_mv")
    // The refresh rolls p_prev up to January and clips its February fraction: the row is in no MV partition.
    order_qt_range_spill_mv_tail "SELECT COUNT(*) FROM range_spill_mv WHERE ts = '2024-02-01 00:00:00.100'"
    // A COMPLETE preview covers every MV partition in one plan, and each of them is read from its own
    // recorded base partition: January's read stops where January's partition does, at 2024-02-01
    // 00:00:00.000, so the 123 milliseconds of February that p_prev holds are admitted by no term. Read as
    // one union for the batch instead, the base side would keep p_prev's own bound -- .123 -- and the row
    // would be admitted through February's range although no base partition recorded for February holds it.
    explain {
        sql "REFRESH MATERIALIZED VIEW range_spill_mv COMPLETE"
        contains "(ts < '2024-02-01 00:00:00.000')"
        notContains "2024-02-01 00:00:00.123"
    }
    // Written after the refresh, so p_prev is the stale partition the compensation re-reads.
    sql """INSERT INTO range_spill_base VALUES ("2024-01-20 00:00:00.000", 5)"""
    sql """ANALYZE TABLE range_spill_base WITH SYNC"""
    sql """set enable_materialized_view_rewrite = true"""
    explain {
        sql("select date_trunc(ts, 'month') as m, sum(amount) from range_spill_base "
                + "where ts >= '2024-02-01' and ts < '2024-03-01' group by date_trunc(ts, 'month')")
        notContains "UNION"
    }
    order_qt_range_spill """SELECT date_trunc(ts, 'month') AS m, SUM(amount) FROM range_spill_base
        WHERE ts >= '2024-02-01' AND ts < '2024-03-01' GROUP BY date_trunc(ts, 'month')"""
    // The same two partitions as one execution batch: a batch that writes February has a partition to put the
    // row in, so before the scopes were paired the row was not filtered but materialized there -- in a
    // partition whose record names p_kept alone, which holds nothing at that instant and would never be
    // compared against a later change of the row.
    sql """drop materialized view if exists range_spill_batch_mv"""
    sql """
        CREATE MATERIALIZED VIEW range_spill_batch_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL PARTITION BY (date_trunc(ts, 'month'))
        DISTRIBUTED BY HASH(ts) BUCKETS 1 PROPERTIES ("replication_num" = "1",
            "refresh_partition_num" = "2")
        AS SELECT ts, SUM(amount) AS total FROM range_spill_base GROUP BY ts
    """
    waitingMTMVTaskFinishedByMvName("range_spill_batch_mv")
    order_qt_range_spill_batch_mv_tail "SELECT COUNT(*) FROM range_spill_batch_mv WHERE ts = '2024-02-01 00:00:00.100'"
}
