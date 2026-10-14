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
        sql """insert into base_read_scope values ("${expiredDay}", 1), ("${keptDay}", 2), ("${today}", 3)"""

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

    // The same read with a partition_sync_limit window: the expired partition shares its `d` with the kept
    // one, and the default partition's row is in the same MV partition. The MV partition has to keep the
    // partitions the window kept plus the default partition's rows -- not the expired one, which no snapshot
    // names, or dropping it later would leave its row in the MV with the partition still judged synchronized.
    sql """drop materialized view if exists list_default_scope_mv"""
    sql """drop table if exists list_default_scope_base"""
    sql """
        CREATE TABLE list_default_scope_base (d DATE NOT NULL, k INT NOT NULL, amount BIGINT)
        DUPLICATE KEY(d, k)
        PARTITION BY LIST(d, k) (
            PARTITION p_expired VALUES IN ((\"2020-01-01\", 1)),
            PARTITION p_kept VALUES IN ((\"2020-01-01\", 2), (\"2038-01-01\", 2)),
            PARTITION p_default
        )
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")
    """
    sql """INSERT INTO list_default_scope_base VALUES
        (\"2020-01-01\", 1, 1), (\"2020-01-01\", 2, 2), (\"2038-01-01\", 2, 3), (\"2020-01-01\", 3, 4)"""
    sql """
        CREATE MATERIALIZED VIEW list_default_scope_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        PARTITION BY (d)
        DISTRIBUTED BY HASH(d) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\",
            \"partition_sync_limit\" = \"2\", \"partition_sync_time_unit\" = \"YEAR\")
        AS SELECT d, k, SUM(amount) AS total FROM list_default_scope_base GROUP BY d, k
    """
    waitingMTMVTaskFinishedByMvName("list_default_scope_mv")
    order_qt_list_default_scope "SELECT d, k, total FROM list_default_scope_mv"

    // And the expired partition is one no MV partition is recorded with, so dropping it is not a change the
    // MV partition has to answer for: what it holds was read from what it is recorded with.
    sql """ALTER TABLE list_default_scope_base DROP PARTITION p_expired"""
    order_qt_list_default_scope_dropped "SELECT d, k, total FROM list_default_scope_mv"

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
}
