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
    def today = java.time.LocalDate.now()
    def expiredDay = today.minusDays(5)
    def keptDay = today.minusDays(1)
    // The expired day is read by a refresh only while it falls inside a retained MV partition's range, and
    // both days are inside the same year only from the third of January on. Skipped rather than asserted
    // when they are not, so that a run that exercises the case is a run that can fail without the change.
    org.junit.Assume.assumeTrue("the expired day and today are in the same year",
            expiredDay.getYear() == today.getYear())

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
    order_qt_list_scope "SELECT d, region, total FROM list_scope_mv ORDER BY d, region"
}
