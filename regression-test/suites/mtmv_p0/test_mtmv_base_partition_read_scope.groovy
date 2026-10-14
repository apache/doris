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

import org.junit.Assert

suite("test_mtmv_base_partition_read_scope") {
    // A refresh reads the base partitions the MV partition is recorded with, and no others. The window of
    // partition_sync_limit below keeps the last two days, so the day before them is recorded nowhere: it
    // must not be read either, or its rows would sit in the MV partition -- whose key range does cover
    // them -- while the snapshot says the MV does not hold that partition. A base partition dropped after
    // such a read would then be invisible to the sync check, and the transparent rewrite would serve the
    // rows of a partition the base table no longer has.
    //
    // The MV partition is a year and the base table's are days, so the range it is read through is wider
    // than what it is recorded with. Days are taken relative to today, and the assertion is against what
    // the window keeps rather than a fixed number of rows: when today is early enough in January that the
    // kept days fall in the new year, the MV partition that covers the expired day does not exist and both
    // the MV and the expectation lose it -- the case is then not exercised, but it is still asserted.
    def today = java.time.LocalDate.now()
    def expiredDay = today.minusDays(5)
    def keptDay = today.minusDays(1)

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

    def windowStart = today.minusDays(2)
    def keptRows = sql("select count(*) from base_read_scope where k1 >= '${windowStart}'")[0][0]
    def mvRows = sql("select count(*) from mv_read_scope")[0][0]
    Assert.assertEquals(keptRows.toString(), mvRows.toString())

    // And the partition the window left out is the one missing, not some other.
    def expiredRows = sql("select count(*) from mv_read_scope where k1 = '${expiredDay}'")[0][0]
    Assert.assertEquals("0", expiredRows.toString())
}
