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

suite("test_zone_map_delete") {
    def tableName = "test_zone_map_delete_tbl"

    // comparison predicate
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (   `k1` int(11) NULL,   `k2` int(11) NULL,   `v1` int(11) NULL )DUPLICATE KEY(`k1`,k2) DISTRIBUTED BY HASH(`k1`) BUCKETS 1 PROPERTIES("replication_num" = "1");"""
    sql """insert into ${tableName} values(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5);"""
    sql """delete from ${tableName} where v1 > 3;"""
    qt_sql """select * from ${tableName} ORDER BY k1;"""

    // in predicate
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (   `k1` int(11) NULL,   `k2` int(11) NULL,   `v1` int(11) NULL )DUPLICATE KEY(`k1`,k2) DISTRIBUTED BY HASH(`k1`) BUCKETS 1 PROPERTIES("replication_num" = "1");"""
    sql """insert into ${tableName} values(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5);"""
    sql """delete from ${tableName} where v1 in (3);"""
    qt_sql """select * from ${tableName} ORDER BY k1;"""

    // null predicate
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (   `k1` int(11) NULL,   `k2` int(11) NULL,   `v1` int(11) NULL )DUPLICATE KEY(`k1`,k2) DISTRIBUTED BY HASH(`k1`) BUCKETS 1 PROPERTIES("replication_num" = "1");"""
    sql """insert into ${tableName} values(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5);"""
    sql """delete from ${tableName} where v1 IS NOT NULL;"""
    qt_sql """select * from ${tableName} ORDER BY k1;"""

    // not in predicate
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (   `k1` int(11) NULL,   `k2` int(11) NULL,   `v1` int(11) NULL )DUPLICATE KEY(`k1`,k2) DISTRIBUTED BY HASH(`k1`) BUCKETS 1 PROPERTIES("replication_num" = "1");"""
    sql """insert into ${tableName} values(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5);"""
    sql """delete from ${tableName} where v1 not in (3);"""
    qt_sql """select * from ${tableName} ORDER BY k1;"""

    // not in predicate
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (   `k1` int(11) NULL,   `k2` int(11) NULL,   `v1` int(11) NULL )DUPLICATE KEY(`k1`,k2) DISTRIBUTED BY HASH(`k1`) BUCKETS 1 PROPERTIES("replication_num" = "1");"""
    sql """insert into ${tableName} values(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5);"""
    sql """delete from ${tableName} where v1 not in (0);"""
    qt_sql """select * from ${tableName} ORDER BY k1;"""

    sql """ DROP TABLE IF EXISTS ${tableName} """

    // =========================
    sql """
    CREATE TABLE IF NOT EXISTS ${tableName} (
        `k1` bigint(20) NULL,
        `k2` largeint(40) NULL,
        `k3` largeint(40) NULL
    ) ENGINE=OLAP
    AGGREGATE KEY(`k1`, `k2`, `k3`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "true"
    );
    """

    sql """ truncate table ${tableName}; """

    sql """ insert into ${tableName} values(0,1,11),(0,1,22),(0,1,33),(0,1,44),(0,1,55),(0,1,66),(0,1,77),(0,1,88),(0,1,99),(0,1,100),(0,1,101),(0,1,102),(0,1,111),(0,1,122),(0,1,133),(0,1,144),(0,1,155),(0,1,166),(0,1,177),(0,1,188),(0,1,199),(0,1,200),(0,1,201),(0,1,202);"""

    sql """ delete from ${tableName} where k2=0;"""

    sql """ delete from ${tableName} where k2=1;"""

    qt_sql """ select k2,k3 from ${tableName} ORDER BY k3;"""

    qt_sql """ select k2,k3 from ${tableName} where k2 = 1 ORDER BY k3;"""


    sql """ truncate table ${tableName}; """

    sql """ insert into ${tableName} values(0,1,11),(0,1,22),(0,1,33),(0,1,44),(0,1,55),(0,1,66),(0,1,77),(0,1,88),(0,1,99),(0,1,100),(0,1,101),(0,1,102),(0,1,111),(0,1,122),(0,1,133),(0,1,144),(0,1,155),(0,1,166),(0,1,177),(0,1,188),(0,1,199),(0,1,200),(0,1,201),(0,1,202);"""

    sql """ delete from ${tableName} where k2=1 and k2=0;"""

    qt_sql """ select k2,k3 from ${tableName} ORDER BY k3;"""

    qt_sql """ select k2,k3 from ${tableName} where k2 = 1 ORDER BY k3;"""


    sql """ truncate table ${tableName}; """

    sql """ insert into ${tableName} values(0,1,11),(0,1,22),(0,1,33),(0,1,44),(0,1,55),(0,1,66),(0,1,77),(0,1,88),(0,1,99),(0,1,100),(0,1,101),(0,1,102),(0,1,111),(0,1,122),(0,1,133),(0,1,144),(0,1,155),(0,1,166),(0,1,177),(0,1,188),(0,1,199),(0,1,200),(0,1,201),(0,1,202);"""

    sql """ delete from ${tableName} where k2 is null and k3=00;"""

    qt_sql """ select k2,k3 from ${tableName} ORDER BY k3;"""

    qt_sql """ select k2,k3 from ${tableName} where k2 is null ORDER BY k3;"""

    sql """delete from ${tableName} where k2 is not null and k3=11;"""

    qt_sql """select k2,k3 from ${tableName} ORDER BY k3;"""

    qt_sql """select k2,k3 from ${tableName} where k2 is not null ORDER BY k3;"""

    sql """delete from ${tableName} where k2=1 and k3=22;"""

    qt_sql """select k2,k3 from ${tableName} ORDER BY k3;"""

    qt_sql """select k2,k3 from ${tableName} where k2=1 ORDER BY k3;"""

    sql """delete from ${tableName} where k2!=0 and k3=33;"""

    qt_sql """select k2,k3 from ${tableName} ORDER BY k3;"""

    qt_sql """select k2,k3 from ${tableName} where k2!=0 ORDER BY k3;"""

    sql """delete from ${tableName} where k2 not in (0) and k3=44;"""

    qt_sql """select k2,k3 from ${tableName} ORDER BY k3;"""

    qt_sql """select k2,k3 from ${tableName} where k2 not in (0) ORDER BY k3;"""

    sql """delete from ${tableName} where k2=1 and k3 >= 11 and k3 <=200;"""

    qt_sql """select k2,k3 from ${tableName} ORDER BY k3;"""

    qt_sql """select k2,k3 from ${tableName} where k3 = 201 ORDER BY k3;"""


    sql """truncate table ${tableName};"""

    sql """insert into ${tableName} values(0,1,11),(0,1,22),(0,1,33),(0,1,44),(0,1,55),(0,1,66),(0,1,77),(0,1,88),(0,1,99),(0,1,100),(0,1,101),(0,1,102),(0,1,111),(0,1,122),(0,1,133),(0,1,144),(0,1,155),(0,1,166),(0,1,177),(0,1,188),(0,1,199),(0,1,200),(0,1,201),(0,1,202);"""

    sql """delete from ${tableName} where k2=1 and k3 <=202 and k3 >= 33;"""

    qt_sql """select k2,k3 from ${tableName} ORDER BY k3;"""

    qt_sql """select k2,k3 from ${tableName} where k3 = 11 ORDER BY k3;"""


    sql """truncate table ${tableName};"""

    sql """insert into ${tableName} values(0,1,11),(0,1,22),(0,1,33),(0,1,44),(0,1,55),(0,1,66),(0,1,77),(0,1,88),(0,1,99),(0,1,100),(0,1,101),(0,1,102),(0,1,111),(0,1,122),(0,1,133),(0,1,144),(0,1,155),(0,1,166),(0,1,177),(0,1,188),(0,1,199),(0,1,200),(0,1,201),(0,1,202);"""

    sql """delete from ${tableName} where k2 is null;"""

    qt_sql """select k2,k3 from ${tableName} ORDER BY k3;"""

    qt_sql """select k2,k3 from ${tableName} where k2 is not null ORDER BY k3;"""


    sql """truncate table ${tableName};"""

    sql """insert into ${tableName} values(0,null,11),(0,null,22),(0,null,33),(0,null,44),(0,null,55),(0,null,66),(0,null,77),(0,null,88),(0,null,99),(0,null,100),(0,null,101),(0,null,102),(0,null,111),(0,null,122),(0,null,133),(0,null,144),(0,null,155),(0,null,166),(0,null,177),(0,null,188),(0,null,199),(0,null,200),(0,null,201),(0,null,202);"""

    sql """delete from ${tableName} where k2 is not null;"""

    qt_sql """select k2,k3 from ${tableName} ORDER BY k3;"""

    qt_sql """select k2,k3 from ${tableName} where k2 is null ORDER BY k3;"""


    // less than predicate
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (   `k1` int(11) NULL,   `k2` int(11) NULL,   `v1` int(11) NULL )DUPLICATE KEY(`k1`,k2) DISTRIBUTED BY HASH(`k1`) BUCKETS 1 PROPERTIES("replication_num" = "1");"""
    sql """insert into ${tableName} values(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5);"""
    sql """delete from ${tableName} where v1 < 3;"""
    qt_less_than_delete """select * from ${tableName} ORDER BY k1;"""

    // The tables above all fit in one page, so they only tell whether a page is kept. This one
    // is large enough to have many pages: a page holds 16384 rows, so runs of 50000 equal
    // values leave whole pages sitting inside a single run.
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
    CREATE TABLE ${tableName} (
        `k1` int NOT NULL,
        `v1` int NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
    PROPERTIES (
    "replication_num" = "1",
    "disable_auto_compaction" = "true"
    );
    """
    sql """insert into ${tableName} select number, number div 50000 from numbers("number" = "150000");"""

    // Delete one whole run. The pages inside it hold that value only, so the delete condition
    // covers them completely and they can be dropped without being read.
    sql """delete from ${tableName} where v1 in (1);"""
    qt_whole_page_delete """select v1, count(*) from ${tableName} group by v1 order by v1;"""

    // The page skip only runs for columns that also carry a query predicate, so query on v1.
    // `v1 >= 1` is false for some rows, which keeps it from being dropped as always true.
    // 50000 rows are deleted; anything less reaching the row level filter means whole pages
    // were dropped by the zone map first.
    sql """ set enable_profile = true; """
    def pageSkipQueryId = "test_zone_map_delete_page_skip_" + System.currentTimeMillis()
    profile(pageSkipQueryId) {
        run {
            sql "/* ${pageSkipQueryId} */ select count(*) from ${tableName} where v1 >= 1"
        }
        check { profileString, exception ->
            def matcher = java.util.regex.Pattern
                    .compile("RowsDelFiltered:\\s*(?:[\\d.]+[KMB]?\\s*\\()?(\\d+)\\)?")
                    .matcher(profileString)
            assertTrue(matcher.find(), "RowsDelFiltered is missing from the profile")
            def rowsDelFiltered = Integer.parseInt(matcher.group(1))
            log.info("rows the delete condition filtered one by one: {}", rowsDelFiltered)
            assertTrue(rowsDelFiltered < 50000,
                       "expected whole pages to be skipped, RowsDelFiltered=" + rowsDelFiltered)
        }
    }
    sql """ set enable_profile = false; """

    sql """ DROP TABLE IF EXISTS ${tableName} """

}
