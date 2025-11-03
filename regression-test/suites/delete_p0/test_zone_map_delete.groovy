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


    sql """ DROP TABLE IF EXISTS ${tableName} """

}
