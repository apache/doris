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

 suite("test_tenant_level_colocate") {
     def colocateTableNameMaster = "colocate_table_master"
     def colocateTableNameSlave = "colocate_table_slave"
     def rightTable = "right_table"

     sql """ DROP TABLE IF EXISTS ${rightTable} """
     sql """
         CREATE TABLE IF NOT EXISTS `${rightTable}` (
           `k1` int(11) NOT NULL COMMENT "",
           `v1` int(11) NOT NULL COMMENT ""
         ) ENGINE=OLAP
         DUPLICATE KEY(`k1`)
         DISTRIBUTED BY HASH(`k1`) BUCKETS 10
         PROPERTIES (
           "replication_allocation" = "tag.location.default: 1"
         )
     """

     sql """ DROP TABLE IF EXISTS ${colocateTableNameMaster} """
     sql """
         CREATE TABLE IF NOT EXISTS `${colocateTableNameMaster}` (
           `c1` int(11) NULL COMMENT "",
           `c2` int(11) NULL COMMENT "",
           `m1` int(11) NULL COMMENT ""
         ) ENGINE=OLAP
         DUPLICATE KEY(`c1`, `c2`)
         PARTITION BY RANGE(`c2`)
         (PARTITION p1 VALUES [("-2147483648"), ("2")),
         PARTITION p2 VALUES [("2"), (MAXVALUE)))
         DISTRIBUTED BY HASH(`c1`) BUCKETS 8
         PROPERTIES (
           "replication_allocation" = "tag.location.default: 1",
           "colocate_group" = "tag.location.default:group1"
         )
     """

     sql """ DROP TABLE IF EXISTS ${colocateTableNameSlave} """
     sql """
         CREATE TABLE IF NOT EXISTS `${colocateTableNameSlave}` (
           `c1` int(11) NULL COMMENT "",
           `c2` int(11) NULL COMMENT "",
           `m2` int(11) NULL COMMENT ""
         ) ENGINE=OLAP
         DUPLICATE KEY(`c1`, `c2`)
         PARTITION BY RANGE(`c2`)
         (PARTITION p1 VALUES [("-2147483648"), ("2")),
         PARTITION p2 VALUES [("2"), (MAXVALUE)))
         DISTRIBUTED BY HASH(`c1`) BUCKETS 16
         PROPERTIES (
           "replication_allocation" = "tag.location.default: 1",
           "colocate_slave" = "tag.location.default:group1"
         )
     """

     sql """ INSERT INTO ${colocateTableNameMaster} VALUES
         (0, 0, 0),
         (1, 1, 1),
         (2, 2, 2)
         ;
     """

     sql """ INSERT INTO ${colocateTableNameSlave} VALUES
         (1, 1, 1),
         (2, 2, 2),
         (3, 3, 3)
         ;
     """

     sql """ INSERT INTO ${rightTable} VALUES
         (1, 1),
         (2, 2),
         (3, 3),
         (4, 4)
         ;
     """

     order_qt_select_q1 """  select k1,c1,m1,v1 from ${colocateTableNameMaster} right outer join ${rightTable} on ${colocateTableNameMaster}.c1 = ${rightTable}.k1 order by k1; """
     order_qt_select_q2 """  select k1,c1,m2,v1 from ${colocateTableNameSlave} right outer join ${rightTable} on ${colocateTableNameSlave}.c1 = ${rightTable}.k1 order by k1; """

     order_qt_select_q3 """
        with t1 as (
            select c1, sum(m1) as m1 from ${colocateTableNameMaster} group by c1
        )
        select k1,c1,m1,v1 from t1 right outer join ${rightTable} on t1.c1 = ${rightTable}.k1 order by k1;
     """

     order_qt_select_q4 """
        with t1 as (
            select c1, sum(m2) as m2 from ${colocateTableNameSlave} group by c1
        )
        select k1,c1,m2,v1 from t1 right outer join ${rightTable} on t1.c1 = ${rightTable}.k1 order by k1;
     """

     order_qt_select_q5 """
        with t1 as (
            select c1, sum(m1) as m1 from ${colocateTableNameMaster} group by c1
        ),
        t2 as (
            select c1, sum(m2) as m2 from ${colocateTableNameSlave} group by c1
        )
        select k1, m1, m2, v1
        from t1 join t2 on t1.c1 = t2.c1 right join ${rightTable} on t1.c1 = ${rightTable}.k1
        order by k1;
     """

     order_qt_select_q6 """
        with t1 as (
            select c1, sum(m1) as m1 from ${colocateTableNameMaster} group by c1
        ),
        t2 as (
            select c1, sum(m2) as m2 from ${colocateTableNameSlave} group by c1
        )
        select /*+ SET_VAR(enable_nereids_distribute_planner=false)*/
            k1, m1, m2, v1
        from t1 join t2 on t1.c1 = t2.c1 right join ${rightTable} on t1.c1 = ${rightTable}.k1
        order by k1;
     """

     explain {
         sql """
                with t1 as (
                    select c1, sum(m1) as m1 from ${colocateTableNameMaster} group by c1
                ),
                t2 as (
                    select c1, sum(m2) as m2 from ${colocateTableNameSlave} group by c1
                )
                select /*+ SET_VAR(disable_join_reorder=true)*/
                    k1, m1, m2, v1
                from t1 join t2 on t1.c1 = t2.c1 right join ${rightTable} on t1.c1 = ${rightTable}.k1
                order by k1;
            """
         contains "join op: INNER JOIN(COLOCATE[])[]"
         contains "join op: RIGHT OUTER JOIN(BUCKET_SHUFFLE)[]"
     }

     sql """ DROP TABLE IF EXISTS ${rightTable} FORCE """
     sql """ DROP TABLE IF EXISTS ${colocateTableNameMaster} FORCE """
     sql """ DROP TABLE IF EXISTS ${colocateTableNameSlave} FORCE """
 }