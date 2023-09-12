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

suite("explode") {
    qt_explode """ select e1 from (select 1 k1) as t lateral view explode([1,2,3]) tmp1 as e1; """
    qt_explode_outer """ select e1 from (select 1 k1) as t lateral view explode_outer([1,2,3]) tmp1 as e1; """

    // array is null
    qt_explode """ select e1 from (select 1 k1) as t lateral view explode(null) tmp1 as e1; """
    qt_explode_outer """ select e1 from (select 1 k1) as t lateral view explode_outer(null) tmp1 as e1; """

    // array is empty
    qt_explode """ select e1 from (select 1 k1) as t lateral view explode([]) tmp1 as e1; """
    qt_explode_outer """ select e1 from (select 1 k1) as t lateral view explode_outer([]) tmp1 as e1; """

    // array with null elements
    qt_explode """ select e1 from (select 1 k1) as t lateral view explode([null,1,null]) tmp1 as e1; """
    qt_explode_outer """ select e1 from (select 1 k1) as t lateral view explode_outer([null,1,null]) tmp1 as e1; """

    sql """ DROP TABLE IF EXISTS d_table; """
    sql """
            create table d_table(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into d_table select 1,1,1,'a';"

    qt_test1 """select e1 from (select k1 from d_table) as t lateral view explode_numbers(5) tmp1 as e1;"""
    qt_test2 """select e1 from (select k1 from d_table) as t lateral view explode_numbers(5) tmp1 as e1 where e1=k1;"""
    qt_test3 """select e1,k1 from (select k1 from d_table) as t lateral view explode_numbers(5) tmp1 as e1;"""

    sql """ DROP TABLE IF EXISTS baseall_explode_numbers; """
    sql """
            CREATE TABLE `baseall_explode_numbers` (
            `k3` int(11) NULL
            ) ENGINE=OLAP
            duplicate KEY(`k3`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k3`) BUCKETS 5
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            );
        """
    sql "insert into baseall_explode_numbers values(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15);"
    qt_test4 """select k3,e from baseall_explode_numbers as U lateral view explode_numbers(5) tmp1 as e order by k3,e;"""
    qt_test5 """select k3,e from baseall_explode_numbers as U lateral view explode_numbers(10) tmp1 as e order by k3,e;"""
}
