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

suite("test_insert_tablet_sink") {
    sql """drop table if exists `table_largeint`;"""
    sql """drop table if exists `tmp_varchar`;"""

    sql """
            CREATE TABLE `tmp_varchar` (
            `k1` bigint(20) not NULL,
            `c_varchar` varchar(65533) not NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, c_varchar)
        COMMENT 'OLAP'
        AUTO PARTITION BY list (c_varchar) ()
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
    """

    sql """
    CREATE TABLE `table_largeint` (
                `k1` bigint(20) not NULL,
                `c_largeint` largeint not NULL,
                str string null
            ) ENGINE=OLAP
            UNIQUE KEY(`k1`, c_largeint)
            COMMENT 'OLAP'
            AUTO PARTITION BY list (c_largeint) ()
            DISTRIBUTED BY HASH(`k1`) BUCKETS 10
            PROPERTIES("replication_num" = "1");

    """

    sql """insert into tmp_varchar values(1, "170141183460469231731687303715884105727")"""
    sql """insert into tmp_varchar values(2, "-170141183460469231731687303715884105728")"""
    sql """insert into tmp_varchar values(3,'333');"""
    sql """insert into tmp_varchar values(4,'444');"""
    sql """insert into tmp_varchar values(5,'555');"""
    sql """insert into tmp_varchar values(6,'666');"""
    
    qt_select """select * from tmp_varchar order by 1;"""

    sql """ set skip_delete_bitmap = true; """
    sql """ set enable_memtable_on_sink_node = true; """
    sql """ set parallel_pipeline_task_num = 2; """


    sql """ insert into table_largeint select k1,c_varchar,cast(rand() * 50000000 as bigint) from tmp_varchar where k1>=3; """
    explain {
        sql "insert into table_largeint select k1,c_varchar,cast(rand() * 50000000 as bigint) from tmp_varchar;"
        contains "TABLET_SINK_SHUFFLE_PARTITIONED"
    }
    
    sql """ insert into table_largeint select k1,c_varchar,cast(rand() * 50000000 as bigint) from tmp_varchar; """
    qt_select """select k1,c_largeint from table_largeint order by 1;"""
}
