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

// This will test database recover 

suite("test_recover_all") {
    def testTable = "test_table"
    def db = "test_recover_all_db"
    sql "drop database IF EXISTS $db force"
    sql "CREATE DATABASE IF NOT EXISTS $db "
    sql "use $db "
    sql """
        CREATE TABLE IF NOT EXISTS table1 (
         `id` int(11),
        `name` varchar(128),
        `da` date
    )
    engine=olap
    duplicate key(id)
    partition by range(da)(
        PARTITION p3 VALUES LESS THAN ('2023-01-01'),
        PARTITION p4 VALUES LESS THAN ('2024-01-01'),
        PARTITION p5 VALUES LESS THAN ('2025-01-01')
    )
    distributed by hash(id) buckets 2
    properties(
        "replication_num"="1",
        "light_schema_change"="true"
    );
    """
    sql """
        CREATE TABLE IF NOT EXISTS table2 (
        `id` int(11),
        `name` varchar(128),
        `da` date
    )
    engine=olap
    duplicate key(id)
    partition by range(da)(
        PARTITION p3 VALUES LESS THAN ('2023-01-01'),
        PARTITION p4 VALUES LESS THAN ('2024-01-01'),
        PARTITION p5 VALUES LESS THAN ('2025-01-01')
    )
    distributed by hash(id) buckets 2
    properties(
        "replication_num"="1",
        "light_schema_change"="true"
    );
    """
    qt_select "show tables";
    
    sql " drop table table2"
    qt_select2 "show tables";
    checkNereidsExecute("recover table table2;")
    qt_select3 "show tables";
    sql " drop table table2"
    qt_select4 "show tables";    
    checkNereidsExecute("recover table table2 as table3;")
    qt_select5 "show tables";

    sql """ insert into table3 values(1, 'a', '2022-01-02'); """
    sql """ insert into table3 values(2, 'a', '2023-01-02'); """
    sql """ insert into table3 values(3, 'a', '2024-01-02'); """
    sql """ SYNC;"""
    
    qt_select_check_1 """ select * from  table3 order by id,name,da; """
    sql """ ALTER TABLE table3 DROP PARTITION p3""";
    qt_select_partitions "select PARTITION_NAME from information_schema.partitions where TABLE_NAME = \"table3\" and TABLE_SCHEMA=\"${db}\" order by PARTITION_NAME"; 
    sql """ recover partition p3  from table3; """
    qt_select_partitions_2 "select PARTITION_NAME from information_schema.partitions where TABLE_NAME = \"table3\" and TABLE_SCHEMA=\"${db}\" order by PARTITION_NAME"; 

    qt_select_check_1 """ select * from  table3 order by id,name,da; """
    sql """ ALTER TABLE table3 DROP PARTITION p3""";
    qt_select_partitions_3 "select PARTITION_NAME from information_schema.partitions where TABLE_NAME = \"table3\" and TABLE_SCHEMA=\"${db}\" order by PARTITION_NAME"; 
    sql """ recover partition p3 as p1 from table3; """
    qt_select_partitions_4 "select PARTITION_NAME from information_schema.partitions where TABLE_NAME = \"table3\" and TABLE_SCHEMA=\"${db}\" order by PARTITION_NAME"; 
    qt_select_check_1 """ select * from  table3 order by id,name,da; """
}

