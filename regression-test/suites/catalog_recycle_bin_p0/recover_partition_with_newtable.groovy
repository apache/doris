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

suite("recover_partition_with_newtable") {
    def dbName = "recover_partition_with_newtable_db"
    sql "drop database if exists ${dbName} force"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "use ${dbName}"

    def table = "test_table"
    def table_new = "test_table_new"
    // create table and insert data
    sql """ drop table if exists ${table} """
    sql """
    create table ${table} (
        `id` int(11),
        `name` varchar(128),
        `da` date
    )
    engine=olap
    duplicate key(id)
    partition by range(da)(
        PARTITION p1 VALUES LESS THAN ('2023-01-01'),
        PARTITION p2 VALUES LESS THAN ('2024-01-01'),
        PARTITION p3 VALUES LESS THAN ('2025-01-01')
    )
    distributed by hash(id) buckets 2
    properties(
        "replication_num"="1",
        "light_schema_change"="true"
    );
    """

    sql """ insert into ${table} values(1, 'a', '2022-01-02'); """
    sql """ insert into ${table} values(2, 'a', '2023-01-02'); """
    sql """ insert into ${table} values(3, 'a', '2024-01-02'); """

    qt_select_check_1 """select * from ${table}  order by id,name,da; """

    // drop partition
    sql """ ALTER TABLE ${table} DROP PARTITION p1; """
    sql """ ALTER TABLE ${table} DROP PARTITION p2; """
    sql """ ALTER TABLE ${table} DROP PARTITION p3; """ 


    sql """ RECOVER PARTITION p1 from ${table} as ${table_new}; """ 
    sql """ RECOVER PARTITION p2 from ${table} as ${table_new}; """
    sql """ RECOVER PARTITION p3 from ${table} as ${table_new}; """    
        
    qt_select_check_1 """select * from ${table_new}  order by id,name,da; """

}
