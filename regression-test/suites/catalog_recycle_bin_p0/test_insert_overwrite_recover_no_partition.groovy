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

suite("test_insert_overwrite_recover_no_partition") {
    def table = "test_insert_overwrite_recover_no_partition"
    def table_bk = "test_insert_overwrite_recover_no_partition_backup"
    // create table and insert data for range.
    sql """ drop table if exists ${table} force"""
    sql """
    create table ${table} (
        `id` int(11),
        `name` varchar(128),
        `da` date
    )
    engine=olap
    duplicate key(id)
    distributed by hash(id) buckets 2
    properties(
        "replication_num"="1",
        "light_schema_change"="true"
    );
    """

    sql """ insert into ${table} values(1, 'a', '2022-01-02'); """
    sql """ insert into ${table} values(2, 'a', '2023-01-02'); """
    sql """ insert into ${table} values(3, 'a', '2024-01-02'); """
    sql """ SYNC;"""

    qt_select_check_1 """ select * from  ${table} order by id,name,da; """

    sql """ insert overwrite  table ${table} values(3, 'a', '2024-01-02'); """

    
    qt_select_check_2 """ select * from  ${table} order by id,name,da; """
    
    // now unpartition data is kept inside the recycle bin.
    // we need to recover it as another partition in the table.
    sql """ recover partition ${table} as p2  from ${table}; """

    // create a table to copy the data only for partition p2.

    sql """ drop table if exists ${table_bk} force"""
    sql """
    create table ${table_bk} (
        `id` int(11),
        `name` varchar(128),
        `da` date
    )
    engine=olap
    duplicate key(id)
    distributed by hash(id) buckets 2
    properties(
        "replication_num"="1",
        "light_schema_change"="true"
    );
    """
    sql """ insert into ${table_bk} select * from ${table} partition p2; """

    sql """ alter table ${table} replace with table ${table_bk}; """

    // data from the select should be same as data before overwrite.
    qt_select_check_3 """ select * from  ${table} order by id,name,da; """

}
