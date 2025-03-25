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

suite("test_truncate_multiple") {
    def table = "test_truncate_multiple"

    // create table and insert data
    sql """ drop table if exists ${table}"""
    sql """
    create table ${table} (
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

    sql """ insert into ${table} values(1, 'a', '2022-01-02'); """
    sql """ insert into ${table} values(2, 'a', '2023-01-02'); """
    sql """ insert into ${table} values(3, 'a', '2024-01-02'); """
    sql """ SYNC;"""

    qt_select_check_1 """ select * from  ${table} order by id,name,da; """

    sql """ truncate  table ${table}; """

    qt_select_check_2 """ select * from  ${table} order by id,name,da; """

    // add only one record, so that some paritions can be empty
    sql """ insert into ${table} values(100, 'a', '2022-01-02'); """
    // check empty partition drop
    sql """ SYNC;"""
    qt_select_check_3 """ select * from  ${table} order by id,name,da; """    
    sql """ truncate  table ${table}; """

    qt_select_check_4 """ select * from  ${table} order by id,name,da; """    
    // after truncate , there would be new partition created,
    // drop forcefully so that this partitions not kept in recycle bin.
    sql """ ALTER TABLE ${table} DROP PARTITION p3 force; """
    sql """ ALTER TABLE ${table} DROP PARTITION p4 force; """
    sql """ ALTER TABLE ${table} DROP PARTITION p5 force; """

    // recover the data from the recycel bin , data present before truncate.
    sql """ recover partition p3  from ${table}; """
    sql """ recover partition p4  from ${table}; """
    sql """ recover partition p5  from ${table}; """    

    // data should match whatever before truncate.
    qt_select_check_5 """ select * from  ${table} order by id,name,da; """
}
