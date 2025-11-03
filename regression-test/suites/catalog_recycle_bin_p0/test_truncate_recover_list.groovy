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

suite("test_truncate_recover_list") {
    def table = "test_truncate_recover_list"

    // create table and insert data
    sql """ drop table if exists ${table}"""
    sql """
        CREATE TABLE  ${table} (k1 CHAR(5) NOT NULL, v1 CHAR(5) REPLACE NOT NULL )
        AGGREGATE KEY(k1)
        PARTITION BY LIST(k1) ( PARTITION p1 VALUES IN ("a","b"), PARTITION p2 VALUES IN ("c","d") )
        DISTRIBUTED BY HASH(k1) BUCKETS 5 PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
    sql "insert into ${table} values('a', 'b')"
    sql "insert into ${table} values('c', 'd')"

    sql """ SYNC;"""

    qt_select_check_1 """ select * from  ${table} order by k1,v1; """

    sql """ truncate  table ${table}; """

    qt_select_check_2 """ select * from  ${table} order by k1,v1; """
    // after truncate , there would be new partition created,
    // drop forcefully so that this partitions not kept in recycle bin.    
    sql """ ALTER TABLE ${table} DROP PARTITION p1 force; """
    sql """ ALTER TABLE ${table} DROP PARTITION p2 force; """

    sql """ recover partition p1  from ${table}; """
    sql """ recover partition p2  from ${table}; """

    qt_select_check_3 """ select * from  ${table} order by k1,v1; """
    
    sql """ truncate  table ${table} PARTITION(p1); """
    qt_select_check_4 """ select * from  ${table} order by k1,v1; """
    // drop forcefully the partition which is created in the truncate partition process.
    sql """ ALTER TABLE ${table} DROP PARTITION p1 force; """
    sql """ recover partition p1  from ${table}; """
    qt_select_check_5 """ select * from  ${table} order by k1,v1; """   

}
