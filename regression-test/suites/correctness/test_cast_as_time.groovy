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

suite("test_cast_as_time") {
     sql """ DROP TABLE IF EXISTS tbl_cast_as_time """
      sql """
        CREATE TABLE tbl_cast_as_time (
            id INT DEFAULT '10',
            str VARCHAR(32) DEFAULT ''
        ) ENGINE=OLAP
        AGGREGATE KEY(id,str)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES (
         "replication_allocation" = "tag.location.default: 1",
         "in_memory" = "false",
         "storage_format" = "V2"
        );
    """
    sql 'set enable_nereids_planner=true'
    sql """
        insert into tbl_cast_as_time values(300,'19:18:17')
    """
    sql """
        insert into tbl_cast_as_time values(360,'30:20')
    """
    sql """
        insert into tbl_cast_as_time values(202020,'400')
    """
    qt_select1 """
        select cast(id as time) from tbl_cast_as_time order by id
    """
    qt_select2 """
        select cast(str as time) from tbl_cast_as_time order by id
    """
    qt_select3 """
        select cast('2023-02-21 19:19:19' as time)
    """    
    qt_select4 """
       select cast("10:10:10" as time)
    """    
    qt_select5 """
       select cast("10:10:10" as datetimev2)
    """   
}