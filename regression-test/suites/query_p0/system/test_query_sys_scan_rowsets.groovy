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

import java.text.SimpleDateFormat;
import java.util.Date;

suite("test_query_sys_scan_rowsets", "query,p0") {
    def dbName1 = "test_query_sys_scan_rowsets"

    if (!isCloudMode()) {
        log.info("not cloud mode")
        return
    }


    sql("CREATE DATABASE IF NOT EXISTS ${dbName1}")

    // test rowsets
    qt_desc_rowsets """ desc information_schema.rowsets """ 
    def rowsets_table_name = """ test_query_sys_scan_rowsets.test_query_rowset """  
    sql """ drop table if exists ${rowsets_table_name}  """ 

    sql """ 
        create table ${rowsets_table_name}( 
            a int , 
            b boolean , 
            c string ) 
        DISTRIBUTED BY HASH(`a`) BUCKETS 1 
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "enable_single_replica_compaction"="true"
        );
    """

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    def now = sdf.format(new Date()).toString();
    
    def rowsets_table_name_tablets = sql_return_maparray """ show tablets from ${rowsets_table_name}; """
    def tablet_id = rowsets_table_name_tablets[0].TabletId

    sql """ select * from ${rowsets_table_name};  """
    order_qt_rowsets1 """  select START_VERSION,END_VERSION from information_schema.rowsets where TABLET_ID=${tablet_id}  group by START_VERSION,END_VERSION order by START_VERSION,END_VERSION; """ 
    
    sql """ insert into  ${rowsets_table_name} values (1,0,"abc");  """ 
    sql """ select * from ${rowsets_table_name};  """
    order_qt_rowsets2 """  select START_VERSION,END_VERSION from information_schema.rowsets where TABLET_ID=${tablet_id}  group by START_VERSION,END_VERSION order by START_VERSION,END_VERSION; """ 
    
    sql """ insert into  ${rowsets_table_name} values (2,1,"hello world");  """ 
    sql """ insert into  ${rowsets_table_name} values (3,0,"dssadasdsafafdf");  """ 
    sql """ select * from ${rowsets_table_name};  """
    order_qt_rowsets3 """  select START_VERSION,END_VERSION from information_schema.rowsets where TABLET_ID=${tablet_id}  group by START_VERSION,END_VERSION order by START_VERSION,END_VERSION; """ 
    
    sql """ insert into  ${rowsets_table_name} values (4,0,"abcd");  """
    sql """ select * from ${rowsets_table_name};  """
    order_qt_rowsets4 """  select START_VERSION,END_VERSION from information_schema.rowsets where TABLET_ID=${tablet_id} and NEWEST_WRITE_TIMESTAMP>='${now}' group by START_VERSION,END_VERSION order by START_VERSION,END_VERSION; """ 
}