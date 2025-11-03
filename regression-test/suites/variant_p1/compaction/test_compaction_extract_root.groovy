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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_compaction_extract_root", "p1") {
    def tableName = "test_t"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            k bigint,
            v variant<properties("variant_max_subcolumns_count" = "2")>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
        );
    """

    sql """insert into ${tableName}  select 0, '{"a": 11245, "b" : {"state" : "open", "code" : 2}}'  as json_str
        union  all select 8, '{"a": 1123}' as json_str union all select 0, '{"a" : 1234, "xxxx" : "aaaaa"}' as json_str from numbers("number" = "4096") limit 4096 ;"""


    sql """insert into ${tableName} select 1, '{"a": 11245, "b" : {"state" : "colse", "code" : 2}}'  as json_str
        union  all select 1, '{"a": 1123}' as json_str union all select 1, '{"a" : 1234, "xxxx" : "bbbbb"}' as json_str from numbers("number" = "4096") limit 4096 ;"""


    sql """insert into ${tableName} select 2, '{"a": 11245, "b" : {"state" : "flat", "code" : 3}}'  as json_str
        union  all select 2, '{"a": 1123}' as json_str union all select 2, '{"a" : 1234, "xxxx" : "ccccc"}' as json_str from numbers("number" = "4096") limit 4096 ;"""


    sql """insert into ${tableName}  select 3, '{"a" : 1234, "xxxx" : 4, "point" : 5}'  as json_str
        union  all select 3, '{"a": 1123}' as json_str union all select 3, '{"a": 11245, "b" : 42003}' as json_str from numbers("number" = "4096") limit 4096 ;"""


    sql """insert into ${tableName} select 4, '{"a" : 1234, "xxxx" : "eeeee", "point" : 5}'  as json_str
        union  all select 4, '{"a": 1123}' as json_str union all select 4, '{"a": 11245, "b" : 42004}' as json_str from numbers("number" = "4096") limit 4096 ;"""


    sql """insert into ${tableName} select 5, '{"a" : 1234, "xxxx" : "fffff", "point" : 42000}'  as json_str
        union  all select 5, '{"a": 1123}' as json_str union all select 5, '{"a": 11245, "b" : 42005}' as json_str from numbers("number" = "4096") limit 4096 ;"""

    // // fix cast to string tobe {}
    // qt_select_b_1 """ SELECT count(cast(v['b'] as string)) FROM test_t where cast(v['b'] as string) != '{}' """
    qt_select_b_2 """ SELECT count(cast(v['b'] as int)) FROM test_t"""
    // TODO, sparse columns with v['b'] will not be merged in hierachical_data_reader with sparse columns
    // qt_select_b_2 """ select v['b'] from test_t where  cast(v['b'] as string) != '42005' and  cast(v['b'] as string) != '42004' and  cast(v['b'] as string) != '42003' order by cast(v['b'] as string); """

    qt_select_1_bfcompact """select v['b'] from test_t where k = 0 and cast(v['a'] as int) = 11245;"""

    //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
    def tablets = sql_return_maparray """ show tablets from ${tableName}; """

    // trigger compactions for all tablets in ${tableName}
    trigger_and_wait_compaction(tableName, "cumulative")

    // fix cast to string tobe {}
    qt_select_b_3 """ SELECT count(cast(v['b'] as string)) FROM test_t"""
    qt_select_b_4 """ SELECT count(cast(v['b'] as int)) FROM test_t"""
    // TODO, sparse columns with v['b'] will not be merged in hierachical_data_reader with sparse columns
    // qt_select_b_5 """ select v['b'] from test_t where  cast(v['b'] as string) != '42005' and  cast(v['b'] as string) != '42004' and  cast(v['b'] as string) != '42003' order by cast(v['b'] as string); """

    qt_select_1 """select v['b'] from test_t where k = 0 and cast(v['a'] as int) = 11245;"""
    
}
