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

suite("test_compaction_extract_root", "p1,nonConcurrent") {
    def tableName = "test_t"

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
    def set_be_config = { key, value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }
    String backend_id;
    backend_id = backendId_to_backendIP.keySet()[0]
    def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
    logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)
    def configList = parseJson(out.trim())
    assert configList instanceof List

    boolean disableAutoCompaction = true
    for (Object ele in (List) configList) {
        assert ele instanceof List<String>
        if (((List<String>) ele)[0] == "disable_auto_compaction") {
            disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
        }
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`) 
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
        );
    """

    set_be_config.call("enable_vertical_segment_writer", "true")
    set_be_config.call("variant_ratio_of_defaults_as_sparse_column", "0.95")
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
    qt_select_b_1 """ SELECT count(cast(v['b'] as string)) FROM test_t"""
    qt_select_b_2 """ SELECT count(cast(v['b'] as int)) FROM test_t"""
    // TODO, sparse columns with v['b'] will not be merged in hierachical_data_reader with sparse columns
    // qt_select_b_2 """ select v['b'] from test_t where  cast(v['b'] as string) != '42005' and  cast(v['b'] as string) != '42004' and  cast(v['b'] as string) != '42003' order by cast(v['b'] as string); """

    qt_select_1_bfcompact """select v['b'] from test_t where k = 0 and cast(v['a'] as int) = 11245;"""

    //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
    def tablets = sql_return_maparray """ show tablets from ${tableName}; """

    // trigger compactions for all tablets in ${tableName}
    for (def tablet in tablets) {
        String tablet_id = tablet.TabletId
        backend_id = tablet.BackendId 
        (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def compactJson = parseJson(out.trim())
        if (compactJson.status.toLowerCase() == "fail") {
            assertEquals(disableAutoCompaction, false)
            logger.info("Compaction was done automatically!")
        }
        if (disableAutoCompaction) {
            assertEquals("success", compactJson.status.toLowerCase())
        }
    }

    // wait for all compactions done
    for (def tablet in tablets) {
        boolean running = true
        do {
            Thread.sleep(1000)
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId 
            (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        } while (running)
    }

    int rowCount = 0
    for (def tablet in tablets) {
        String tablet_id = tablet.TabletId
        (code, out, err) = curl("GET", tablet.CompactionStatus)
        logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        for (String rowset in (List<String>) tabletJson.rowsets) {
            rowCount += Integer.parseInt(rowset.split(" ")[1])
        }
    }
    assert (rowCount <= 8)
    // fix cast to string tobe {}
    qt_select_b_3 """ SELECT count(cast(v['b'] as string)) FROM test_t"""
    qt_select_b_4 """ SELECT count(cast(v['b'] as int)) FROM test_t"""
    // TODO, sparse columns with v['b'] will not be merged in hierachical_data_reader with sparse columns
    // qt_select_b_5 """ select v['b'] from test_t where  cast(v['b'] as string) != '42005' and  cast(v['b'] as string) != '42004' and  cast(v['b'] as string) != '42003' order by cast(v['b'] as string); """

    qt_select_1 """select v['b'] from test_t where k = 0 and cast(v['a'] as int) = 11245;"""
    set_be_config.call("variant_ratio_of_defaults_as_sparse_column", "1")
}
