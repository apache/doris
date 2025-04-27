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
import org.awaitility.Awaitility

suite("test_compaction_sparse_column", "p1,nonConcurrent") {
    def tableName = "test_compaction"
    String backend_id;
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

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

    def set_be_config = { key, value ->
        (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
        logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
    }

    try {
        set_be_config.call("write_buffer_size", "10240")

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
                 "disable_auto_compaction" = "true",
                 "variant_max_subcolumns_count" = "3"
            );
        """

        def triger_compaction = { ->
             //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
            def tablets = sql_return_maparray """ show tablets from ${tableName}; """

            // trigger compactions for all tablets in ${tableName}
            for (def tablet in tablets) {
                String tablet_id = tablet.TabletId
                backend_id = tablet.BackendId
                (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
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
                Awaitility.await().untilAsserted(() -> {
                    String tablet_id = tablet.TabletId
                    backend_id = tablet.BackendId
                    (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                    logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                    assertEquals(code, 0)
                    def compactionStatus = parseJson(out.trim())
                    assertEquals("success", compactionStatus.status.toLowerCase())
                    return compactionStatus.run_status;
                });
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
            assert (rowCount <= 7)
        }

        // b is sparse
        // a is dense
        sql """insert into ${tableName} select 0, '{"a": 11245, "b" : 42000}'  as json_str
            union  all select 0, '{"a": 1123}' as json_str union all select 0, '{"a" : 1234, "xxxx" : "aaaaa"}' as json_str from numbers("number" = "4096") limit 4096 ;"""

        // b is sparse 
        // a, xxxx is dense
        sql """insert into ${tableName} select 1, '{"a": 11245, "b" : 42001}'  as json_str
            union  all select 1, '{"a": 1123}' as json_str union all select 1, '{"a" : 1234, "xxxx" : "bbbbb"}' as json_str from numbers("number" = "4096") limit 4096 ;"""

        // b is sparse        
        // xxxx is dense
        sql """insert into ${tableName} select 2, '{"a": 11245, "b" : 42002}'  as json_str
            union  all select 2, '{"a": 1123}' as json_str union all select 2, '{"a" : 1234, "xxxx" : "ccccc"}' as json_str from numbers("number" = "4096") limit 4096 ;"""

        // point, xxxx is sparse        
        // a, b is dense
        sql """insert into ${tableName} select 3, '{"a" : 1234, "point" : 1, "xxxx" : "ddddd"}'  as json_str
            union  all select 3, '{"a": 1123}' as json_str union all select 3, '{"a": 11245, "b" : 42003}' as json_str from numbers("number" = "4096") limit 4096 ;"""


        // xxxx, eeeee is sparse
        // a, b is dense
        sql """insert into ${tableName} select 4, '{"a" : 1234, "xxxx" : "eeeee", "point" : 5}'  as json_str
            union  all select 4, '{"a": 1123}' as json_str union all select 4, '{"a": 11245, "b" : 42004}' as json_str from numbers("number" = "4096") limit 4096 ;"""
        
        //   xxxx, point is sparse
        // a, b is dense
        sql """insert into ${tableName} select 5, '{"a" : 1234, "xxxx" : "fffff", "point" : 42000}'  as json_str
            union  all select 5, '{"a": 1123}' as json_str union all select 5, '{"a": 11245, "b" : 42005}' as json_str from numbers("number" = "4096") limit 4096 ;"""

        sql """insert into ${tableName} values (6, '{"b" : "789"}')"""
        
        qt_select_b_bfcompact """ SELECT count(cast(v['b'] as string)) FROM ${tableName};"""
        qt_select_xxxx_bfcompact """ SELECT count(cast(v['xxxx'] as string)) FROM ${tableName};"""
        qt_select_point_bfcompact """ SELECT count(cast(v['point'] as bigint)) FROM ${tableName};"""
        qt_select_1_bfcompact """ SELECT count(cast(v['xxxx'] as string)) FROM ${tableName} where cast(v['xxxx'] as string) = 'aaaaa';"""
        qt_select_2_bfcompact """ SELECT count(cast(v['xxxx'] as string)) FROM ${tableName} where cast(v['xxxx'] as string) = 'bbbbb';"""
        qt_select_3_bfcompact """ SELECT count(cast(v['xxxx'] as string)) FROM ${tableName} where cast(v['xxxx'] as string) = 'ccccc';"""
        qt_select_4_bfcompact """ SELECT count(cast(v['xxxx'] as string)) FROM ${tableName} where cast(v['xxxx'] as string) = 'eeeee';"""
        qt_select_5_bfcompact """ SELECT count(cast(v['xxxx'] as string)) FROM ${tableName} where cast(v['xxxx'] as string) = 'ddddd';"""
        qt_select_6_bfcompact """ SELECT count(cast(v['xxxx'] as string)) FROM ${tableName} where cast(v['xxxx'] as string) = 'fffff';"""
        qt_select_1_1_bfcompact """ SELECT count(cast(v['b'] as int)) FROM ${tableName} where cast(v['b'] as int) = 42000;"""
        qt_select_2_1_bfcompact """ SELECT count(cast(v['b'] as int)) FROM ${tableName} where cast(v['b'] as int) = 42001;"""
        qt_select_3_1_bfcompact """ SELECT count(cast(v['b'] as int)) FROM ${tableName} where cast(v['b'] as int) = 42002;"""
        qt_select_4_1_bfcompact """ SELECT count(cast(v['b'] as int)) FROM ${tableName} where cast(v['b'] as int) = 42003;"""
        qt_select_5_1_bfcompact """ SELECT count(cast(v['b'] as int)) FROM ${tableName} where cast(v['b'] as int) = 42004;"""
        qt_select_6_1_bfcompact """ SELECT count(cast(v['b'] as int)) FROM ${tableName} where cast(v['b'] as int) = 42005;"""
        qt_select_all_bfcompact """SELECT k, v['a'], v['b'], v['xxxx'], v['point'], v['ddddd'] from ${tableName} where (cast(v['point'] as int) = 1);"""
        
        GetDebugPoint().enableDebugPointForAllBEs("variant_column_writer_impl._get_subcolumn_paths_from_stats", [stats: "24588,12292,12291,3",subcolumns:"a,b,xxxx"])
        triger_compaction.call()
        /**
            variant_statistics {
            subcolumn_non_null_size {
              key: "a"
              value: 24588
            }
            subcolumn_non_null_size {
              key: "b"
              value: 12292
            }
            subcolumn_non_null_size {
              key: "point"
              value: 3
            }
            subcolumn_non_null_size {
              key: "xxxx"
              value: 12291
            }
        */

        qt_select_b """ SELECT count(cast(v['b'] as string)) FROM ${tableName};"""
        qt_select_xxxx """ SELECT count(cast(v['xxxx'] as string)) FROM ${tableName};"""
        qt_select_point """ SELECT count(cast(v['point'] as bigint)) FROM ${tableName};"""
        qt_select_1 """ SELECT count(cast(v['xxxx'] as string)) FROM ${tableName} where cast(v['xxxx'] as string) = 'aaaaa';"""
        qt_select_2 """ SELECT count(cast(v['xxxx'] as string)) FROM ${tableName} where cast(v['xxxx'] as string) = 'bbbbb';"""
        qt_select_3 """ SELECT count(cast(v['xxxx'] as string)) FROM ${tableName} where cast(v['xxxx'] as string) = 'ccccc';"""
        qt_select_4 """ SELECT count(cast(v['xxxx'] as string)) FROM ${tableName} where cast(v['xxxx'] as string) = 'eeeee';"""
        qt_select_5 """ SELECT count(cast(v['xxxx'] as string)) FROM ${tableName} where cast(v['xxxx'] as string) = 'ddddd';"""
        qt_select_6 """ SELECT count(cast(v['xxxx'] as string)) FROM ${tableName} where cast(v['xxxx'] as string) = 'fffff';"""
        qt_select_1_1 """ SELECT count(cast(v['b'] as int)) FROM ${tableName} where cast(v['b'] as int) = 42000;"""
        qt_select_2_1 """ SELECT count(cast(v['b'] as int)) FROM ${tableName} where cast(v['b'] as int) = 42001;"""
        qt_select_3_1 """ SELECT count(cast(v['b'] as int)) FROM ${tableName} where cast(v['b'] as int) = 42002;"""
        qt_select_4_1 """ SELECT count(cast(v['b'] as int)) FROM ${tableName} where cast(v['b'] as int) = 42003;"""
        qt_select_5_1 """ SELECT count(cast(v['b'] as int)) FROM ${tableName} where cast(v['b'] as int) = 42004;"""
        qt_select_6_1 """ SELECT count(cast(v['b'] as int)) FROM ${tableName} where cast(v['b'] as int) = 42005;"""
        qt_select_all """SELECT k, v['a'], v['b'], v['xxxx'], v['point'], v['ddddd'] from ${tableName} where (cast(v['point'] as int) = 1);"""

        sql "truncate table ${tableName}"
        sql """insert into ${tableName} values (1, '{"1" : 1, "2" : 2, "3" : 3, "4": "4", "a" : 1, "aa":2, "aaa" : 3,"aaaaaa":1234, ".a." : 1}')"""
        sql """insert into ${tableName} values (1, '{"1" : 1, "2" : 2, "3" : 3, "4": "4", "a" : 1, "aa":2, "aaa" : 3,"aaaaaa":1234, ".a." : 1}')"""
        sql """insert into ${tableName} values (1, '{"1" : 1, "2" : 2, "3" : 3, "4": "4", "a" : 1, "aa":2, "aaa" : 3,"aaaaaa":1234, ".a." : 1}')"""
        sql """insert into ${tableName} values (1, '{"1" : 1, "2" : 2, "3" : 3, "4": "4", "a" : 1, "aa":2, "aaa" : 3,"aaaaaa":1234, ".a." : 1}')"""
        qt_sql """select v['aaaa'] from ${tableName}"""
        qt_sql """select v['aaa'] from ${tableName}"""
        qt_sql """select v['aa'] from ${tableName}"""
        qt_sql """select v['1'] from ${tableName}"""
    } finally {
        // try_sql("DROP TABLE IF EXISTS ${tableName}")
        GetDebugPoint().disableDebugPointForAllBEs("variant_column_writer_impl._get_subcolumn_paths_from_stats")
        set_be_config.call("write_buffer_size", "209715200")
        // set_be_config.call("variant_max_subcolumns_count", "5")
    }
}
