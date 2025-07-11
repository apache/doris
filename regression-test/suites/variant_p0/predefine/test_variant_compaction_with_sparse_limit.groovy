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

suite("test_compaction_variant_predefine_with_sparse_limit", "nonConcurrent") {
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def set_be_config = { key, value ->
    for (String backend_id: backendId_to_backendIP.keySet()) {
        def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }
    try {
        String backend_id = backendId_to_backendIP.keySet()[0]
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

        set_be_config("variant_max_sparse_column_statistics_size", "2")
        def create_table = { tableName, buckets="auto", key_type="DUPLICATE" ->
            sql "DROP TABLE IF EXISTS ${tableName}"
            def var_def = "variant <'sala' : int, 'ddd' : double, 'z' : double>"
            if (key_type == "AGGREGATE") {
                var_def = "variant <'sala' : int, 'ddd' : double, 'z' : double> replace"
            }
            sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    k bigint,
                    v ${var_def}
                )
                ${key_type} KEY(`k`)
                DISTRIBUTED BY HASH(k) BUCKETS ${buckets}
                properties("replication_num" = "1", "disable_auto_compaction" = "true");
            """
        }
        def key_types = ["DUPLICATE", "UNIQUE", "AGGREGATE"]
        // def key_types = ["AGGREGATE"]
        for (int i = 0; i < key_types.size(); i++) {
            def tableName = "simple_variant_${key_types[i]}"
            // 1. simple cases
            create_table.call(tableName, "1", key_types[i])
            def insert1 = {
                sql """insert into ${tableName} values (1,  '{"x" : [1]}'),(13,  '{"a" : 1}');"""
                sql """insert into ${tableName} values (2,  '{"a" : "1"}'),(14,  '{"a" : [[[1]]]}');"""
                sql """insert into ${tableName} values (3,  '{"x" : [3]}'),(15,  '{"a" : 1}')"""
                sql """insert into ${tableName} values (4,  '{"y": 1}'),(16,  '{"a" : "1223"}');"""
                sql """insert into ${tableName} values (5,  '{"z" : 2.0}'),(17,  '{"a" : [1]}');"""
                sql """insert into ${tableName} values (6,  '{"x" : 111}'),(18,  '{"a" : ["1", 2, 1.1]}');"""
                sql """insert into ${tableName} values (7,  '{"m" : 1}'),(19,  '{"a" : 1, "b" : {"c" : 1}}');"""
                sql """insert into ${tableName} values (8,  '{"l" : 2}'),(20,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
                sql """insert into ${tableName} values (9,  '{"g" : 1.11}'),(21,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
                sql """insert into ${tableName} values (10, '{"z" : 1.1111}'),(22,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}');"""
                sql """insert into ${tableName} values (11, '{"sala" : 0}'),(1999,  '{"a" : 1, "b" : {"c" : 1}}'),(19921,  '{"a" : 1, "b" : 10}');"""
                sql """insert into ${tableName} values (12, '{"dddd" : 0.1}'),(1022,  '{"a" : 1, "b" : 10}'),(1029,  '{"a" : 1, "b" : {"c" : 1}}');"""
            }
            insert1.call();
            insert1.call();
            qt_sql_1 "SELECT * FROM ${tableName} ORDER BY k, cast(v as string); "
            qt_sql_2 "select k, cast(v['a'] as array<int>) from  ${tableName} where  size(cast(v['a'] as array<int>)) > 0 order by k"
            qt_sql_3 "select k, v['a'], cast(v['b'] as string) from  ${tableName} where  length(cast(v['b'] as string)) > 4 order  by k"
            qt_sql_5 "select cast(v['b'] as string), cast(v['b']['c'] as string) from  ${tableName} where cast(v['b'] as string) != 'null' and cast(v['b'] as string) != '{}' order by k desc, 1, 2 limit 10;"


            //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,QueryHits,PathHash,MetaUrl,CompactionStatus
            def tablets = sql_return_maparray """ show tablets from ${tableName}; """

            // trigger compactions for all tablets in ${tableName}
            trigger_and_wait_compaction(tableName, "cumulative")

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
            // assert (rowCount < 8)
            qt_sql_11 "SELECT * FROM ${tableName} ORDER BY k, cast(v as string); "
            qt_sql_22 "select k, cast(v['a'] as array<int>) from  ${tableName} where  size(cast(v['a'] as array<int>)) > 0 order by k"
            qt_sql_33 "select k, v['a'], cast(v['b'] as string) from  ${tableName} where  length(cast(v['b'] as string)) > 4 order  by k"
            qt_sql_55 "select cast(v['b'] as string), cast(v['b']['c'] as string) from  ${tableName} where cast(v['b'] as string) != 'null' and cast(v['b'] as string) != '{}' order by k desc limit 10;"
        }
        for (int i = 0; i < key_types.size(); i++) {
            def tableName = "simple_variant_${key_types[i]}"
            def insert2 = {
                sql """insert into ${tableName} values (1, '{"sala" : 0.1, "ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}'),(1022,  '{"ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}'),(1029,  '{"a" : 1, "b" : {"c" : 1}}');"""
                sql """insert into ${tableName} values (2, '{"sala" : 0.1, "ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}'),(1022,  '{"ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}'),(1029,  '{"a" : 1, "b" : {"c" : 1}}');"""
                sql """insert into ${tableName} values (3, '{"sala" : 0.1, "ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}'),(1022,  '{"ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}'),(1029,  '{"a" : 1, "b" : {"c" : 1}}');"""
                sql """insert into ${tableName} values (4, '{"sala" : 0.1, "ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}'),(1022,  '{"ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}'),(1029,  '{"a" : 1, "b" : {"c" : 1}}');"""
                sql """insert into ${tableName} values (5, '{"sala" : 0.1, "ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}'),(1022,  '{"ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}'),(1029,  '{"a" : 1, "b" : {"c" : 1}}');"""
            }
            insert2.call();
            insert2.call();
            trigger_and_wait_compaction(tableName, "cumulative")
            sql "set topn_opt_limit_threshold = 1"
            order_qt_select "select * from ${tableName} order by k, cast(v as string) limit 5;"
            sql "set topn_opt_limit_threshold = 10"
            order_qt_select "select * from ${tableName} order by k, cast(v as string) limit 5;"
        }
    } finally {
        // set back to default
        set_be_config("variant_max_sparse_column_statistics_size", "10000")
    }
}
