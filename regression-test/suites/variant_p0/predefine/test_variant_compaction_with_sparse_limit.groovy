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
    def enableVariantV2 = true
    def variantV2Function = "parse_to_variant"
    // Nested arrays are intentionally unsupported by Variant V2.

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    sql """ set default_variant_enable_doc_mode = false """
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

        int max_sparse_column_statistics_size = 2
        test {
            sql """ set default_variant_max_sparse_column_statistics_size = 0 """
            exception "variant max sparse column statistics size"
        }
        sql "DROP TABLE IF EXISTS variant_sparse_stats_zero"
        test {
            sql """
                CREATE TABLE variant_sparse_stats_zero (
                    k bigint,
                    v variant <properties("variant_max_sparse_column_statistics_size" = "0")>
                )
                DUPLICATE KEY(`k`)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                properties("replication_num" = "1");
            """
            exception "variant_max_sparse_column_statistics_size must between 1 and 50000"
        }
        def create_table = { tableName, buckets="auto", key_type="DUPLICATE", max_subcolumns_count=2048 ->
            sql "DROP TABLE IF EXISTS ${tableName}"
            def var_def = "variant <MATCH_NAME 'sala' : int, MATCH_NAME 'ddd' : double, MATCH_NAME 'z' : double, properties(\"variant_max_sparse_column_statistics_size\" = \"${max_sparse_column_statistics_size}\")>"
            if (key_type == "AGGREGATE") {
                var_def = "variant <MATCH_NAME 'sala' : int, MATCH_NAME 'ddd' : double, MATCH_NAME 'z' : double, properties(\"variant_max_sparse_column_statistics_size\" = \"${max_sparse_column_statistics_size}\")> replace"
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
            def create_tbl_res = sql """ show create table ${tableName} """
            logger.info("${create_tbl_res}")
            assertTrue(create_tbl_res.toString().contains("variant_max_sparse_column_statistics_size"))
            assertTrue(create_tbl_res.toString().contains("\"variant_max_subcolumns_count\" = \"${max_subcolumns_count}\""))
        }
        def key_types = ["DUPLICATE", "UNIQUE", "AGGREGATE"]
        // def key_types = ["AGGREGATE"]
        for (int i = 0; i < key_types.size(); i++) {
            def max_subcolumns_count = key_types[i] == "AGGREGATE" ? 2048 : 1
            sql """ set default_variant_max_subcolumns_count = ${max_subcolumns_count} """
            def tableName = "simple_variant_${key_types[i]}"
            // 1. simple cases
            create_table.call(tableName, "1", key_types[i], max_subcolumns_count)
            def insert1 = {
                sql """insert into ${tableName} values (1,  ${variantV2Function}('{"x" : [1]}')),(13,  ${variantV2Function}('{"a" : 1}'));"""
                sql """insert into ${tableName} values (2,  ${variantV2Function}('{"a" : "1"}')),(14,  ${variantV2Function}('{"a" : [[[1]]]}'));"""
                sql """insert into ${tableName} values (3,  ${variantV2Function}('{"x" : [3]}')),(15,  ${variantV2Function}('{"a" : 1}'))"""
                sql """insert into ${tableName} values (4,  ${variantV2Function}('{"y": 1}')),(16,  ${variantV2Function}('{"a" : "1223"}'));"""
                sql """insert into ${tableName} values (5,  ${variantV2Function}('{"z" : 2.0}')),(17,  ${variantV2Function}('{"a" : [1]}'));"""
                sql """insert into ${tableName} values (6,  ${variantV2Function}('{"x" : 111}')),(18,  ${variantV2Function}('{"a" : ["1",2,1.1]}'));"""
                sql """insert into ${tableName} values (7,  ${variantV2Function}('{"m" : 1}')),(19,  ${variantV2Function}('{"a" : 1, "b" : {"c" : 1}}'));"""
                sql """insert into ${tableName} values (8,  ${variantV2Function}('{"l" : 2}')),(20,  ${variantV2Function}('{"a" : 1, "b" : {"c" : [{"a" : 1}]}}'));"""
                sql """insert into ${tableName} values (9,  ${variantV2Function}('{"g" : 1.11}')),(21,  ${variantV2Function}('{"a" : 1, "b" : {"c" : [{"a" : 1}]}}'));"""
                sql """insert into ${tableName} values (10, ${variantV2Function}('{"z" : 1.1111}')),(22,  ${variantV2Function}('{"a" : 1, "b" : {"c" : [{"a" : 1}]}}'));"""
                sql """insert into ${tableName} values (11, ${variantV2Function}('{"sala" : 0}')),(1999,  ${variantV2Function}('{"a" : 1, "b" : {"c" : 1}}')),(19921,  ${variantV2Function}('{"a" : 1, "b" : 10}'));"""
                sql """insert into ${tableName} values (12, ${variantV2Function}('{"dddd" : 0.1}')),(1022,  ${variantV2Function}('{"a" : 1, "b" : 10}')),(1029,  ${variantV2Function}('{"a" : 1, "b" : {"c" : 1}}'));"""
            }
            insert1.call();
            insert1.call();
            if (!enableVariantV2) {
                qt_sql_1 "SELECT * FROM ${tableName} ORDER BY k, cast(v as string); "
                qt_sql_2 "select k, cast(v['a'] as array<int>) from ${tableName} where size(cast(v['a'] as array<int>)) > 0 order by k"
            }
            qt_sql_1_supported "SELECT k, sort_json_object_keys(cast(v as json)) FROM ${tableName} ORDER BY k, 2; "
            qt_sql_2_supported "select k, cast(v['a'] as array<int>) from ${tableName} where size(array_filter(x -> x is not null, cast(v['a'] as array<int>))) > 0 order by k"
            qt_sql_3 "select k, v['a'], cast(v['b'] as string) from  ${tableName} where  length(cast(v['b'] as string)) > 4 order  by k"
            qt_sql_5 "select cast(v['b'] as string), cast(v['b']['c'] as string) from  ${tableName} where cast(v['b'] as string) != 'null' and cast(v['b'] as string) != '{}' order by k desc, 1, 2 limit 10;"


            //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,QueryHits,PathHash,MetaUrl,CompactionStatus
            def tablets = sql_return_maparray """ show tablets from ${tableName}; """

            // trigger compactions for all tablets in ${tableName}
            trigger_and_wait_compaction(tableName, "cumulative", 1800)

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
            if (!enableVariantV2) {
                qt_sql_11 "SELECT * FROM ${tableName} ORDER BY k, cast(v as string); "
                qt_sql_22 "select k, cast(v['a'] as array<int>) from ${tableName} where size(cast(v['a'] as array<int>)) > 0 order by k"
            }
            qt_sql_11_supported "SELECT k, sort_json_object_keys(cast(v as json)) FROM ${tableName} ORDER BY k, 2; "
            qt_sql_22_supported "select k, cast(v['a'] as array<int>) from ${tableName} where size(array_filter(x -> x is not null, cast(v['a'] as array<int>))) > 0 order by k"
            qt_sql_33 "select k, v['a'], cast(v['b'] as string) from  ${tableName} where  length(cast(v['b'] as string)) > 4 order  by k"
            qt_sql_55 "select cast(v['b'] as string), cast(v['b']['c'] as string) from  ${tableName} where cast(v['b'] as string) != 'null' and cast(v['b'] as string) != '{}' order by k desc limit 10;"
        }
        for (int i = 0; i < key_types.size(); i++) {
            def tableName = "simple_variant_${key_types[i]}"
            def insert2 = {
                sql """insert into ${tableName} values (1, ${variantV2Function}('{"sala" : 0.1, "ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}')),(1022,  ${variantV2Function}('{"ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}')),(1029,  ${variantV2Function}('{"a" : 1, "b" : {"c" : 1}}'));"""
                sql """insert into ${tableName} values (2, ${variantV2Function}('{"sala" : 0.1, "ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}')),(1022,  ${variantV2Function}('{"ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}')),(1029,  ${variantV2Function}('{"a" : 1, "b" : {"c" : 1}}'));"""
                sql """insert into ${tableName} values (3, ${variantV2Function}('{"sala" : 0.1, "ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}')),(1022,  ${variantV2Function}('{"ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}')),(1029,  ${variantV2Function}('{"a" : 1, "b" : {"c" : 1}}'));"""
                sql """insert into ${tableName} values (4, ${variantV2Function}('{"sala" : 0.1, "ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}')),(1022,  ${variantV2Function}('{"ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}')),(1029,  ${variantV2Function}('{"a" : 1, "b" : {"c" : 1}}'));"""
                sql """insert into ${tableName} values (5, ${variantV2Function}('{"sala" : 0.1, "ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}')),(1022,  ${variantV2Function}('{"ddd" : 1, "z" : 10, "a" : 1, "b" : {"c" : 1}}')),(1029,  ${variantV2Function}('{"a" : 1, "b" : {"c" : 1}}'));"""
            }
            insert2.call();
            insert2.call();
            trigger_and_wait_compaction(tableName, "cumulative", 1800)
            sql "set topn_opt_limit_threshold = 1"
            order_qt_select "select * from ${tableName} order by k, cast(v as string) limit 5;"
            sql "set topn_opt_limit_threshold = 10"
            order_qt_select "select * from ${tableName} order by k, cast(v as string) limit 5;"
        }
    } finally {
        // sql "DROP TABLE IF EXISTS simple_variant_DUPLICATE"
        // sql "DROP TABLE IF EXISTS simple_variant_UNIQUE"
        // sql "DROP TABLE IF EXISTS simple_variant_AGGREGATE"
    }
}
