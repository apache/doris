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

suite("test_index_compaction_null_arr", "array_contains_inverted_index") {
    // here some variable to control inverted index query
    sql """ set enable_profile=true"""
    sql """ set enable_pipeline_x_engine=true;"""
    sql """ set enable_inverted_index_query=true"""
    sql """ set enable_common_expr_pushdown=true """
    sql """ set enable_common_expr_pushdown_for_inverted_index=true """

    def isCloudMode = isCloudMode()
    def tableName = "test_index_compaction_null_dups_arr"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    boolean disableAutoCompaction = false
  
    def set_be_config = { key, value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    def trigger_full_compaction_on_tablets = { tablets ->
        for (def tablet : tablets) {
            String tablet_id = tablet.TabletId
            String backend_id = tablet.BackendId
            int times = 1

            String compactionStatus;
            do{
                def (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                ++times
                sleep(2000)
                compactionStatus = parseJson(out.trim()).status.toLowerCase();
            } while (compactionStatus!="success" && times<=10 && compactionStatus!="e-6010")


            if (compactionStatus == "fail") {
                assertEquals(disableAutoCompaction, false)
                logger.info("Compaction was done automatically!")
            }
            if (disableAutoCompaction && compactionStatus!="e-6010") {
                assertEquals("success", compactionStatus)
            }
        }
    }

    def wait_full_compaction_done = { tablets ->
        for (def tablet in tablets) {
            boolean running = true
            do {
                Thread.sleep(1000)
                String tablet_id = tablet.TabletId
                String backend_id = tablet.BackendId
                def (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)
        }
    }

    def get_rowset_count = { tablets ->
        int rowsetCount = 0
        for (def tablet in tablets) {
            def (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
        }
        return rowsetCount
    }

    def check_config = { String key, String value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
            logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def configList = parseJson(out.trim())
            assert configList instanceof List
            for (Object ele in (List) configList) {
                assert ele instanceof List<String>
                if (((List<String>) ele)[0] == key) {
                    assertEquals(value, ((List<String>) ele)[2])
                }
            }
        }
    }

    def insert_data = { -> 
        sql """insert into ${tableName} values
            (1,null,['addr qie3'],'yy','lj',[100]),
            (2,null,['hehe'],null,'lala',[200]),
            (3,['beijing'],['addr xuanwu'],'wugui',null,[300]),
            (4,['beijing'],['addr fengtai'],'fengtai1','fengtai2',null),
            (5,['beijing'],['addr chaoyang'],'wangjing','donghuqu',[500]),
            (6,['shanghai'],[null],null,'haha',[null]),
            (7,['tengxun'],['qie'],'addr gg','lj',[null]),
            (8,['tengxun2'],['null'],null,'lj',[800])
    """
    }

    def run_sql = { -> 
        // select all data
        qt_select_0 "SELECT * FROM ${tableName} ORDER BY id"

        // test IS NULL , IS NOT NULL
        qt_select_is_null_1 "SELECT * FROM ${tableName} WHERE city IS NULL ORDER BY id"
        qt_select_is_null_2 "SELECT * FROM ${tableName} WHERE city IS NOT NULL ORDER BY id"
        qt_select_is_null_3 "SELECT * FROM ${tableName} WHERE addr IS NULL ORDER BY id"
        qt_select_is_null_4 "SELECT * FROM ${tableName} WHERE addr IS NOT NULL ORDER BY id"
        qt_select_is_null_5 "SELECT * FROM ${tableName} WHERE n IS NULL ORDER BY id"
        qt_select_is_null_6 "SELECT * FROM ${tableName} WHERE n IS NOT NULL ORDER BY id"

        // test array element IS NULL , IS NOT NULL
        qt_select_is_null_7 "SELECT * FROM ${tableName} WHERE city[1] IS NULL ORDER BY id"
        qt_select_is_null_8 "SELECT * FROM ${tableName} WHERE city[1] IS NOT NULL ORDER BY id"
        qt_select_is_null_9 "SELECT * FROM ${tableName} WHERE addr[1] IS NULL ORDER BY id"
        qt_select_is_null_10 "SELECT * FROM ${tableName} WHERE addr[1] IS NOT NULL ORDER BY id"
        qt_select_is_null_11 "SELECT * FROM ${tableName} WHERE n[1] IS NULL ORDER BY id"
        qt_select_is_null_12 "SELECT * FROM ${tableName} WHERE n[1] IS NOT NULL ORDER BY id"


        // test compare predicate
        qt_select_compare_11 "SELECT * FROM ${tableName} WHERE array_contains(city, 'shanghai') ORDER BY id"
        qt_select_compare_12 "SELECT * FROM ${tableName} WHERE !array_contains(city, 'shanghai') ORDER BY id"
        qt_select_compare_13 "SELECT * FROM ${tableName} WHERE city[1] <= 'shanghai' ORDER BY id"
        qt_select_compare_14 "SELECT * FROM ${tableName} WHERE city[1] >= 'shanghai' ORDER BY id"

        qt_select_compare_21 "SELECT * FROM ${tableName} WHERE array_contains(n, 500) ORDER BY id"
        qt_select_compare_22 "SELECT * FROM ${tableName} WHERE !array_contains(n, 500) ORDER BY id"

        qt_select_compare_23 "SELECT * FROM ${tableName} WHERE n[1] <= 500 ORDER BY id"
        qt_select_compare_24 "SELECT * FROM ${tableName} WHERE n[1] >= 500 ORDER BY id"

        // test match predicates
        qt_select_match_1 "SELECT * FROM ${tableName} WHERE array_contains(addr, 'addr fengtai') ORDER BY id"
        qt_select_match_2 "SELECT * FROM ${tableName} WHERE array_contains(addr, 'addr') ORDER BY id"
    }

    def run_test = { tablets ->
        insert_data.call()
        insert_data.call()

        run_sql.call()

        int replicaNum = 1
        def dedup_tablets = deduplicate_tablets(tablets)
        if (dedup_tablets.size() > 0) {
            replicaNum = Math.round(tablets.size() / dedup_tablets.size())
            if (replicaNum != 1 && replicaNum != 3) {
                assert(false)
            }
        }

        // before full compaction, there are 3 rowsets.
        int rowsetCount = get_rowset_count.call(tablets);
        assert (rowsetCount == 3 * replicaNum)

        // tigger full compaction for all tablets
        trigger_full_compaction_on_tablets.call(tablets)

        // wait for full compaction done
        wait_full_compaction_done.call(tablets)

        // after full compaction, there is only 1 rowset.
        rowsetCount = get_rowset_count.call(tablets);
        if (isCloudMode) {
            assert (rowsetCount == (1 + 1) * replicaNum)
        } else {
            assert (rowsetCount == 1 * replicaNum)
        }

        run_sql.call()

        // insert more data and trigger full compaction again
        insert_data.call()

        run_sql.call()

        rowsetCount = get_rowset_count.call(tablets);
        if (isCloudMode) {
            assert (rowsetCount == (2 + 1) * replicaNum)
        } else {
            assert (rowsetCount == 2 * replicaNum)
        }

        // tigger full compaction for all tablets
        trigger_full_compaction_on_tablets.call(tablets)

        // wait for full compaction done
        wait_full_compaction_done.call(tablets)

        // after full compaction, there is only 1 rowset.
        rowsetCount = get_rowset_count.call(tablets);
        if (isCloudMode) {
            assert (rowsetCount == (1 + 1) * replicaNum)
        } else {
            assert (rowsetCount == 1 * replicaNum)
        }

        run_sql.call()
    }

    boolean invertedIndexCompactionEnable = false
    boolean has_update_be_config = false
    try {
        String backend_id;
        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
        
        logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def configList = parseJson(out.trim())
        assert configList instanceof List

        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == "inverted_index_compaction_enable") {
                invertedIndexCompactionEnable = Boolean.parseBoolean(((List<String>) ele)[2])
                logger.info("inverted_index_compaction_enable: ${((List<String>) ele)[2]}")
            }
            if (((List<String>) ele)[0] == "disable_auto_compaction") {
                disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
                logger.info("disable_auto_compaction: ${((List<String>) ele)[2]}")
            }
        }
        set_be_config.call("inverted_index_compaction_enable", "true")
        has_update_be_config = true
        // check updated config
        check_config.call("inverted_index_compaction_enable", "true");


        /**
        * test for duplicated key table
        */
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql  """
            CREATE TABLE IF NOT EXISTS `${tableName}` (
                `id` int NULL COMMENT "",
                `city` array<varchar(20)> NULL COMMENT "[]",
                `addr` array<varchar(20)> NULL COMMENT "[]",
                `name` varchar(20) NULL COMMENT "",
                `compy` varchar(20) NULL COMMENT "",
                `n` array<int> NULL COMMENT "[]",
                INDEX idx_city(city) USING INVERTED,
                INDEX idx_addr(addr) USING INVERTED PROPERTIES("parser"="none"),
                INDEX idx_n(n) USING INVERTED
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "in_memory" = "false",
                "storage_format" = "V2"
            )
            """

        //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """

        run_test.call(tablets)

        /**
        * test for unique key table
        */
        tableName = "test_index_compaction_null_unique_arr"

        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql  """
            CREATE TABLE IF NOT EXISTS `${tableName}` (
                `id` int NULL COMMENT "",
                `city` array<varchar(20)> NULL COMMENT "[]",
                `addr` array<varchar(20)> NULL COMMENT "[]",
                `name` varchar(20) NULL COMMENT "",
                `compy` varchar(20) NULL COMMENT "",
                `n` array<int> NULL COMMENT "[]",
                INDEX idx_city(city) USING INVERTED,
                INDEX idx_addr(addr) USING INVERTED PROPERTIES("parser"="none"),
                INDEX idx_n(n) USING INVERTED
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "in_memory" = "false",
                "storage_format" = "V2"
            )
            """

        tablets = sql_return_maparray """ show tablets from ${tableName}; """
        run_test.call(tablets)

    } finally {
        if (has_update_be_config) {
            set_be_config.call("inverted_index_compaction_enable", invertedIndexCompactionEnable.toString())
        }
    }
}
