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

suite("test_index_compaction_failure_injection", "nonConcurrent") {
    def isCloudMode = isCloudMode()
    def tableName = "test_index_compaction_failure_injection_dups"
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
        sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 99); """
        sql """ INSERT INTO ${tableName} VALUES (2, "andy", "andy love apple", 100); """
        sql """ INSERT INTO ${tableName} VALUES (2, "bason", "bason hate pear", 99); """
        sql """ INSERT INTO ${tableName} VALUES (3, "andy", "andy love apple", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "bason", "bason hate pear", 99); """
    }

    def run_sql = { -> 
        qt_sql """ select * from ${tableName} order by id, name, hobbies, score """
        qt_sql """ select * from ${tableName} where name match "andy" order by id, name, hobbies, score """
        qt_sql """ select * from ${tableName} where hobbies match "pear" order by id, name, hobbies, score """
        qt_sql """ select * from ${tableName} where score < 100 order by id, name, hobbies, score """
    }

    def run_test = { tablets ->
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

        // before full compaction, there are 7 rowsets.
        int rowsetCount = get_rowset_count.call(tablets);
        assert (rowsetCount == 7 * replicaNum)

        // tigger full compaction for all tablets with fault injection
        try {
            GetDebugPoint().enableDebugPointForAllBEs("index_compaction_compact_column_throw_error")
            logger.info("trigger_full_compaction_on_tablets with fault injection: index_compaction_compact_column_throw_error")
            trigger_full_compaction_on_tablets.call(tablets)
            wait_full_compaction_done.call(tablets)
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("index_compaction_compact_column_throw_error")
        }
        // after fault injection, there are still 7 rowsets.
        rowsetCount = get_rowset_count.call(tablets);
        assert (rowsetCount == 7 * replicaNum)

        logger.info("trigger_full_compaction_on_tablets normally")
        // trigger full compactions for all tablets in ${tableName}
        // this time, index compaction of some columns will be skipped because of the fault injection
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

        // insert 6 rows, so there are 7 rowsets.
        rowsetCount = get_rowset_count.call(tablets);
        if (isCloudMode) {
            assert (rowsetCount == (7 + 1) * replicaNum)
        } else {
            assert (rowsetCount == 7 * replicaNum)
        }

        // tigger full compaction for all tablets with fault injection
        try {
            GetDebugPoint().enableDebugPointForAllBEs("index_compaction_compact_column_status_not_ok")
            logger.info("trigger_full_compaction_on_tablets with fault injection: index_compaction_compact_column_status_not_ok")
            trigger_full_compaction_on_tablets.call(tablets)
            wait_full_compaction_done.call(tablets)
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("index_compaction_compact_column_status_not_ok")
        }

        // insert more data
        insert_data.call()

        sql """ select * from ${tableName} """

        // after fault injection, there are still 7 rowsets.
        // and we insert 6 rows, so there are 13 rowsets.
        rowsetCount = get_rowset_count.call(tablets);
        if (isCloudMode) {
            assert (rowsetCount == (13 + 1) * replicaNum)
        } else {
            assert (rowsetCount == 13 * replicaNum)
        }

        logger.info("trigger_full_compaction_on_tablets normally")
        // trigger full compactions for all tablets in ${tableName}
        // this time, index compaction of some columns will be skipped because of the fault injection
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
        
        sql """ select * from ${tableName} """

        // insert 6 rows, so there are 7 rowsets.
        rowsetCount = get_rowset_count.call(tablets);
        if (isCloudMode) {
            assert (rowsetCount == (7 + 1) * replicaNum)
        } else {
            assert (rowsetCount == 7 * replicaNum)
        }
        // tigger full compaction for all tablets normally
        // this time, index compaction will be done successfully
        logger.info("trigger_full_compaction_on_tablets normally")
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
        sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NULL,
                `name` varchar(255) NULL,
                `hobbies` text NULL,
                `score` int(11) NULL,
                index index_name (name) using inverted,
                index index_hobbies (hobbies) using inverted properties("parser"="english"),
                index index_score (score) using inverted
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ( "replication_num" = "1", "disable_auto_compaction" = "true", "inverted_index_storage_format" = "V1");
        """

        //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """

        run_test.call(tablets)

        /**
        * test for unique key table
        */
        tableName = "test_index_compaction_failure_injection_unique"

        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NULL,
                `name` varchar(255) NULL,
                `hobbies` text NULL,
                `score` int(11) NULL,
                index index_name (name) using inverted,
                index index_hobbies (hobbies) using inverted properties("parser"="english"),
                index index_score (score) using inverted
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ( 
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "inverted_index_storage_format" = "V1"
            );
        """

        tablets = sql_return_maparray """ show tablets from ${tableName}; """
        run_test.call(tablets)

    } finally {
        if (has_update_be_config) {
            set_be_config.call("inverted_index_compaction_enable", invertedIndexCompactionEnable.toString())
        }
    }
}
