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

suite("test_index_compaction_with_multi_index_segments_arr", "nonConcurrent") {
    // here some variable to control inverted index query
    sql """ set enable_profile=true"""
    sql """ set enable_pipeline_x_engine=true;"""
    sql """ set enable_inverted_index_query=true"""
    sql """ set enable_common_expr_pushdown=true """
    sql """ set enable_common_expr_pushdown_for_inverted_index=true """


    def isCloudMode = isCloudMode()
    def tableName = "test_index_compaction_with_multi_index_segments_arr"
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
            } while (compactionStatus!="success" && times<=10)


            if (compactionStatus == "fail") {
                assertEquals(disableAutoCompaction, false)
                logger.info("Compaction was done automatically!")
            }
            if (disableAutoCompaction) {
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

    boolean invertedIndexCompactionEnable = false
    int invertedIndexMaxBufferedDocs = -1;
    boolean has_update_be_config = false

    try {
        String backend_id = backendId_to_backendIP.keySet()[0]
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
            if (((List<String>) ele)[0] == "inverted_index_max_buffered_docs") {
                invertedIndexMaxBufferedDocs = Integer.parseInt(((List<String>) ele)[2])
                logger.info("inverted_index_max_buffered_docs: ${((List<String>) ele)[2]}")
            }
        }
        set_be_config.call("inverted_index_compaction_enable", "true")
        set_be_config.call("inverted_index_max_buffered_docs", "5")
        has_update_be_config = true

        // check config
        check_config.call("inverted_index_compaction_enable", "true")
        check_config.call("inverted_index_max_buffered_docs", "5")

        /**
        * test duplicated tables
        * 1. insert 10 rows
        * 2. insert another 10 rows
        * 3. full compaction
        * 4. insert 10 rows, again
        * 5. full compaction
        */
        table_name = "test_index_compaction_with_multi_index_segments_dups_arr"
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE ${tableName} (
                `file_time` DATETIME NOT NULL,
                `comment_id` array<int(11)>  NULL,
                `body` array<TEXT> NULL DEFAULT "[]",
                INDEX idx_comment_id (`comment_id`) USING INVERTED COMMENT '''',
                INDEX idx_body (`body`) USING INVERTED PROPERTIES("parser" = "none") COMMENT ''''
            ) ENGINE=OLAP
            DUPLICATE KEY(`file_time`)
            COMMENT 'OLAP'
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
            );
        """

        // insert 10 rows
        sql """ INSERT INTO ${tableName} VALUES ("2018-02-21 12:00:00", [1], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [2], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [3], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [4], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [5], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [6], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [7], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [8], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [9], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [10], ["I\'m using the builds"]); """
        // insert another 10 rows
        sql """ INSERT INTO ${tableName} VALUES ("2018-02-21 12:00:00", [1], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [2], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [3], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [4], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [5], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [6], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [7], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [8], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [9], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [10], ["I\'m using the builds"]); """

        qt_sql """ select * from ${tableName} order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where array_contains(body, "using") order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where array_contains(body, "the") order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where comment_id[1]< 8 order by file_time, comment_id[1], body[1] """

        //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        def dedup_tablets = deduplicate_tablets(tablets)

        // In the p0 testing environment, there are no expected operations such as scaling down BE (backend) services
        // if tablets or dedup_tablets is empty, exception is thrown, and case fail
        int replicaNum = Math.floor(tablets.size() / dedup_tablets.size())
        if (replicaNum != 1 && replicaNum != 3)
        {
            assert(false);
        }

        // before full compaction, there are 3 rowsets.
        int rowsetCount = get_rowset_count.call(tablets)
        assert (rowsetCount == 3 * replicaNum)

        // trigger full compactions for all tablets in ${tableName}
        trigger_full_compaction_on_tablets.call(tablets)

        // wait for full compaction done
        wait_full_compaction_done.call(tablets)

        // after full compaction, there is only 1 rowset.
        rowsetCount = get_rowset_count.call(tablets)
        if (isCloudMode) {
            assert (rowsetCount == (1 + 1) * replicaNum)
        } else {
            assert (rowsetCount == 1 * replicaNum)
        }

        qt_sql """ select * from ${tableName} order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where array_contains(body, "using") order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where array_contains(body, "the") order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where comment_id[1]< 8 order by file_time, comment_id[1], body[1] """

        // insert 10 rows, again
        sql """ INSERT INTO ${tableName} VALUES ("2018-02-21 12:00:00", [1], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [2], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [3], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [4], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [5], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [6], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [7], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [8], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [9], ["I\'m using the builds"]),
                                                ("2018-02-21 12:00:00", [10], ["I\'m using the builds"]); """

        sql """ select * from ${tableName} """

        tablets = sql_return_maparray """ show tablets from ${tableName}; """

        // before full compaction, there are 2 rowsets.
        rowsetCount = get_rowset_count.call(tablets)
        if (isCloudMode) {
            assert (rowsetCount == (2 + 1) * replicaNum)
        } else {
            assert (rowsetCount == 2 * replicaNum)
        }
        // trigger full compactions for all tablets in ${tableName}
        trigger_full_compaction_on_tablets.call(tablets)

        // wait for full compaction done
        wait_full_compaction_done.call(tablets)

        // after full compaction, there is only 1 rowset.
        rowsetCount = get_rowset_count.call(tablets)
        if (isCloudMode) {
            assert (rowsetCount == (1 + 1) * replicaNum)
        } else {
            assert (rowsetCount == 1 * replicaNum)
        }

        qt_sql """ select * from ${tableName} order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where array_contains(body, "using") order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where array_contains(body, "the") order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where comment_id[1]< 8 order by file_time, comment_id[1], body[1] """

        /**
        * test unique tables
        * 1. insert 10 rows
        * 2. insert another 10 rows
        * 3. full compaction
        * 4. insert 10 rows, again
        * 5. full compaction
        */
        table_name = "test_index_compaction_with_multi_index_segments_unique_arr"
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE ${tableName} (
                `file_time` DATETIME NOT NULL,
                `comment_id` array<int(11)>  NULL,
                `body` array<TEXT> NULL DEFAULT "[]",
                INDEX idx_comment_id (`comment_id`) USING INVERTED COMMENT '''',
                INDEX idx_body (`body`) USING INVERTED PROPERTIES("parser" = "none") COMMENT ''''
            ) ENGINE=OLAP
            UNIQUE KEY(`file_time`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`file_time`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true",
            "enable_unique_key_merge_on_write" = "true"
            );
        """

        // insert 10 rows
        sql """ INSERT INTO ${tableName} VALUES ("2018-02-21 12:00:00", [1], ["I\'m using the builds"]),
                                                ("2018-02-21 13:00:00", [2], ["I\'m using the builds"]),
                                                ("2018-02-21 14:00:00", [3], ["I\'m using the builds"]),
                                                ("2018-02-21 15:00:00", [4], ["I\'m using the builds"]),
                                                ("2018-02-21 16:00:00", [5], ["I\'m using the builds"]),
                                                ("2018-02-21 17:00:00", [6], ["I\'m using the builds"]),
                                                ("2018-02-21 18:00:00", [7], ["I\'m using the builds"]),
                                                ("2018-02-21 19:00:00", [8], ["I\'m using the builds"]),
                                                ("2018-02-21 20:00:00", [9], ["I\'m using the builds"]),
                                                ("2018-02-21 21:00:00", [10], ["I\'m using the builds"]); """
        // insert another 10 rows
        sql """ INSERT INTO ${tableName} VALUES ("2018-02-21 12:00:00", [1], ["I\'m using the builds"]),
                                                ("2018-02-21 13:00:00", [2], ["I\'m using the builds"]),
                                                ("2018-02-21 14:00:00", [3], ["I\'m using the builds"]),
                                                ("2018-02-21 15:00:00", [4], ["I\'m using the builds"]),
                                                ("2018-02-21 16:00:00", [5], ["I\'m using the builds"]),
                                                ("2018-02-21 17:00:00", [6], ["I\'m using the builds"]),
                                                ("2018-02-21 18:00:00", [7], ["I\'m using the builds"]),
                                                ("2018-02-21 19:00:00", [8], ["I\'m using the builds"]),
                                                ("2018-02-21 20:00:00", [9], ["I\'m using the builds"]),
                                                ("2018-02-21 21:00:00", [10], ["I\'m using the builds"]); """

        qt_sql """ select * from ${tableName} order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where array_contains(body, "using") order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where array_contains(body, "the") order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where comment_id[1]< 8 order by file_time, comment_id[1], body[1] """

        //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
        tablets = sql_return_maparray """ show tablets from ${tableName}; """

        // before full compaction, there are 3 rowsets.
        rowsetCount = get_rowset_count.call(tablets)
        assert (rowsetCount == 3 * replicaNum)

        // trigger full compactions for all tablets in ${tableName}
        trigger_full_compaction_on_tablets.call(tablets)

        // wait for full compaction done
        wait_full_compaction_done.call(tablets)

        // after full compaction, there is only 1 rowset.
        rowsetCount = get_rowset_count.call(tablets)
        if (isCloudMode) {
            assert (rowsetCount == (1 + 1) * replicaNum)
        } else {
            assert (rowsetCount == 1 * replicaNum)
        }

        qt_sql """ select * from ${tableName} order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where array_contains(body, "using") order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where array_contains(body, "the") order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where comment_id[1]< 8 order by file_time, comment_id[1], body[1] """

        // insert 10 rows, again
        sql """ INSERT INTO ${tableName} VALUES ("2018-02-21 12:00:00", [1], ["I\'m using the builds"]),
                                                ("2018-02-21 13:00:00", [2], ["I\'m using the builds"]),
                                                ("2018-02-21 14:00:00", [3], ["I\'m using the builds"]),
                                                ("2018-02-21 15:00:00", [4], ["I\'m using the builds"]),
                                                ("2018-02-21 16:00:00", [5], ["I\'m using the builds"]),
                                                ("2018-02-21 17:00:00", [6], ["I\'m using the builds"]),
                                                ("2018-02-21 18:00:00", [7], ["I\'m using the builds"]),
                                                ("2018-02-21 19:00:00", [8], ["I\'m using the builds"]),
                                                ("2018-02-21 20:00:00", [9], ["I\'m using the builds"]),
                                                ("2018-02-21 21:00:00", [10], ["I\'m using the builds"]); """
        
        sql """ select * from ${tableName} """

        tablets = sql_return_maparray """ show tablets from ${tableName}; """

        // before full compaction, there are 2 rowsets.
        rowsetCount = get_rowset_count.call(tablets)
        if (isCloudMode) {
            assert (rowsetCount == (2 + 1) * replicaNum)
        } else {
            assert (rowsetCount == 2 * replicaNum)
        }
        // trigger full compactions for all tablets in ${tableName}
        trigger_full_compaction_on_tablets.call(tablets)

        // wait for full compaction done
        wait_full_compaction_done.call(tablets)

        // after full compaction, there is only 1 rowset.
        rowsetCount = get_rowset_count.call(tablets)
        if (isCloudMode) {
            assert (rowsetCount == (1 + 1) * replicaNum)
        } else {
            assert (rowsetCount == 1 * replicaNum)
        }
        qt_sql """ select * from ${tableName} order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where array_contains(body, "using") order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where array_contains(body, "the") order by file_time, comment_id[1], body[1] """
        qt_sql """ select * from ${tableName} where comment_id[1]< 8 order by file_time, comment_id[1], body[1] """


    } finally {
        if (has_update_be_config) {
            set_be_config.call("inverted_index_compaction_enable", invertedIndexCompactionEnable.toString())
            set_be_config.call("inverted_index_max_buffered_docs", invertedIndexMaxBufferedDocs.toString())
        }
    }
}
