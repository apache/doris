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

suite("test_index_compaction_with_multi_index_segments", "p0") {
    def tableName = "test_index_compaction_with_multi_index_segments"
  
    def set_be_config = { key, value ->
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    boolean disableAutoCompaction = true
    boolean invertedIndexCompactionEnable = false
    int invertedIndexMaxBufferedDocs = -1;
    boolean has_update_be_config = false

    try {
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

        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE ${tableName} (
                `file_time` DATETIME NOT NULL,
                `comment_id` int(11)  NULL,
                `body` TEXT NULL DEFAULT "",
                INDEX idx_comment_id (`comment_id`) USING INVERTED COMMENT '''',
                INDEX idx_body (`body`) USING INVERTED PROPERTIES("parser" = "unicode") COMMENT ''''
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
        sql """ INSERT INTO ${tableName} VALUES ("2018-02-21 12:00:00", 1, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 2, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 3, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 4, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 5, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 6, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 7, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 8, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 9, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 10, "I\'m using the builds"); """
        // insert another 10 rows
        sql """ INSERT INTO ${tableName} VALUES ("2018-02-21 12:00:00", 1, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 2, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 3, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 4, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 5, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 6, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 7, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 8, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 9, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 10, "I\'m using the builds"); """

        qt_sql """ select * from ${tableName} order by file_time, comment_id, body """
        qt_sql """ select * from ${tableName} where body match "using" order by file_time, comment_id, body """
        qt_sql """ select * from ${tableName} where body match "the" order by file_time, comment_id, body """
        qt_sql """ select * from ${tableName} where comment_id < 8 order by file_time, comment_id, body """

        //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
        String[][] tablets = sql """ show tablets from ${tableName}; """

        def replicaNum = get_table_replica_num(tableName)
        logger.info("get table replica num: " + replicaNum)
        // before full compaction, there are 3 rowsets.
        int rowsetCount = 0
        for (String[] tablet in tablets) {
            String tablet_id = tablet[0]
            def compactionStatusUrlIndex = 18
            (code, out, err) = curl("GET", tablet[compactionStatusUrlIndex])
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
        }
        assert (rowsetCount == 3 * replicaNum)

        // trigger full compactions for all tablets in ${tableName}
        for (String[] tablet in tablets) {
            String tablet_id = tablet[0]
            backend_id = tablet[2]
            times = 1

            do{
                (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                ++times
                sleep(2000)
            } while (parseJson(out.trim()).status.toLowerCase()!="success" && times<=10)

            def compactJson = parseJson(out.trim())
            if (compactJson.status.toLowerCase() == "fail") {
                assertEquals(disableAutoCompaction, false)
                logger.info("Compaction was done automatically!")
            }
            if (disableAutoCompaction) {
                assertEquals("success", compactJson.status.toLowerCase())
            }
        }

        // wait for full compaction done
        for (String[] tablet in tablets) {
            boolean running = true
            do {
                Thread.sleep(1000)
                String tablet_id = tablet[0]
                backend_id = tablet[2]
                (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)
        }

        // after full compaction, there is only 1 rowset.
        
        rowsetCount = 0
        for (String[] tablet in tablets) {
            String tablet_id = tablet[0]
            def compactionStatusUrlIndex = 18
            (code, out, err) = curl("GET", tablet[compactionStatusUrlIndex])
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
        }
        assert (rowsetCount == 1 * replicaNum)

        qt_sql """ select * from ${tableName} order by file_time, comment_id, body """
        qt_sql """ select * from ${tableName} where body match "using" order by file_time, comment_id, body """
        qt_sql """ select * from ${tableName} where body match "the" order by file_time, comment_id, body """
        qt_sql """ select * from ${tableName} where comment_id < 8 order by file_time, comment_id, body """

        // insert 10 rows, again
        sql """ INSERT INTO ${tableName} VALUES ("2018-02-21 12:00:00", 1, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 2, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 3, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 4, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 5, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 6, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 7, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 8, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 9, "I\'m using the builds"),
                                                ("2018-02-21 12:00:00", 10, "I\'m using the builds"); """

        tablets = sql """ show tablets from ${tableName}; """

        replicaNum = get_table_replica_num(tableName)
        logger.info("get table replica num: " + replicaNum)
        // before full compaction, there are 2 rowsets.
        rowsetCount = 0
        for (String[] tablet in tablets) {
            String tablet_id = tablet[0]
            def compactionStatusUrlIndex = 18
            (code, out, err) = curl("GET", tablet[compactionStatusUrlIndex])
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
        }
        assert (rowsetCount == 2 * replicaNum)

        // trigger full compactions for all tablets in ${tableName}
        for (String[] tablet in tablets) {
            String tablet_id = tablet[0]
            backend_id = tablet[2]
            times = 1

            do{
                (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                ++times
                sleep(2000)
            } while (parseJson(out.trim()).status.toLowerCase()!="success" && times<=10)

            def compactJson = parseJson(out.trim())
            if (compactJson.status.toLowerCase() == "fail") {
                assertEquals(disableAutoCompaction, false)
                logger.info("Compaction was done automatically!")
            }
            if (disableAutoCompaction) {
                assertEquals("success", compactJson.status.toLowerCase())
            }
        }

        // wait for full compaction done
        for (String[] tablet in tablets) {
            boolean running = true
            do {
                Thread.sleep(1000)
                String tablet_id = tablet[0]
                backend_id = tablet[2]
                (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)
        }

        // after full compaction, there is only 1 rowset.
        
        rowsetCount = 0
        for (String[] tablet in tablets) {
            String tablet_id = tablet[0]
            def compactionStatusUrlIndex = 18
            (code, out, err) = curl("GET", tablet[compactionStatusUrlIndex])
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
        }
        assert (rowsetCount == 1 * replicaNum)

        qt_sql """ select * from ${tableName} order by file_time, comment_id, body """
        qt_sql """ select * from ${tableName} where body match "using" order by file_time, comment_id, body """
        qt_sql """ select * from ${tableName} where body match "the" order by file_time, comment_id, body """
        qt_sql """ select * from ${tableName} where comment_id < 8 order by file_time, comment_id, body """

    } finally {
        if (has_update_be_config) {
            set_be_config.call("inverted_index_compaction_enable", invertedIndexCompactionEnable.toString())
            set_be_config.call("inverted_index_max_buffered_docs", invertedIndexMaxBufferedDocs.toString())
        }
    }
}