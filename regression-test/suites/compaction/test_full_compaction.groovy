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

suite("test_full_compaction") {
    def tableName = "test_full_compaction"

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
            `user_id` INT NOT NULL, `value` INT NOT NULL)
            UNIQUE KEY(`user_id`) 
            DISTRIBUTED BY HASH(`user_id`) 
            BUCKETS 1 
            PROPERTIES ("replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true",
            "enable_unique_key_merge_on_write" = "true");"""

        // version1 (1,1)(2,2)
        sql """ INSERT INTO ${tableName} VALUES
            (1,1),(2,2)
            """
        qt_1 """select * from ${tableName} order by user_id"""


        // version2 (1,10)(2,20)
        sql """ INSERT INTO ${tableName} VALUES
            (1,10),(2,20)
            """
        qt_2 """select * from ${tableName} order by user_id"""


        // version3 (1,100)(2,200)
        sql """ INSERT INTO ${tableName} VALUES
            (1,100),(2,200)
            """
        qt_3 """select * from ${tableName} order by user_id"""


        // version4 (1,100)(2,200)(3,300)
        sql """ INSERT INTO ${tableName} VALUES
            (3,300)
            """
        qt_4 """select * from ${tableName} order by user_id"""


        // version5 (1,100)(2,200)(3,100)
        sql """update ${tableName} set value = 100 where user_id = 3"""
        qt_5 """select * from ${tableName} order by user_id"""


        // version6 (1,100)(2,200)
        sql """delete from ${tableName} where user_id = 3"""
        qt_6 """select * from ${tableName} order by user_id"""

        sql "SET skip_delete_predicate = true"
        sql "SET skip_delete_sign = true"
        sql "SET skip_delete_bitmap = true"
        // show all hidden data
        // (1,10)(1,100)(2,2)(2,20)(2,200)(3,300)(3,100)
        qt_skip_delete """select * from ${tableName} order by user_id"""

        //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
        String[][] tablets = sql """ show tablets from ${tableName}; """

        // before full compaction, there are 7 rowsets.
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
        assert (rowsetCount == 7)

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
        assert (rowsetCount == 1)

        // make sure all hidden data has been deleted
        // (1,100)(2,200)
        qt_select_final """select * from ${tableName} order by user_id"""
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
