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

suite("test_cumu_compaction_with_delete") {
    def tableName = "test_cumu_compaction_with_delete"
    def cloudMode = isCloudMode()

    def check_version_and_cumu_point = { tablets, version, cumu_point ->
        // before compaction, there are 6 rowsets.
        int rowsetCount = 0
        int cumuPoint = 0
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
            //logger.info(tabletJson)
            cumuPoint = tabletJson["cumulative point"]
        }
        assert (rowsetCount ==  version * 1)
        assert (cumuPoint == cumu_point)
    }

    def cumulative_compaction = { tablets, backendId_to_backendIP, backendId_to_backendHttpPort, backend_id ->
        // trigger cumu compactions for all tablets in ${tableName}
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId

            (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            sleep(1000)

            def compactJson = parseJson(out.trim())
        }
    }

    try {
        String backend_id;

        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
            `user_id` INT NOT NULL,
            `value` INT NOT NULL)
            UNIQUE KEY(`user_id`) 
            DISTRIBUTED BY HASH(`user_id`) 
            BUCKETS 1 
            PROPERTIES ("replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true",
            "enable_mow_light_delete" = "true")"""

        // [0-1] | [2-2] [3-3] [4-4] [5-5] [6-6] [7-7]
        //  cumu point =  2
        sql """ INSERT INTO ${tableName} VALUES (1,1)"""
        sql """ INSERT INTO ${tableName} VALUES (2,2)"""
        sql """ INSERT INTO ${tableName} VALUES (3,3)"""
        sql """ INSERT INTO ${tableName} VALUES (4,4)"""
        sql """ INSERT INTO ${tableName} VALUES (5,5)"""
        sql """ INSERT INTO ${tableName} VALUES (6,6)"""
        qt_1 """select * from ${tableName} order by user_id, value"""

        //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """

        check_version_and_cumu_point(tablets, 7, 2)

        // [0-1] [2-7] |
        //  cumu point = 2
        cumulative_compaction(tablets, backendId_to_backendIP, backendId_to_backendHttpPort, backend_id)
        if (cloudMode) {
            check_version_and_cumu_point(tablets, 2, 2)
        } else {
            check_version_and_cumu_point(tablets, 2, 8)
        }

        // [0-1] [2-7] | [8-8]
        //  cumu point = 8
        sql """ delete from ${tableName} where user_id = 1"""
        qt_3 """select * from ${tableName} order by user_id, value"""

        // [0-1] [2-7] [8-8] |
        //  cumu point = 9
        cumulative_compaction(tablets, backendId_to_backendIP, backendId_to_backendHttpPort, backend_id)
        check_version_and_cumu_point(tablets, 3, 9)

        // [0-1] [2-7] [8-8] | [9-9]
        //  cumu point = 9
        sql """ delete from ${tableName} where user_id = 2"""
        qt_4 """select * from ${tableName} order by user_id, value"""

        // [0-1] [2-7] [8-8] [9-9] |
        //  cumu point = 10
        cumulative_compaction(tablets, backendId_to_backendIP, backendId_to_backendHttpPort, backend_id)
        check_version_and_cumu_point(tablets, 4, 10)

        // [0-1] [2-7] [8-8] [9-9] | [10-10]
        //  cumu point = 10
        sql """ delete from ${tableName} where user_id = 3"""
        qt_5 """select * from ${tableName} order by user_id, value"""

        // [0-1] [2-7] [8-8] [9-9] [10-10] |
        //  cumu point = 11
        cumulative_compaction(tablets, backendId_to_backendIP, backendId_to_backendHttpPort, backend_id)
        check_version_and_cumu_point(tablets, 5, 11)

        // [0-1] [2-7] [8-8] [9-9] [10-10] | [11-11]
        //  cumu point = 11
        sql """ delete from ${tableName} where user_id = 4"""
        qt_5 """select * from ${tableName} order by user_id, value"""

        // [0-1] [2-7] [8-8] [9-9] [10-10] [11-11] |
        //  cumu point = 12
        cumulative_compaction(tablets, backendId_to_backendIP, backendId_to_backendHttpPort, backend_id)
        check_version_and_cumu_point(tablets, 6, 12)

        // [0-1] [2-7] [8-8] [9-9] [10-10] [11-11] | [12-12]
        //  cumu point = 12
        sql """ delete from ${tableName} where user_id = 5"""
        qt_5 """select * from ${tableName} order by user_id, value"""

        // [0-1] [2-7] [8-8] [9-9] [10-10] [11-11] [12-12] |
        //  cumu point = 13
        cumulative_compaction(tablets, backendId_to_backendIP, backendId_to_backendHttpPort, backend_id)
        check_version_and_cumu_point(tablets, 7, 13)

        // trigger base compactions for all tablets in ${tableName}
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId

            (code, out, err) = be_run_base_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            sleep(1000)

            def compactJson = parseJson(out.trim())
        }

        // after base compaction, there is only 1 rowset.
        rowsetCount = 0
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
        }
        if (cloudMode) {
            assert (rowsetCount == 2)
        } else {
            assert (rowsetCount == 1)
        }

        qt_6 """select * from ${tableName} order by user_id, value"""

    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
