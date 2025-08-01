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

suite("test_mow_table_with_format_v2", "inverted_index_format_v2") {
    def tableName = "test_mow_table_with_format_v2"

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(10000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def calc_segment_count = { tablet ->
        int segment_count = 0
        String tablet_id = tablet.TabletId
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET ")
        sb.append(tablet.CompactionStatus)
        String command = sb.toString()
        // wait for cleaning stale_rowsets
        def process = command.execute()
        def code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        def out = process.getText()
        logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        for (String rowset in (List<String>) tabletJson.rowsets) {
            segment_count += Integer.parseInt(rowset.split(" ")[1])
        }
        return segment_count
    }

    try {
        //BackendId,Cluster,IP,HeartbeatPort,BePort,HttpPort,BrpcPort,LastStartTime,LastHeartbeat,Alive,SystemDecommissioned,ClusterDecommissioned,TabletNum,DataUsedCapacity,AvailCapacity,TotalCapacity,UsedPct,MaxDiskUsedPct,Tag,ErrMsg,Version,Status
        String[][] backends = sql """ show backends; """
        assertTrue(backends.size() > 0)
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        StringBuilder showConfigCommand = new StringBuilder();
        showConfigCommand.append("curl -X GET http://")
        showConfigCommand.append(backendId_to_backendIP.get(backend_id))
        showConfigCommand.append(":")
        showConfigCommand.append(backendId_to_backendHttpPort.get(backend_id))
        showConfigCommand.append("/api/show_config")
        logger.info(showConfigCommand.toString())
        def process = showConfigCommand.toString().execute()
        int code = process.waitFor()
        String err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        String out = process.getText()
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
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `user_id` LARGEINT NOT NULL COMMENT "用户id",
                `date` DATE NOT NULL COMMENT "数据灌入日期时间",
                `datev2` DATEV2 NOT NULL COMMENT "数据灌入日期时间",
                `datetimev2_1` DATETIMEV2(3) NOT NULL COMMENT "数据灌入日期时间",
                `datetimev2_2` DATETIMEV2(6) NOT NULL COMMENT "数据灌入日期时间",
                `city` VARCHAR(20) COMMENT "用户所在城市",
                `age` SMALLINT COMMENT "用户年龄",
                `sex` TINYINT COMMENT "用户性别",
                `last_visit_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                `last_update_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次更新时间",
                `datetime_val1` DATETIMEV2(3) DEFAULT "1970-01-01 00:00:00.111" COMMENT "用户最后一次访问时间",
                `datetime_val2` DATETIME(6) DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次更新时间",
                `last_visit_date_not_null` DATETIME NOT NULL DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                `cost` BIGINT DEFAULT "0" COMMENT "用户总消费",
                `max_dwell_time` INT DEFAULT "0" COMMENT "用户最大停留时间",
                `min_dwell_time` INT DEFAULT "99999" COMMENT "用户最小停留时间",
                INDEX idx_user_id (`user_id`) USING INVERTED,
                INDEX idx_date (`date`) USING INVERTED,
                INDEX idx_city (`city`) USING INVERTED)
            UNIQUE KEY(`user_id`, `date`, `datev2`, `datetimev2_1`, `datetimev2_2`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
            PROPERTIES ( "replication_num" = "1", "disable_auto_compaction" = "true", "enable_unique_key_merge_on_write" = "true" );
        """

        sql """ INSERT INTO ${tableName} VALUES
             (1, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, '2020-01-01', '2020-01-01', '2017-10-01 11:11:11.170000', '2017-10-01 11:11:11.110111', '2020-01-01', 1, 30, 20)
            """

        sql """ INSERT INTO ${tableName} VALUES
             (1, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2017-10-01 11:11:11.160000', '2017-10-01 11:11:11.100111', '2020-01-02', 1, 31, 19)
            """

        sql """ INSERT INTO ${tableName} VALUES
             (2, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Shanghai', 10, 1, '2020-01-02', '2020-01-02', '2017-10-01 11:11:11.150000', '2017-10-01 11:11:11.130111', '2020-01-02', 1, 31, 21)
            """

        sql """ INSERT INTO ${tableName} VALUES
             (2, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Shanghai', 10, 1, '2020-01-03', '2020-01-03', '2017-10-01 11:11:11.140000', '2017-10-01 11:11:11.120111', '2020-01-03', 1, 32, 20)
            """

        sql """ INSERT INTO ${tableName} VALUES
             (3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Guangzhou', 10, 1, '2020-01-03', '2020-01-03', '2017-10-01 11:11:11.100000', '2017-10-01 11:11:11.140111', '2020-01-03', 1, 32, 22)
            """

        sql """ INSERT INTO ${tableName} VALUES
             (3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Guangzhou', 10, 1, '2020-01-04', '2020-01-04', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.150111', '2020-01-04', 1, 33, 21)
            """

        sql """ INSERT INTO ${tableName} VALUES
             (3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Shenzhen', 10, 1, NULL, NULL, NULL, NULL, '2020-01-05', 1, 34, 20)
            """

        sql """ INSERT INTO ${tableName} VALUES
             (4, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Shenzhen', 10, 1, NULL, NULL, NULL, NULL, '2020-01-05', 1, 34, 20)
            """

        sql """ sync """
        sql """ set enable_common_expr_pushdown = true """

        qt_select_default """ SELECT * FROM ${tableName} t WHERE city MATCH 'Beijing' ORDER BY user_id,date,city,age,sex,last_visit_date,last_update_date,last_visit_date_not_null,cost,max_dwell_time,min_dwell_time; """

        //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,QueryHits,PathHash,MetaUrl,CompactionStatus
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """

        // trigger compactions for all tablets in ${tableName}
        // trigger_and_wait_compaction(tableName, "cumulative")
        // check indexes
        for (def tablet in tablets) {
            boolean running = true
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId
            String ip = backendId_to_backendIP.get(backend_id)
            String port = backendId_to_backendHttpPort.get(backend_id)
            be_show_tablet_status(ip, port, tablet_id)
            (code, out, err) = be_show_tablet_status(ip, port, tablet_id)
            logger.info("Run show: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(out.contains("[0-1]"))
            assertTrue(out.contains("[2-2]"))
            assertTrue(out.contains("[3-3]"))
            assertTrue(out.contains("[4-4]"))
            assertTrue(out.contains("[5-5]"))
            assertTrue(out.contains("[6-6]"))
            assertTrue(out.contains("[7-7]"))
            assertTrue(out.contains("[8-8]"))
            assertTrue(out.contains("[9-9]"))
            logger.info("run compaction:" + tablet_id)
            (code, out, err) = be_run_cumulative_compaction(ip, port, tablet_id)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            do {
                Thread.sleep(100)
                (code, out, err) = be_get_compaction_status(ip, port, tablet_id)
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)
            (code, out, err) = be_show_tablet_status(ip, port, tablet_id)
            logger.info("Run show: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(out.contains("[0-1]"))
            // Parse the tablet status to get rowset count after compaction
            def tabletJson = parseJson(out.trim())
            def rowsets = tabletJson.rowsets
            int activeRowsetCount = rowsets.size()
            // After compaction, we should have fewer rowsets than before (originally 9 rowsets: [0-1], [2-2], ..., [9-9])
            // The exact number depends on compaction strategy, but should be less than 9
            assertTrue(activeRowsetCount < 9, "Expected fewer rowsets after compaction, got: ${activeRowsetCount}")
            assertTrue(activeRowsetCount >= 2, "Expected at least 2 rowsets after compaction, got: ${activeRowsetCount}")
            // Verify we still have [0-1] and some compacted rowsets starting from version 2
            boolean hasBaseRowset = rowsets.any { it.contains("[0-1]") }
            boolean hasCompactedRowsets = rowsets.any { it.contains("[2-") }
            assertTrue(hasBaseRowset, "Should have base rowset [0-1]")
            assertTrue(hasCompactedRowsets, "Should have compacted rowsets starting from version 2")
            check_nested_index_file(ip, port, tablet_id, activeRowsetCount, 3, "V2")
        }

        int segmentsCount = 0
        for (def tablet in tablets) {
            segmentsCount += calc_segment_count(tablet)
        }

        def dedup_tablets = deduplicate_tablets(tablets)

        // In the p0 testing environment, there are no expected operations such as scaling down BE (backend) services
        // if tablets or dedup_tablets is empty, exception is thrown, and case fail
        int replicaNum = Math.floor(tablets.size() / dedup_tablets.size())
        if (replicaNum != 1 && replicaNum != 3)
        {
            assert(false);
        }

        assert (segmentsCount <= 8*replicaNum)
        qt_select_default2 """ SELECT * FROM ${tableName} t WHERE city MATCH 'Beijing' ORDER BY user_id,date,city,age,sex,last_visit_date,last_update_date,last_visit_date_not_null,cost,max_dwell_time,min_dwell_time; """
    } finally {
    }
}
