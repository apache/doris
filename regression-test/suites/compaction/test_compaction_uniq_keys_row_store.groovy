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


suite("test_compaction_uniq_keys") {
    def tableName = "compaction_uniq_keys_row_store_regression_test"
    def setPrepareStmtArgs = {stmt, user_id, date, datev2, datetimev2_1, datetimev2_2, city, age, sex ->
        java.text.SimpleDateFormat formater = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
        stmt.setInt(1, user_id)
        println "2easdad a $date"
        stmt.setDate(2, java.sql.Date.valueOf(date))
        stmt.setDate(3, java.sql.Date.valueOf(datev2))
        stmt.setTimestamp(4, new java.sql.Timestamp(formater.parse(datetimev2_1).getTime()))
        stmt.setTimestamp(5, new java.sql.Timestamp(formater.parse(datetimev2_2).getTime()))
        stmt.setString(6, city)
        stmt.setInt(7, age)
        stmt.setInt(8, sex)
    }

    try {
        //BackendId,Cluster,IP,HeartbeatPort,BePort,HttpPort,BrpcPort,LastStartTime,LastHeartbeat,Alive,SystemDecommissioned,ClusterDecommissioned,TabletNum,DataUsedCapacity,AvailCapacity,TotalCapacity,UsedPct,MaxDiskUsedPct,Tag,ErrMsg,Version,Status
        String[][] backends = sql """ show backends; """
        assertTrue(backends.size() > 0)
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        for (String[] backend in backends) {
            backendId_to_backendIP.put(backend[0], backend[2])
            backendId_to_backendHttpPort.put(backend[0], backend[5])
        }

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

        def checkValue = { ->
            println "fuckkkkk"
            def user = context.config.jdbcUser
            def password = context.config.jdbcPassword
            def url = context.config.jdbcUrl + "&useServerPrepStmts=true"
            def result1 = connect(user=user, password=password, url=url) {
                def stmt = prepareStatement """ SELECT * FROM ${tableName} t where user_id = ? and date = ? and datev2 = ? and datetimev2_1 = ? and datetimev2_2 = ? and city = ? and age = ? and sex = ?; """
                setPrepareStmtArgs stmt, 1, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.21', '2017-10-01 11:11:11.11', 'Beijing', 10, 1
                qe_point_select stmt
                setPrepareStmtArgs stmt, 1, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.22', '2017-10-01 11:11:11.12', 'Beijing', 10, 1
                qe_point_select stmt
                setPrepareStmtArgs stmt, 2, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.23', '2017-10-01 11:11:11.13', 'Beijing', 10, 1
                qe_point_select stmt
                setPrepareStmtArgs stmt, 2, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.24', '2017-10-01 11:11:11.14', 'Beijing', 10, 1
                qe_point_select stmt
                setPrepareStmtArgs stmt, 3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.25', '2017-10-01 11:11:11.15', 'Beijing', 10, 1
                qe_point_select stmt
                setPrepareStmtArgs stmt, 3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.26', '2017-10-01 11:11:11.16', 'Beijing', 10, 1
                qe_point_select stmt
                setPrepareStmtArgs stmt, 3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.27', '2017-10-01 11:11:11.17', 'Beijing', 10, 1
                qe_point_select stmt
                setPrepareStmtArgs stmt, 4, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.28', '2017-10-01 11:11:11.18', 'Beijing', 10, 1
                qe_point_select stmt
            }
        }

        def user = context.config.jdbcUser
        def password = context.config.jdbcPassword
        def url = context.config.jdbcUrl + "&useServerPrepStmts=true"
        def tablets = null
        def result1 = connect(user=user, password=password, url=url) {

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `user_id` int NOT NULL COMMENT "用户id",
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
                `min_dwell_time` INT DEFAULT "99999" COMMENT "用户最小停留时间")
            UNIQUE KEY(`user_id`, `date`, `datev2`, `datetimev2_1`, `datetimev2_2`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
            PROPERTIES ( "replication_num" = "1", "enable_unique_key_merge_on_write" = "true", "light_schema_change" = "true", "store_row_column" = "true" );
        """

        sql """ INSERT INTO ${tableName} VALUES
             (1, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.021', '2017-10-01 11:11:11.011', 'Beijing', 10, 1, '2020-01-01', '2020-01-01', '2017-10-01 11:11:11.170000', '2017-10-01 11:11:11.110111', '2020-01-01', 1, 30, 20)
            """

        sql """ INSERT INTO ${tableName} VALUES
             (1, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.022', '2017-10-01 11:11:11.012', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2017-10-01 11:11:11.160000', '2017-10-01 11:11:11.100111', '2020-01-02', 1, 31, 19)
            """

        sql """ INSERT INTO ${tableName} VALUES
             (2, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.023', '2017-10-01 11:11:11.013', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2017-10-01 11:11:11.150000', '2017-10-01 11:11:11.130111', '2020-01-02', 1, 31, 21)
            """

        sql """ INSERT INTO ${tableName} VALUES
             (2, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.024', '2017-10-01 11:11:11.014', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2017-10-01 11:11:11.140000', '2017-10-01 11:11:11.120111', '2020-01-03', 1, 32, 20)
            """

        sql """ INSERT INTO ${tableName} VALUES
             (3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.025', '2017-10-01 11:11:11.015', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2017-10-01 11:11:11.100000', '2017-10-01 11:11:11.140111', '2020-01-03', 1, 32, 22)
            """

        sql """ INSERT INTO ${tableName} VALUES
             (3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.026', '2017-10-01 11:11:11.016', 'Beijing', 10, 1, '2020-01-04', '2020-01-04', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.150111', '2020-01-04', 1, 33, 21)
            """

        sql """ INSERT INTO ${tableName} VALUES
             (3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.027', '2017-10-01 11:11:11.017', 'Beijing', 10, 1, NULL, NULL, NULL, NULL, '2020-01-05', 1, 34, 20)
            """

        sql """ INSERT INTO ${tableName} VALUES
             (4, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.028', '2017-10-01 11:11:11.018', 'Beijing', 10, 1, NULL, NULL, NULL, NULL, '2020-01-05', 1, 34, 20)
            """
        //TabletId,ReplicaIdBackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
        tablets = sql """ show tablets from ${tableName}; """
        }

        checkValue()


        // trigger compactions for all tablets in ${tableName}
        for (String[] tablet in tablets) {
            String tablet_id = tablet[0]
            backend_id = tablet[2]
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X POST http://")
            sb.append(backendId_to_backendIP.get(backend_id))
            sb.append(":")
            sb.append(backendId_to_backendHttpPort.get(backend_id))
            sb.append("/api/compaction/run?tablet_id=")
            sb.append(tablet_id)
            sb.append("&compact_type=cumulative")

            String command = sb.toString()
            process = command.execute()
            code = process.waitFor()
            err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            out = process.getText()
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
        for (String[] tablet in tablets) {
            boolean running = true
            do {
                Thread.sleep(1000)
                String tablet_id = tablet[0]
                backend_id = tablet[2]
                StringBuilder sb = new StringBuilder();
                sb.append("curl -X GET http://")
                sb.append(backendId_to_backendIP.get(backend_id))
                sb.append(":")
                sb.append(backendId_to_backendHttpPort.get(backend_id))
                sb.append("/api/compaction/run_status?tablet_id=")
                sb.append(tablet_id)

                String command = sb.toString()
                logger.info(command)
                process = command.execute()
                code = process.waitFor()
                err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
                out = process.getText()
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)
        }

        int rowCount = 0
        for (String[] tablet in tablets) {
            String tablet_id = tablet[0]
            StringBuilder sb = new StringBuilder();
            def compactionStatusUrlIndex = 17
            sb.append("curl -X GET ")
            sb.append(tablet[compactionStatusUrlIndex])
            String command = sb.toString()
            // wait for cleaning stale_rowsets
            process = command.execute()
            code = process.waitFor()
            err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            out = process.getText()
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            for (String rowset in (List<String>) tabletJson.rowsets) {
                rowCount += Integer.parseInt(rowset.split(" ")[1])
            }
        }
        assert (rowCount < 8)
        checkValue()
    } finally {
        // try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
