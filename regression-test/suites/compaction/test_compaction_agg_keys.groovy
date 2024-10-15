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

suite("test_compaction_agg_keys") {
    def tableName = "compaction_agg_keys_regression_test"

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
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `user_id` LARGEINT NOT NULL COMMENT "用户id",
                `date` DATE NOT NULL COMMENT "数据灌入日期时间",
                `datev2` DATEV2 NOT NULL COMMENT "数据灌入日期时间",
                `datetimev2_1` DATETIMEV2(3) NOT NULL COMMENT "数据灌入日期时间",
                `datetimev2_2` DATETIMEV2(6) NOT NULL COMMENT "数据灌入日期时间",
                `city` VARCHAR(20) COMMENT "用户所在城市",
                `age` SMALLINT COMMENT "用户年龄",
                `sex` TINYINT COMMENT "用户性别",
                `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                `last_update_date` DATETIME REPLACE_IF_NOT_NULL DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次更新时间",
                `datetime_val1` DATETIMEV2(3) REPLACE DEFAULT "1970-01-01 00:00:00.111" COMMENT "用户最后一次访问时间",
                `datetime_val2` DATETIME(6) REPLACE_IF_NOT_NULL DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次更新时间",
                `last_visit_date_not_null` DATETIME REPLACE NOT NULL DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
                `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
                `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间",
                `hll_col` HLL HLL_UNION NOT NULL COMMENT "HLL列",
                `bitmap_col` Bitmap BITMAP_UNION NOT NULL COMMENT "bitmap列" )
            AGGREGATE KEY(`user_id`, `date`, `datev2`, `datetimev2_1`, `datetimev2_2`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
            PROPERTIES ( "replication_num" = "1" );
        """

        sql """ INSERT INTO ${tableName} VALUES
             (1, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, '2020-01-01', '2020-01-01', '2017-10-01 11:11:11.170000', '2017-10-01 11:11:11.110111', '2020-01-01', 1, 30, 20, hll_hash(1), to_bitmap(1))
            """

        sql """ INSERT INTO ${tableName} VALUES
             (1, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2017-10-01 11:11:11.160000', '2017-10-01 11:11:11.100111', '2020-01-02', 1, 31, 19, hll_hash(2), to_bitmap(2))
            """

        sql """ INSERT INTO ${tableName} VALUES
             (2, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2017-10-01 11:11:11.150000', '2017-10-01 11:11:11.130111', '2020-01-02', 1, 31, 21, hll_hash(2), to_bitmap(2))
            """

        sql """ INSERT INTO ${tableName} VALUES
             (2, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2017-10-01 11:11:11.140000', '2017-10-01 11:11:11.120111', '2020-01-03', 1, 32, 20, hll_hash(3), to_bitmap(3))
            """

        sql """ INSERT INTO ${tableName} VALUES
             (3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2017-10-01 11:11:11.100000', '2017-10-01 11:11:11.140111', '2020-01-03', 1, 32, 22, hll_hash(3), to_bitmap(3))
            """

        sql """ INSERT INTO ${tableName} VALUES
             (3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, '2020-01-04', '2020-01-04', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.150111', '2020-01-04', 1, 33, 21, hll_hash(4), to_bitmap(4))
            """

        sql """ INSERT INTO ${tableName} VALUES
             (3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, NULL, NULL, NULL, NULL, '2020-01-05', 1, 34, 20, hll_hash(5), to_bitmap(5))
            """

        sql """ INSERT INTO ${tableName} VALUES
             (4, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, NULL, NULL, NULL, NULL, '2020-01-05', 1, 34, 20, hll_hash(5), to_bitmap(5))
            """

        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,QueryHits,VersionCount,PathHash,MetaUrl,CompactionStatus
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """

        // trigger compactions for all tablets in ${tableName}
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId
            (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
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
        for (def tablet in tablets) {
            Awaitility.await().untilAsserted(() -> {
                String tablet_id = tablet.TabletId
                backend_id = tablet.BackendId
                (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)

                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                return compactionStatus.run_status;                

            });
        }

        def replicaNum = get_table_replica_num(tableName)
        logger.info("get table replica num: " + replicaNum)
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
        assert (rowCount < 8 * replicaNum)
        qt_select_default2 """ SELECT * FROM ${tableName} t ORDER BY user_id; """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
