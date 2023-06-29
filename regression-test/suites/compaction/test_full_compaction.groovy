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

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                ( `user_id` INT NOT NULL, `value` INT NOT NULL)
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
        qt_1 """select * from ${tableName}"""


        // version2 (1,10)(2,20)
        sql """ INSERT INTO ${tableName} VALUES
            (1,10),(2,20)
            """
        qt_2 """select * from ${tableName}"""


        // version3 (1,100)(2,200)
        sql """ INSERT INTO ${tableName} VALUES
            (1,100),(2,200)
            """
        qt_3 """select * from ${tableName}"""


        // version4 (1,100)(2,200)(3,300)
        sql """ INSERT INTO ${tableName} VALUES
            (3,300)
            """
        qt_4 """select * from ${tableName}"""


        // version5 (1,100)(2,200)(3,100)
        sql """update ${tableName} set value = 100 where user_id = 3"""
        qt_5 """select * from ${tableName}"""


        // version6 (1,100)(2,200)
        sql """delete from ${tableName} where user_id = 3"""
        qt_6 """select * from ${tableName}"""


        streamLoad {
            // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
            // db 'regression_test'
            table tableName

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', ','
            set 'compress_type', 'GZ'
            set "columns", columnsMap[tableName]
            set 'timeout', '72000'
            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file """${getS3Url() + '/regression/clickhouse/brown/' + tableName}.gz"""

            time 0


            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }

        // ------------------------------------------------------ 
        //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
        String[][] tablets = sql """ show tablets from ${tableName}; """

        // trigger compactions for all tablets in ${tableName}
        for (String[] tablet in tablets) {
            String tablet_id = tablet[0]
            backend_id = tablet[2]
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

        int rowCount = 0
        for (String[] tablet in tablets) {
            String tablet_id = tablet[0]
            def compactionStatusUrlIndex = 18
            (code, out, err) = curl("GET", tablet[compactionStatusUrlIndex])
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            for (String rowset in (List<String>) tabletJson.rowsets) {
                rowCount += Integer.parseInt(rowset.split(" ")[1])
            }
        }
        assert (rowCount <= 8)
        qt_select_default3 """ SELECT * FROM ${tableName} t ORDER BY user_id,date,city,age,sex,last_visit_date,last_update_date,last_visit_date_not_null,cost,max_dwell_time,min_dwell_time; """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}