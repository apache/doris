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
import org.apache.doris.regression.util.Http

suite("test_fix_tablet_stat_fault_injection", "nonConcurrent") {
    if(isCloudMode()){
        def tableName = "test_fix_tablet_stat_fault_injection"
        def bucketSize = 10
        def partitionSize = 100
        def maxPartition = partitionSize + 1
        def create_table_sql = """
                    CREATE TABLE IF NOT EXISTS ${tableName}
                        (
                        `k1` INT NULL,
                        `v1` INT NULL,
                        `v2` INT NULL
                        )
                        UNIQUE KEY (k1)
                        PARTITION BY RANGE(`k1`)
                        (
                            FROM (1) TO (${maxPartition}) INTERVAL 1
                        )
                        DISTRIBUTED BY HASH(`k1`) BUCKETS ${bucketSize}
                        PROPERTIES (
                        "replication_num" = "1",
                        "disable_auto_compaction" = "true"
                        );
                """
        def insertData = {
                def backendId_to_backendIP = [:]
                def backendId_to_backendHttpPort = [:]
                getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
            try {
                // enable debug point
                GetDebugPoint().enableDebugPointForAllBEs("CloudFullCompaction::modify_rowsets.wrong_compaction_data_size")
                // insert data
                sql """ DROP TABLE IF EXISTS ${tableName} """

                sql "${create_table_sql}"
                (1..partitionSize).each { i ->
                    sql "insert into ${tableName} values (${i},1,1);"
                    sql "insert into ${tableName} values (${i},2,2);"
                    sql "insert into ${tableName} values (${i},3,3);"
                    sql "insert into ${tableName} values (${i},4,4);"
                    sql "insert into ${tableName} values (${i},5,5);"
                }

                sql "select count(*) from ${tableName};"
                sleep(60000)
                qt_select_1 "show data from ${tableName};"

                // check rowsets num
                def tablets = sql_return_maparray """ show tablets from ${tableName}; """
                // before full compaction, there are 6 rowsets.
                int rowsetCount = 0
                for (def tablet in tablets) {
                    String tablet_id = tablet.TabletId
                    (code, out, err) = curl("GET", tablet.CompactionStatus)
                    logger.info("Show tablets status after insert data: code=" + code + ", out=" + out + ", err=" + err)
                    assertEquals(code, 0)
                    def tabletJson = parseJson(out.trim())
                    assert tabletJson.rowsets instanceof List
                    rowsetCount +=((List<String>) tabletJson.rowsets).size()
                }
                assert (rowsetCount == 6 * bucketSize * partitionSize) 

                // trigger full compactions for all tablets in ${tableName}
                for (def tablet in tablets) {
                    String tablet_id = tablet.TabletId
                    backend_id = tablet.BackendId
                    times = 1

                    do{
                        (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                        ++times
                    } while (parseJson(out.trim()).status.toLowerCase()!="success" && times<=10)

                    def compactJson = parseJson(out.trim())
                    assertEquals("success", compactJson.status.toLowerCase())
                }

                // wait for full compaction done
                for (def tablet in tablets) {
                    boolean running = true
                    do {
                        String tablet_id = tablet.TabletId
                        backend_id = tablet.BackendId
                        (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                        logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                        assertEquals(code, 0)
                        def compactionStatus = parseJson(out.trim())
                        assertEquals("success", compactionStatus.status.toLowerCase())
                        running = compactionStatus.run_status
                    } while (running)
                }

                sleep(60000)
                // after full compaction, there are 2 rowsets.
                rowsetCount = 0
                for (def tablet in tablets) {
                    String tablet_id = tablet.TabletId
                    (code, out, err) = curl("GET", tablet.CompactionStatus)
                    logger.info("Show tablets status after full compaction: code=" + code + ", out=" + out + ", err=" + err)
                    assertEquals(code, 0)
                    def tabletJson = parseJson(out.trim())
                    assert tabletJson.rowsets instanceof List
                    rowsetCount +=((List<String>) tabletJson.rowsets).size()
                }
                // assert (rowsetCount == 2 * bucketSize * partitionSize)

                // data size should be very large
                sql "select count(*) from ${tableName};"
                qt_select_2 "show data from ${tableName};"


                fix_tablet_stats(getTableId(tableName))

                sleep(60000)
                // after fix, there are 2 rowsets.
                rowsetCount = 0
                for (def tablet in tablets) {
                    String tablet_id = tablet.TabletId
                    (code, out, err) = curl("GET", tablet.CompactionStatus)
                    //logger.info("Show tablets status after fix stats: code=" + code + ", out=" + out + ", err=" + err)
                    assertEquals(code, 0)
                    def tabletJson = parseJson(out.trim())
                    assert tabletJson.rowsets instanceof List
                    rowsetCount +=((List<String>) tabletJson.rowsets).size()
                }
                // assert (rowsetCount == 2 * bucketSize * partitionSize)
                // after fix table stats, data size should be normal
                sql "select count(*) from ${tableName};"
                qt_select_3 "show data from ${tableName};"
            } finally {
                //try_sql("DROP TABLE IF EXISTS ${tableName}")
                GetDebugPoint().disableDebugPointForAllBEs("CloudFullCompaction::modify_rowsets.wrong_compaction_data_size")
            }
        }
        insertData()
    }
}

