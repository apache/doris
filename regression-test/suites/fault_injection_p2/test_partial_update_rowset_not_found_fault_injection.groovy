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

suite("test_partial_update_rowset_not_found_fault_injection", "p2,nonConcurrent") {
    def testTable = "test_partial_update_rowset_not_found_fault_injection"
    sql """ DROP TABLE IF EXISTS ${testTable}"""
    sql """
        create table ${testTable}
        (
        `k1` INT,
        `v1` INT NOT NULL,
        `v2` INT NOT NULL,
        `v3` INT NOT NULL,
        `v4` INT NOT NULL,
        `v5` INT NOT NULL,
        `v6` INT NOT NULL,
        `v7` INT NOT NULL,
        `v8` INT NOT NULL,
        `v9` INT NOT NULL,
        `v10` INT NOT NULL
        )
        UNIQUE KEY (`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1",
        "disable_auto_compaction" = "true"
        );
    """

    def load_data =  {
        streamLoad {
            table 'test_partial_update_rowset_not_found_fault_injection'
            set 'column_separator', ','
            set 'compress_type', 'GZ'


            file """${getS3Url()}/regression/fault_injection/test_partial_update_rowset_not_found_falut_injection1.csv.gz"""

            time 300000 

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
    }

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_params = [string:[:]]

    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    load_data()
    def error = false


    GetDebugPoint().clearDebugPointsForAllBEs()
    try {
        GetDebugPoint().enableDebugPointForAllBEs("VerticalSegmentWriter._append_block_with_partial_content.sleep")
        GetDebugPoint().enableDebugPointForAllBEs("SizeBaseCumulativeCompactionPolicy.pick_input_rowsets.return_input_rowsets")
        def thread = Thread.start{
            try {
                sql """update ${testTable} set v10=1"""
            }
            catch (Exception e){
                logger.info(e.getMessage())
                error = true
            }
        }

        Thread.sleep(2000)
        // trigger compactions for all tablets in ${tableName}
        def tablets = sql_return_maparray """ show tablets from ${testTable}; """
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId
            (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
        }

        thread.join()
        assertFalse(error)
    } catch (Exception e){
        logger.info(e.getMessage())
        assertFalse(true)
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("VerticalSegmentWriter._append_block_with_partial_content.sleep")
        GetDebugPoint().disableDebugPointForAllBEs("SizeBaseCumulativeCompactionPolicy.pick_input_rowsets.return_input_rowsets")
    }
}
