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

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

enum FileSizeChange {
    LARGER,
    SMALLER,
    UNCHANGED
}

suite("test_show_index_data_p2", "p2") {

    def show_table_name = "test_show_index_data_p2"

    def load_json_data = { table_name, file_name ->
        // load the json data
        streamLoad {
            table "${table_name}"

            // set http request header params
            set 'read_json_by_line', 'true' 
            set 'format', 'json' 
            set 'max_filter_ratio', '0.1'
            file file_name // import json file
            time 10000 // limit inflight 10s

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                        throw exception
                }
                logger.info("Stream load ${file_name} result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                // assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    

    def convert_size = { str ->
        def matcher = str =~ /(\d+(\.\d+)?)\s*(KB|MB|GB|TB|B)/
        if (matcher) {
            def value = matcher[0][1] as double
            def unit = matcher[0][3]
            logger.info("value is: " + value + " unit is: " + unit)
            def result = 0.0
            switch (unit) {
                case 'KB':
                    result = value * 1024
                    break
                case 'MB':
                    result = value * 1024 * 1024 
                    break
                case 'GB':
                    result = value * 1024 * 1024 * 1024
                    break
                case 'B':
                    result = value
                    break
                default:
                    throw new IllegalArgumentException("Unknown unit: $unit")
            }
            
            return result
        } else {
           return 0
        }
    }

    sql "DROP TABLE IF EXISTS ${show_table_name}"
    sql """ 
        CREATE TABLE ${show_table_name} (
            `@timestamp` int(11) NULL,
            `clientip` varchar(20) NULL,
            `request` varchar(500) NULL,
            `status` int NULL,
            `size` int NULL,
            INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
            INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "unicode") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`@timestamp`, `clientip`)
        DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "compaction_policy" = "time_series",
            "time_series_compaction_file_count_threshold" = "20",
            "disable_auto_compaction" = "true"
        );
    """

    def compaction = {
        def tablets = sql_return_maparray """ show tablets from ${show_table_name}; """
        for (def tablet in tablets) {
            int beforeSegmentCount = 0
            String tablet_id = tablet.TabletId
            (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            for (String rowset in (List<String>) tabletJson.rowsets) {
                beforeSegmentCount += Integer.parseInt(rowset.split(" ")[1])
            }
            assertEquals(beforeSegmentCount, 110)
        }

        // trigger compactions for all tablets in ${tableName}
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId
            (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactJson = parseJson(out.trim())
            assertEquals("success", compactJson.status.toLowerCase())
        }

        // wait for all compactions done
        for (def tablet in tablets) {
            Awaitility.await().atMost(60, TimeUnit.MINUTES).untilAsserted(() -> {
                Thread.sleep(30000)
                String tablet_id = tablet.TabletId
                backend_id = tablet.BackendId
                (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("compaction task for this tablet is not running", compactionStatus.msg.toLowerCase())
            });
        }

        for (def tablet in tablets) {
            int afterSegmentCount = 0
            String tablet_id = tablet.TabletId
            (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            for (String rowset in (List<String>) tabletJson.rowsets) {
                logger.info("rowset is: " + rowset)
                afterSegmentCount += Integer.parseInt(rowset.split(" ")[1])
            }
            assertEquals(afterSegmentCount, 1)
        }
        
    }

    double localIndexSize = 0
    double localSegmentSize = 0

    def check_size_equal = { double result1, double result2 ->
        double tolerance = 0.1 * Math.max(result1, result2);
        return Math.abs(result1 - result2) <= tolerance;
    }

    def check_show_data = { FileSizeChange expect_idx, FileSizeChange expect_data ->
        Thread.sleep(90000)
        Awaitility.await().atMost(10, TimeUnit.MINUTES).untilAsserted(() -> {
            Thread.sleep(10000)
            def result = sql """ show data all;"""
            logger.info("show data all; result is: ${result}")
            def currentLocalIndexSize = convert_size.call(result[0][4])
            def currentSegmentIndexSize = convert_size.call(result[0][3])

            if (expect_idx == FileSizeChange.LARGER) {
                assertTrue(currentLocalIndexSize > localIndexSize)
            } else if (expect_idx == FileSizeChange.SMALLER) {
                assertTrue(currentLocalIndexSize < localIndexSize)
            } else {
                assertTrue(check_size_equal(currentLocalIndexSize, localIndexSize))
            }

            if (expect_data == FileSizeChange.LARGER) {
                assertTrue(currentSegmentIndexSize > localSegmentSize)
            } else if (expect_data == FileSizeChange.SMALLER) {
                assertTrue(currentSegmentIndexSize < localSegmentSize)
            } else {
                assertTrue(check_size_equal(currentSegmentIndexSize, localSegmentSize))
            }

            assertTrue(currentLocalIndexSize != 0)
            assertTrue(currentSegmentIndexSize != 0)
            localIndexSize = currentLocalIndexSize
            localSegmentSize = currentSegmentIndexSize

            def result2 = sql """ select * from information_schema.tables where TABLE_NAME = '${show_table_name}' """
            logger.info("result 2 is: ${result2}")
            def currentLocalIndexSize2 = result2[0][11] as double
            def currentSegmentIndexSize2 = result2[0][9] as double
            logger.info("currentLocalIndexSize2 is: ${currentLocalIndexSize2}, currentSegmentIndexSize2 is: ${currentSegmentIndexSize2}")
            assertTrue(check_size_equal(currentLocalIndexSize, currentLocalIndexSize2))
            assertTrue(check_size_equal(currentSegmentIndexSize, currentSegmentIndexSize2))
            logger.info("show data all localIndexSize is: " + localIndexSize)
            logger.info("show data all localSegmentSize is: " + localSegmentSize)
        });
    }

    def schema_change = {
        def tablets = sql_return_maparray """ show tablets from ${show_table_name}; """
        Set<String> rowsetids = new HashSet<>();
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            for (String rowset in (List<String>) tabletJson.rowsets) {
                int segmentCount = Integer.parseInt(rowset.split(" ")[1])
                if (segmentCount == 0) {
                    continue;
                }
                String rowsetid = rowset.split(" ")[4];
                rowsetids.add(rowsetid)
                logger.info("rowsetid: " + rowsetid)
            }
        }
        sql """ alter table ${show_table_name} drop column clientip"""
        Awaitility.await().atMost(60, TimeUnit.MINUTES).untilAsserted(() -> {
            Thread.sleep(30000)
            tablets = sql_return_maparray """ show tablets from ${show_table_name}; """
            for (def tablet in tablets) {
                String tablet_id = tablet.TabletId
                (code, out, err) = curl("GET", tablet.CompactionStatus)
                logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def tabletJson = parseJson(out.trim())
                assert tabletJson.rowsets instanceof List
                for (String rowset in (List<String>) tabletJson.rowsets) {
                    int segmentCount = Integer.parseInt(rowset.split(" ")[1])
                    if (segmentCount == 0) {
                        continue;
                    }
                    String rowsetid = rowset.split(" ")[4];
                    logger.info("rowsetid: " + rowsetid)
                    assertTrue(!rowsetids.contains(rowsetid))
                }
            }
        });
    }

    def build_index = {
        def tablets = sql_return_maparray """ show tablets from ${show_table_name}; """
        Set<String> rowsetids = new HashSet<>();
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            for (String rowset in (List<String>) tabletJson.rowsets) {
                int segmentCount = Integer.parseInt(rowset.split(" ")[1])
                if (segmentCount == 0) {
                    continue;
                }
                String rowsetid = rowset.split(" ")[4];
                rowsetids.add(rowsetid)
                logger.info("rowsetid: " + rowsetid)
            }
        }
        sql """ ALTER TABLE ${show_table_name} ADD INDEX status_idx (status) using inverted; """
        if (!isCloudMode()) {
            sql """ build index status_idx on ${show_table_name}"""
        }
        Awaitility.await().atMost(60, TimeUnit.MINUTES).untilAsserted(() -> {
            Thread.sleep(30000)
            tablets = sql_return_maparray """ show tablets from ${show_table_name}; """
            for (def tablet in tablets) {
                String tablet_id = tablet.TabletId
                (code, out, err) = curl("GET", tablet.CompactionStatus)
                logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def tabletJson = parseJson(out.trim())
                assert tabletJson.rowsets instanceof List
                for (String rowset in (List<String>) tabletJson.rowsets) {
                    int segmentCount = Integer.parseInt(rowset.split(" ")[1])
                    if (segmentCount == 0) {
                        continue;
                    }
                    String rowsetid = rowset.split(" ")[4];
                    logger.info("rowsetid: " + rowsetid)
                    assertTrue(!rowsetids.contains(rowsetid))
                }
            }
        });
    }

    def drop_index = {
        def tablets = sql_return_maparray """ show tablets from ${show_table_name}; """
        Set<String> rowsetids = new HashSet<>();
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            for (String rowset in (List<String>) tabletJson.rowsets) {
                int segmentCount = Integer.parseInt(rowset.split(" ")[1])
                if (segmentCount == 0) {
                    continue;
                }
                String rowsetid = rowset.split(" ")[4];
                rowsetids.add(rowsetid)
                logger.info("rowsetid: " + rowsetid)
            }
        }
        sql """ DROP INDEX status_idx on ${show_table_name}"""
        Awaitility.await().atMost(60, TimeUnit.MINUTES).untilAsserted(() -> {
            Thread.sleep(30000)
            tablets = sql_return_maparray """ show tablets from ${show_table_name}; """
            for (def tablet in tablets) {
                String tablet_id = tablet.TabletId
                (code, out, err) = curl("GET", tablet.CompactionStatus)
                logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def tabletJson = parseJson(out.trim())
                assert tabletJson.rowsets instanceof List
                for (String rowset in (List<String>) tabletJson.rowsets) {
                    int segmentCount = Integer.parseInt(rowset.split(" ")[1])
                    if (segmentCount == 0) {
                        continue;
                    }
                    String rowsetid = rowset.split(" ")[4];
                    logger.info("rowsetid: " + rowsetid)
                    assertTrue(!rowsetids.contains(rowsetid))
                }
            }
        });
    }

    // 1. load data
    def executor = Executors.newFixedThreadPool(5)
    (1..110).each { i ->
        executor.submit {
            def fileName = "documents-" + i + ".json"
            load_json_data.call(show_table_name, """${getS3Url()}/regression/inverted_index_cases/httplogs/${fileName}""")
        }
    }
    executor.shutdown()
    executor.awaitTermination(60, TimeUnit.MINUTES)

    // 2. check show data
    check_show_data.call(FileSizeChange.LARGER, FileSizeChange.LARGER)

    // 3. compaction
    compaction.call()

    // 4. check show data
    check_show_data.call(FileSizeChange.SMALLER, FileSizeChange.LARGER)

    // 5. schema change
    schema_change.call()

    // 6.check show data
    check_show_data.call(FileSizeChange.SMALLER, FileSizeChange.SMALLER)

    // 7. build index
    build_index.call()

    // 8.check show data
    check_show_data.call(FileSizeChange.LARGER, FileSizeChange.UNCHANGED)

    // 9. drop index
    drop_index.call()

    // 10.check show data
    check_show_data.call(FileSizeChange.SMALLER, FileSizeChange.UNCHANGED)
}
