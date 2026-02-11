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

suite("test_file_cache_info") {
    def custoBeConfig = [
        enable_evict_file_cache_in_advance : false,
        file_cache_enter_disk_resource_limit_mode_percent : 99
    ]

    setBeConfigTemporary(custoBeConfig) {

    String[][] backends = sql """ show backends """
    def backendSockets = []
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    for (String[] backend in backends) {
        if (backend[9].equals("true")) {
            backendIdToBackendIP.put(backend[0], backend[1])
            backendIdToBackendHttpPort.put(backend[0], backend[4])
        }
    }
    assertTrue(backendIdToBackendIP.size() > 0, "No alive backends found")

    backendIdToBackendIP.each { backendId, ip ->
        def socket = ip + ":" + backendIdToBackendHttpPort.get(backendId)
        backendSockets.add(socket)
    }

    sql "drop table IF EXISTS customer"

    sql """
        CREATE TABLE IF NOT EXISTS customer (
            `c_custkey` int NULL,
            `c_name` string NULL,
            `c_address` string NULL,
            `c_city` string NULL,
            `c_nation` string NULL,
            `c_region` string NULL,
            `c_phone` string NULL,
            `c_mktsegment` string NULL
        )
        DUPLICATE KEY(`c_custkey`)
        DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 1
        PROPERTIES (
            "file_cache_ttl_seconds" = "3600"
        )
    """

    sql """
        insert into customer values
        (1, 'Customer#000000001', 'address1', 'city1', 'nation1', 'region1', 'phone1', 'segment1'),
        (2, 'Customer#000000002', 'address2', 'city2', 'nation2', 'region2', 'phone2', 'segment2'),
        (3, 'Customer#000000003', 'address3', 'city3', 'nation3', 'region3', 'phone3', 'segment3'),
        (4, 'Customer#000000004', 'address4', 'city4', 'nation4', 'region4', 'phone4', 'segment4'),
        (5, 'Customer#000000005', 'address5', 'city5', 'nation5', 'region5', 'phone5', 'segment5')
    """
    sql "sync"

    sql "select count(*) from customer"

    Thread.sleep(10000)

    def get_tablet_id = { String tbl_name ->
        def tablets = sql "show tablets from ${tbl_name}"
        assertEquals(tablets.size(), 1, "Should have exactly one tablet with BUCKETS=1")
        return tablets[0][0] as Long
    }

    def tablet_id = get_tablet_id("customer")
    println "Tablet ID: ${tablet_id}"

    def desc_cache_info = sql "desc information_schema.file_cache_info"
    assertTrue(desc_cache_info.size() > 0, "desc information_schema.file_cache_info should not be empty")
    assertEquals(desc_cache_info[0][0].toString().toUpperCase(), "HASH")
    assertEquals(desc_cache_info[1][0].toString().toUpperCase(), "OFFSET")

    def cache_info = sql "select * from information_schema.file_cache_info"
    
    assertTrue(cache_info.size() > 0, "file_cache_info should not be empty for tablet_id ${tablet_id}")
    
    println "First query - File cache info for tablet_id ${tablet_id}:"
    cache_info.each { row ->
        println "  ${row}"
    }

    def clearResults = []
    backendSockets.each { socket ->
        httpTest {
            endpoint ""
            uri socket + "/api/file_cache?op=clear&sync=true"
            op "get"
            check {respCode, body ->
                assertEquals(respCode, 200, "clear local cache fail, maybe you can find something in respond: " + parseJson(body))
                clearResults.add(true)
            }
        }
    }
    assertEquals(clearResults.size(), backendSockets.size(), "Failed to clear cache on some backends")

    Thread.sleep(5000)

    def cache_info_after_clear = sql "select * from information_schema.file_cache_info where tablet_id = ${tablet_id}"
    assertEquals(cache_info_after_clear.size(), 0, "file_cache_info should be empty after clearing cache")

    println "After clearing cache - File cache info is empty as expected"

    sql "select * from customer"

    Thread.sleep(10000)

    def cache_info_reloaded = sql "select * from information_schema.file_cache_info where tablet_id = ${tablet_id}"
    assertTrue(cache_info_reloaded.size() > 0, "file_cache_info should not be empty after reloading data")

    println "After reloading data - File cache info for tablet_id ${tablet_id}:"
    cache_info_reloaded.each { row ->
        println "  ${row}"
    }

    }
}

