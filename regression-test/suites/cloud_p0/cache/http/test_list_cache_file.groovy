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

suite("test_list_cache_file") {
    sql """ use @regression_cluster_name1 """
    String[][] backends = sql """ show backends """
    String backendId;
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    def backendIdToBackendBrpcPort = [:]
    for (String[] backend in backends) {
        if (backend[9].equals("true") && backend[19].contains("regression_cluster_name1")) {
            backendIdToBackendIP.put(backend[0], backend[1])
            backendIdToBackendHttpPort.put(backend[0], backend[4])
            backendIdToBackendBrpcPort.put(backend[0], backend[5])
        }
    }
    assertEquals(backendIdToBackendIP.size(), 1)

    backendId = backendIdToBackendIP.keySet()[0]
    def socket = backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendHttpPort.get(backendId)

    sql "drop table IF EXISTS `user`"

    sql """
        CREATE TABLE IF NOT EXISTS `user` (
            `id` int NULL,
            `name` string NULL
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "file_cache_ttl_seconds" = "2884"
        )
    """

    sql "insert into user select number, cast(rand() as varchar(32)) from numbers(\"number\"=\"1000000\")"

    def get_tablets = { String tbl_name ->
        def res = sql "show tablets from ${tbl_name}"
        List<Integer> tablets = new ArrayList<>()
        for (final def line in res) {
            tablets.add(Integer.valueOf(line[0].toString()))
        }
        return tablets
    }

    def get_rowsets = { int tablet_id ->
        var ret = []
        httpTest {
            endpoint ""
            uri socket + "/api/compaction/show?tablet_id=" + tablet_id
            op "get"
            check {respCode, body ->
                assertEquals(respCode, 200)
                var map = parseJson(body)
                for (final def line in map.get("rowsets")) {
                    var tokens = line.toString().split(" ")
                    ret.add(tokens[4])
                }
            }
        }
        return ret
    }

    var tablets = get_tablets("user")
    var rowsets = get_rowsets(tablets.get(0))
    var segment_file = rowsets[rowsets.size() - 1] + "_0.dat"

    httpTest {
        endpoint ""
        uri socket + "/api/file_cache?op=list_cache&value=" + segment_file
        op "get"
        check {respCode, body ->
            assertEquals(respCode, 200)
            var arr = parseJson(body)
            assertTrue(arr.size() > 0, "There shouldn't be no cache file at all, maybe you need to check disk capacity and modify file_cache_enter_disk_resource_limit_mode_percent in be.conf")
        }
    }

    // clear single segment file cache
    httpTest {
        endpoint ""
        uri socket + "/api/file_cache?op=clear&value=" + segment_file
        op "get"
        check {respCode, body ->
            assertEquals(respCode, 200, "clear local cache fail, maybe you can find something in respond: " + parseJson(body))
        }
    }

    httpTest {
        endpoint ""
        uri socket + "/api/file_cache?op=list_cache&value=" + segment_file
        op "get"
        check {respCode, body ->
            assertEquals(respCode, 200)
            var arr = parseJson(body)
            assertTrue(arr.size() == 0, "local cache files should not greater than 0, because it has already clear")
        }
    }
}
