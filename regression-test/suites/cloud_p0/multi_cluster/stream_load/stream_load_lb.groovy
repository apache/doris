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

import groovy.json.JsonOutput

suite("stream_load_lb") {
    def tableName3 = "test_all_lb"

    try {
    List<String> ipList = new ArrayList<>()
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()

    String[] bes = context.config.multiClusterBes.split(',');
    println("the value is " + context.config.multiClusterBes);
    for(String values : bes) {
        println("the value is " + values);
        String[] beInfo = values.split(':');
        ipList.add(beInfo[0]);
        hbPortList.add(beInfo[1]);
        httpPortList.add(beInfo[2]);
        beUniqueIdList.add(beInfo[3]);
    }

    println("the ip is " + ipList);
    println("the heartbeat port is " + hbPortList);
    println("the http port is " + httpPortList);
    println("the be unique id is " + beUniqueIdList);

    for (unique_id : beUniqueIdList) {
        resp = get_cluster.call(unique_id);
        for (cluster : resp) {
            if (cluster.type == "COMPUTE") {
                drop_cluster.call(cluster.cluster_name, cluster.cluster_id);
            }
        }
    }
    sleep(20000)

    List<List<Object>> result  = sql "show clusters"
    assertTrue(result.size() == 0);

    addClusterLb(beUniqueIdList[0], ipList[0], hbPortList[0],
                 "stream_load_cluster_name0", "stream_load_cluster_id0",
                 ipList[0] + ":" + httpPortList[0], ":" + httpPortList[1]);

    addClusterLb(beUniqueIdList[1], ipList[1], hbPortList[1],
                 "stream_load_cluster_name1", "stream_load_cluster_id1",
                 ipList[0] + ":" + httpPortList[0], ":" + httpPortList[1]);
    sleep(20000)

    result  = sql "show clusters"
    assertTrue(result.size() == 2);

    for (row : result) {
        println row
    }

    sql "SET PROPERTY 'default_cloud_cluster' = ''"
    sql """ use @stream_load_cluster_name0 """

    sql """ drop table if exists ${tableName3} """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName3} (
      `k1` int(11) NULL,
      `k2` tinyint(4) NULL,
      `k3` smallint(6) NULL,
      `k4` bigint(20) NULL,
      `k5` largeint(40) NULL,
      `k6` float NULL,
      `k7` double NULL,
      `k8` decimal(9, 0) NULL,
      `k9` char(10) NULL,
      `k10` varchar(1024) NULL,
      `k11` text NULL,
      `k12` date NULL,
      `k13` datetime NULL
    ) ENGINE=OLAP
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    ;
    """

    sql """ set enable_profile = true """

    // case1 cloud public endpoint
    long txnId = -1;
    streamLoad {
        table "${tableName3}"

        set 'column_separator', ','
        set 'cloud_cluster', 'stream_load_cluster_name1'
        set 'Host', "${ipList[0]}"

        file 'all_types.csv'
        time 10000 // limit inflight 10s

        check { loadResult, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${loadResult}".toString())
            def json = parseJson(loadResult)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(20, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            txnId = json.TxnId
        }
    }

    order_qt_q1 "SELECT count(*) FROM ${tableName3}" // 20
    order_qt_q2 "SELECT count(*) FROM ${tableName3} where k1 <= 10"  // 11

    // case2 cloud private endpoint
    streamLoad {
        table "${tableName3}"

        set 'column_separator', ','
        set 'cloud_cluster', 'stream_load_cluster_name1'
        set 'Host', "${ipList[1]}"

        file 'all_types.csv'
        time 10000 // limit inflight 10s

        check { loadResult, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
                assertEquals
            }
            log.info("Stream load result: ${loadResult}".toString())
            def json = parseJson(loadResult)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(20, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
            txnId = json.TxnId
        }
    }

    order_qt_q3 "SELECT count(*) FROM ${tableName3}" // 20
    order_qt_q4 "SELECT count(*) FROM ${tableName3} where k1 <= 10"  // 11

    updateClusterEndpoint("stream_load_cluster_id0", "xx:xx", "xx:xx")

    updateClusterEndpoint("stream_load_cluster_id1", ipList[1] + ":" + httpPortList[1],
            ipList[0] + ":" + httpPortList[0])

    sleep(30000)

    try {
        setFeConfig('apsaradb_env_enabled', true)

        // case4 apsaradb public endpoint
        streamLoad {
            table "${tableName3}"

            set 'column_separator', ','
            set 'cloud_cluster', 'stream_load_cluster_name1'
            set 'Host', 'xxx-public-xxx'

            file 'all_types.csv'
            time 10000 // limit inflight 10s

            check { loadResult, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${loadResult}".toString())
                def json = parseJson(loadResult)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(20, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
                txnId = json.TxnId
            }
        }

        order_qt_q7 "SELECT count(*) FROM ${tableName3}" // 20
        order_qt_q8 "SELECT count(*) FROM ${tableName3} where k1 <= 10"  // 11

        // case5 apsaradb private endpoint
        streamLoad {
            table "${tableName3}"

            set 'column_separator', ','
            set 'cloud_cluster', 'stream_load_cluster_name1'
            set 'Host', 'xxxx-xxxxx'

            file 'all_types.csv'
            time 10000 // limit inflight 10s

            check { loadResult, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${loadResult}".toString())
                def json = parseJson(loadResult)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(20, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
                txnId = json.TxnId
            }
        }

        order_qt_q9 "SELECT count(*) FROM ${tableName3}" // 20
        order_qt_q10 "SELECT count(*) FROM ${tableName3} where k1 <= 10"  // 11
    } finally {
        setFeConfig('apsaradb_env_enabled', false)
    }
    } finally {
        sql """ drop table if exists ${tableName3} """
    }
}

