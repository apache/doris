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

suite("stream_load") {
    withRestoredMultiClusterState(false) {
    // case1 specific cluster
    def tableName3 = "test_all"
    def tableName4 = "test_all_default_cluster"
    try {
    // Routing assertions need two known BEs, not case-specific compute groups.
    def baseline = multiClusterBaseline()
    checkMultiClusterBaseline(baseline)
    def ipList = baseline.collect { it.nodes[0].ip }
    def httpPortList = baseline.collect { it.nodes[0].http_port.toString() }

    sql "SET PROPERTY 'default_cloud_cluster' = ''"
    sql """ use @${baseline[0].cluster_name} """


    

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

    def before_cluster0_load_rows = get_be_metric(ipList[0], httpPortList[0], "load_rows");
    log.info("before_cluster0_load_rows : ${before_cluster0_load_rows}".toString())
    def before_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    log.info("before_cluster0_flush : ${before_cluster0_flush}".toString())

    def before_cluster1_load_rows = get_be_metric(ipList[1], httpPortList[1], "load_rows");
    log.info("before_cluster1_load_rows : ${before_cluster1_load_rows}".toString())
    def before_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    log.info("before_cluster1_flush : ${before_cluster1_flush}".toString())

    long txnId = -1;
    streamLoad {
        table "${tableName3}"

        set 'column_separator', ','
        set 'cloud_cluster', baseline[1].cluster_name

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
    sql "sync"
    order_qt_all11 "SELECT count(*) FROM ${tableName3}" // 20
    order_qt_all12 "SELECT count(*) FROM ${tableName3} where k1 <= 10"  // 11

    def after_cluster0_load_rows = get_be_metric(ipList[0], httpPortList[0], "load_rows");
    log.info("after_cluster0_load_rows : ${after_cluster0_load_rows}".toString())
    def after_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    log.info("after_cluster0_flush : ${after_cluster0_flush}".toString())

    def after_cluster1_load_rows = get_be_metric(ipList[1], httpPortList[1], "load_rows");
    log.info("after_cluster1_load_rows : ${after_cluster1_load_rows}".toString())
    def after_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    log.info("after_cluster1_flush : ${after_cluster1_flush}".toString())

    assertTrue(before_cluster0_load_rows == after_cluster0_load_rows)
    assertTrue(before_cluster0_flush == after_cluster0_flush)

    assertTrue(before_cluster1_load_rows < after_cluster1_load_rows)
    assertTrue(before_cluster1_flush < after_cluster1_flush)

    /*
    // case2 not specific cluster
    tableName3 = "test_all_no_specific_cluster"
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

    before_cluster0_load_rows = get_be_metric(ipList[0], httpPortList[0], "load_rows");
    log.info("before_cluster0_load_rows : ${before_cluster0_load_rows}".toString())
    before_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    log.info("before_cluster0_flush : ${before_cluster0_flush}".toString())

    before_cluster1_load_rows = get_be_metric(ipList[1], httpPortList[1], "load_rows");
    log.info("before_cluster1_load_rows : ${before_cluster1_load_rows}".toString())
    before_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    log.info("before_cluster1_flush : ${before_cluster1_flush}".toString())

    txnId = -1;
    streamLoad {
        table "${tableName3}"

        set 'column_separator', ','

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
    order_qt_all11 "SELECT count(*) FROM ${tableName3}" // 20
    order_qt_all12 "SELECT count(*) FROM ${tableName3} where k1 <= 10"  // 11

    after_cluster0_load_rows = get_be_metric(ipList[0], httpPortList[0], "load_rows");
    log.info("after_cluster0_load_rows : ${after_cluster0_load_rows}".toString())
    after_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    log.info("after_cluster0_flush : ${after_cluster0_flush}".toString())

    after_cluster1_load_rows = get_be_metric(ipList[1], httpPortList[1], "load_rows");
    log.info("after_cluster1_load_rows : ${after_cluster1_load_rows}".toString())
    after_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    log.info("after_cluster1_flush : ${after_cluster1_flush}".toString())

    if (before_cluster0_load_rows == after_cluster0_load_rows) {
        assertTrue(before_cluster0_flush == after_cluster0_flush)
    }

    if (before_cluster0_load_rows < after_cluster0_load_rows) {
        assertTrue(before_cluster0_flush < after_cluster0_flush)
    }

    if (before_cluster1_load_rows == after_cluster1_load_rows) {
        assertTrue(before_cluster1_flush == after_cluster1_flush)
    }

    if (before_cluster1_load_rows < after_cluster1_load_rows) {
        assertTrue(before_cluster1_flush < after_cluster1_flush)
    }
    */

    // case3 default cluster
    sql "SET PROPERTY 'default_cloud_cluster' = '${baseline[0].cluster_name}'"
    sql """ drop table if exists ${tableName4} """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName4} (
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

    before_cluster0_load_rows = get_be_metric(ipList[0], httpPortList[0], "load_rows");
    log.info("before_cluster0_load_rows : ${before_cluster0_load_rows}".toString())
    before_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    log.info("before_cluster0_flush : ${before_cluster0_flush}".toString())

    before_cluster1_load_rows = get_be_metric(ipList[1], httpPortList[1], "load_rows");
    log.info("before_cluster1_load_rows : ${before_cluster1_load_rows}".toString())
    before_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    log.info("before_cluster1_flush : ${before_cluster1_flush}".toString())

    txnId = -1;
    streamLoad {
        table "${tableName4}"

        set 'column_separator', ','

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
    sql "sync"
    order_qt_all11 "SELECT count(*) FROM ${tableName4}" // 20
    order_qt_all12 "SELECT count(*) FROM ${tableName4} where k1 <= 10"  // 11

    after_cluster0_load_rows = get_be_metric(ipList[0], httpPortList[0], "load_rows");
    log.info("after_cluster0_load_rows : ${after_cluster0_load_rows}".toString())
    after_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    log.info("after_cluster0_flush : ${after_cluster0_flush}".toString())

    after_cluster1_load_rows = get_be_metric(ipList[1], httpPortList[1], "load_rows");
    log.info("after_cluster1_load_rows : ${after_cluster1_load_rows}".toString())
    after_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    log.info("after_cluster1_flush : ${after_cluster1_flush}".toString())

    assertTrue(before_cluster0_load_rows < after_cluster0_load_rows)
    assertTrue(before_cluster0_flush < after_cluster0_flush)

    assertTrue(before_cluster1_load_rows == after_cluster1_load_rows)
    assertTrue(before_cluster1_flush == after_cluster1_flush)

    // case4 FE redirects HTTP stream by compute_group and planning follows the receiving backend
    before_cluster0_load_rows = get_be_metric(ipList[0], httpPortList[0], "load_rows");
    log.info("before_cluster0_load_rows : ${before_cluster0_load_rows}".toString())
    before_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    log.info("before_cluster0_flush : ${before_cluster0_flush}".toString())

    before_cluster1_load_rows = get_be_metric(ipList[1], httpPortList[1], "load_rows");
    log.info("before_cluster1_load_rows : ${before_cluster1_load_rows}".toString())
    before_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    log.info("before_cluster1_flush : ${before_cluster1_flush}".toString())

    streamLoad {
        set 'version', '1'
        set 'sql', """
                insert into ${context.dbName}.${tableName4}
                select * from http_stream("format"="csv", "column_separator"=",")
                """
        set 'compute_group', baseline[1].cluster_name

        file 'all_types.csv'
        time 10000 // limit inflight 10s

        check { loadResult, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("HTTP stream load result: ${loadResult}".toString())
            def json = parseJson(loadResult)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(20, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    order_qt_http_stream_compute_group "SELECT count(*) FROM ${tableName4}"

    after_cluster0_load_rows = get_be_metric(ipList[0], httpPortList[0], "load_rows");
    log.info("after_cluster0_load_rows : ${after_cluster0_load_rows}".toString())
    after_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    log.info("after_cluster0_flush : ${after_cluster0_flush}".toString())

    after_cluster1_load_rows = get_be_metric(ipList[1], httpPortList[1], "load_rows");
    log.info("after_cluster1_load_rows : ${after_cluster1_load_rows}".toString())
    after_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    log.info("after_cluster1_flush : ${after_cluster1_flush}".toString())

    assertTrue(before_cluster0_load_rows == after_cluster0_load_rows)
    assertTrue(before_cluster0_flush == after_cluster0_flush)

    assertTrue(before_cluster1_load_rows < after_cluster1_load_rows)
    assertTrue(before_cluster1_flush < after_cluster1_flush)

    // case5 direct-to-BE stream load ignores compute_group and uses the receiving backend's compute group
    before_cluster0_load_rows = get_be_metric(ipList[0], httpPortList[0], "load_rows");
    log.info("before_cluster0_load_rows : ${before_cluster0_load_rows}".toString())
    before_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    log.info("before_cluster0_flush : ${before_cluster0_flush}".toString())

    before_cluster1_load_rows = get_be_metric(ipList[1], httpPortList[1], "load_rows");
    log.info("before_cluster1_load_rows : ${before_cluster1_load_rows}".toString())
    before_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    log.info("before_cluster1_flush : ${before_cluster1_flush}".toString())

    streamLoad {
        table "${tableName4}"
        directToBe ipList[0], httpPortList[0].toInteger()

        set 'column_separator', ','
        set 'compute_group', baseline[1].cluster_name

        file 'all_types.csv'
        time 10000 // limit inflight 10s

        check { loadResult, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Direct-to-BE stream load result: ${loadResult}".toString())
            def json = parseJson(loadResult)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(20, json.NumberTotalRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "sync"
    order_qt_direct_be_compute_group "SELECT count(*) FROM ${tableName4}"

    after_cluster0_load_rows = get_be_metric(ipList[0], httpPortList[0], "load_rows");
    log.info("after_cluster0_load_rows : ${after_cluster0_load_rows}".toString())
    after_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    log.info("after_cluster0_flush : ${after_cluster0_flush}".toString())

    after_cluster1_load_rows = get_be_metric(ipList[1], httpPortList[1], "load_rows");
    log.info("after_cluster1_load_rows : ${after_cluster1_load_rows}".toString())
    after_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    log.info("after_cluster1_flush : ${after_cluster1_flush}".toString())

    assertTrue(before_cluster0_load_rows < after_cluster0_load_rows)
    assertTrue(before_cluster0_flush < after_cluster0_flush)

    assertTrue(before_cluster1_load_rows == after_cluster1_load_rows)
    assertTrue(before_cluster1_flush == after_cluster1_flush)
    } finally {
    sql "SET PROPERTY 'default_cloud_cluster' = ''"
    sql """ drop table if exists ${tableName3} """
    sql """ drop table if exists ${tableName4} """
    }
    }
}

