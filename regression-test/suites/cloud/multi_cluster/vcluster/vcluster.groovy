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

suite("vcluster") {
    if (isCloudMode()) return

    def token = context.config.metaServiceToken
    def instance_id = context.config.multiClusterInstance
    String tableName = "test_all_vcluster"

    List<String> ipList = new ArrayList<>()
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()
    List<String> bePortList = new ArrayList<>()

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
            log.info("lw test drop : ${cluster.type} ".toString())
            if (cluster.type == "COMPUTE" || cluster.type == "VIRTUAL") {
                drop_cluster.call(cluster.cluster_name, cluster.cluster_id);
            }
        }
    }
    wait_cluster_change()

    List<List<Object>> result  = sql "show clusters"
    assertTrue(result.size() == 0);

    add_cluster.call(beUniqueIdList[0], ipList[0], hbPortList[0],
                     "regression_cluster_name0", "regression_cluster_id0");
    add_cluster.call(beUniqueIdList[1], ipList[1], hbPortList[1],
                     "regression_cluster_name1", "regression_cluster_id1");
    add_vcluster.call("regression_vcluster_name0", "regression_vcluster_id0",
                     "regression_cluster_name1", "regression_cluster_name0");
    wait_cluster_change()

    result  = sql "show clusters"
    assertTrue(result.size() == 3);

    for (row : result) {
        println row
    }

    try {
        sql """ use @regression_vcluster_name0 """
        sql """ drop table if exists ${tableName} """

        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
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
            table "${tableName}"

            set 'column_separator', ','
            set 'cloud_cluster', 'regression_vcluster_name0'

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
        order_qt_all11 "SELECT count(*) FROM ${tableName}" // 20
        order_qt_all12 "SELECT count(*) FROM ${tableName} where k1 <= 10"  // 11

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

        // fill bePortList
        for (int i = 0; i < ipList.size(); ++i) {
            result = sql """show backends"""
            for (row : result) {
                println row
                println row[2]
                if (ipList[i] == row[1] && hbPortList[i] == row[2]) {
                    bePortList.add(row[5]);
                }
            }
        }

        set = [ipList[1] + ":" +bePortList[1]] as Set
        sql """ select count(k2) AS theCount, k3 from test_all_vcluster group by k3 order by theCount limit 1 """
        checkProfileNew.call(set)
        //checkProfileNew1.call("e329bc41f42c49f5-9326cb8429dacc06")
    } finally {
        sql """ drop table if exists ${tableName} """
    }
}
