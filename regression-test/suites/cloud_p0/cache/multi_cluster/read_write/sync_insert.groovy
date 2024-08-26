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

suite("sync_insert") {
    List<String> ipList = new ArrayList<>();
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> brpcPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()

    String[] bes = context.config.multiClusterBes.split(',');
    println("the value is " + context.config.multiClusterBes);
    int num = 0
    for(String values : bes) {
        if (num++ == 2) break;
        println("the value is " + values);
        String[] beInfo = values.split(':');
        ipList.add(beInfo[0]);
        hbPortList.add(beInfo[1]);
        httpPortList.add(beInfo[2]);
        beUniqueIdList.add(beInfo[3]);
        brpcPortList.add(beInfo[4]);
    }

    println("the ip is " + ipList);
    println("the heartbeat port is " + hbPortList);
    println("the http port is " + httpPortList);
    println("the be unique id is " + beUniqueIdList);
    println("the brpc port is " + brpcPortList);

    def clearFileCache = { ip, port ->
        httpTest {
            endpoint ""
            uri ip + ":" + port + """/api/file_cache?op=clear&sync=true"""
            op "get"
            body ""
        }
    }

    def getMetricsMethod = { ip, port, check_func ->
        httpTest {
            endpoint ip + ":" + port
            uri "/brpc_metrics"
            op "get"
            check check_func
        }
    }

    def table1 = "test_dup_tab_basic_int_tab_nullable"
    sql """ drop table if exists ${table1} """

    // clear all file cache on all BEs of all clusters
    clearFileCache.call(ipList[0], httpPortList[0]);
    clearFileCache.call(ipList[1], httpPortList[1]);

    sql """
CREATE TABLE IF NOT EXISTS `${table1}` (
  `siteid` int(11) NULL COMMENT "",
  `citycode` int(11) NULL COMMENT "",
  `userid` int(11) NULL COMMENT "",
  `pv` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`siteid`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`siteid`) BUCKETS 1
"""

    long insertedSizeBytes = 795
    connect('root') {
        sql "use @regression_cluster_name0"
        sql "use regression_test_cloud_p0_cache_multi_cluster_read_write"
        sql """ set enable_multi_cluster_sync_load = true """
        // all the inserted values will result in insertedSizeBytes bytes of size
        sql """insert into test_dup_tab_basic_int_tab_nullable values
            (9,10,11,12),
            (9,10,11,12),
            (21,null,23,null),
            (1,2,3,4),
            (1,2,3,4),
            (13,14,15,16),
            (13,21,22,16),
            (13,14,15,16),
            (13,21,22,16),
            (17,18,19,20),
            (17,18,19,20),
            (null,21,null,23),
            (22,null,24,25),
            (26,27,null,29),
            (5,6,7,8),
            (5,6,7,8)
        """
        sql """ set enable_multi_cluster_sync_load = false """
    }

    sleep(30000) // wait for download from remote

    // get insert size of first cluster, may contain system tables
    long srcClusterSize = 0
    getMetricsMethod.call(ipList[0], brpcPortList[0]) {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag = false;
            for (String line in strs) {
                if (line.contains("file_cache_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    srcClusterSize = line.substring(i).toLong()
                    flag = true
                    break
                }
            }
            assertTrue(flag)
    }

    // get synced insert size of first cluster
    long dstClusterSize = 0
    getMetricsMethod.call(ipList[1], brpcPortList[1]) {
        respCode, body ->
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            def strs = out.split('\n')
            Boolean flag = false;
            for (String line in strs) {
                if (line.contains("file_cache_cache_size")) {
                    if (line.startsWith("#")) {
                        continue
                    }
                    def i = line.indexOf(' ')
                    dstClusterSize = line.substring(i).toLong()
                    flag = true
                    break
                }
            }
            assertTrue(flag)
    }
    // FIXME(gavin): this is a strong assertion, make it weaker and robuster
    assertEquals(insertedSizeBytes, dstClusterSize)
    org.junit.Assert.assertTrue("insertedSizeBytes ${insertedSizeBytes} <= dstClusterSize ${dstClusterSize}", insertedSizeBytes <= dstClusterSize)
    org.junit.Assert.assertTrue("insertedSizeBytes ${insertedSizeBytes} <= srcClusterSize ${dstClusterSize}", insertedSizeBytes <= srcClusterSize)
    org.junit.Assert.assertTrue("dstClusterSize ${insertedSizeBytes} <= srcClusterSize ${dstClusterSize}", dstClusterSize <= srcClusterSize)
    sql "drop table if exists ${table1}"
}
