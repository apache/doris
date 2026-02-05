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

import org.apache.doris.regression.suite.ClusterOptions
import groovy.json.JsonSlurper
import org.awaitility.Awaitility;
import static java.util.concurrent.TimeUnit.SECONDS;
import org.apache.doris.regression.util.NodeType

suite('test_read_from_peer', 'docker') {
    if (!isCloudMode()) {
        return;
    }
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1',
        'workload_group_check_interval_ms=1'
    ]
    options.beConfigs += [
        'file_cache_each_block_size=131072',
        // 'sys_log_verbose_modules=*',
        'enable_cache_read_from_peer=true'
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true
    options.enableDebugPoints()
    
    def tableName = "test_read_from_table"

    def clusterBe = { clusterName ->
        def bes = sql_return_maparray "show backends"
        def clusterBes = bes.findAll { be -> be.Tag.contains(clusterName) }
        logger.info("cluster {}, bes {}", clusterName, clusterBes)
        clusterBes[0]
    }

    def testCase = { String clusterName, String runType ->
        def startTime = System.currentTimeMillis()
        GetDebugPoint().enableDebugPointForAllBEs("CachedRemoteFileReader.read_at_impl.change_type", [type: runType])
        
        try {
            sql """
                use @$clusterName
            """

            def be = clusterBe(clusterName)
            def haveCacheBe = clusterBe("compute_cluster")

            switch (runType) {
                case "peer":
                    GetDebugPoint().enableDebugPoint(be.Host, be.HttpPort as int, NodeType.BE, "PeerFileCacheReader::_fetch_from_peer_cache_blocks", 
                        [host: haveCacheBe.Host, port: haveCacheBe.BrpcPort])
                    break
                case "s3":
                    break
                default:
                    throw new IllegalArgumentException("Invalid type: $runType. Expected: peer, s3")
            }
            
            // Execute the query and measure time
            def queryStartTime = System.currentTimeMillis()
            def ret = sql """
                select count(*) from $tableName
            """
            logger.info("select ret={}", ret)
            def queryTime = System.currentTimeMillis() - queryStartTime
            logger.info("Test completed - Type:{}, Cluster: {}, Query execution time: {}ms", runType, clusterName, queryTime)
        } catch (Exception e) {
            def totalTime = System.currentTimeMillis() - startTime
            logger.info("Test failed after {}ms - Type: {}, Cluster: {} Error: {}", totalTime, runType, clusterName, e.message)
            throw e
        }
    }

    docker(options) {
        // 添加一个新的cluster, 只从s3上读
        cluster.addBackend(1, "readS3cluster")

        // 添加一个新的cluster, 只从peer上读
        cluster.addBackend(1, "readPeercluster")

        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                ss_sold_date_sk bigint,
                ss_sold_time_sk bigint,
                ss_item_sk bigint,
                ss_customer_sk bigint,
                ss_cdemo_sk bigint,
                ss_hdemo_sk bigint,
                ss_addr_sk bigint,
                ss_store_sk bigint,
                ss_promo_sk bigint,
                ss_ticket_number bigint,
                ss_quantity integer,
                ss_wholesale_cost decimal(7,2),
                ss_list_price decimal(7,2),
                ss_sales_price decimal(7,2),
                ss_ext_discount_amt decimal(7,2),
                ss_ext_sales_price decimal(7,2),
                ss_ext_wholesale_cost decimal(7,2),
                ss_ext_list_price decimal(7,2),
                ss_ext_tax decimal(7,2),
                ss_coupon_amt decimal(7,2),
                ss_net_paid decimal(7,2),
                ss_net_paid_inc_tax decimal(7,2),
                ss_net_profit decimal(7,2)
            )
            DUPLICATE KEY(ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk)
            DISTRIBUTED BY HASH(ss_customer_sk) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            )
        """

        sql """
            use @compute_cluster
        """

        // in compute_cluster be-1, cache all data in file cache
        def txnId = -1;
        // version 2
        streamLoad {
            table "${tableName}"

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'compress_type', 'GZ'

            file """${getS3Url()}/regression/tpcds/sf1/store_sales.dat.gz"""
            // file """store_sales.dat.gz"""

            time 10000 // limit inflight 10s
            setFeAddr cluster.getAllFrontends().get(0).host, cluster.getAllFrontends().get(0).httpPort

            check { res, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${res}".toString())
                def json = parseJson(res)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }

        def ret = sql """
            select count(*) from $tableName
        """
        logger.info("ret after load, ret {}", ret)

        testCase("compute_cluster", "s3")
        testCase("readS3cluster", "s3")
        testCase("readPeercluster", "peer")
    }
}
