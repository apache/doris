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

suite('test_read_from_peer', 'docker') {
    if (!isCloudMode()) {
        return
    }

    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1',
        'auto_check_statistics_in_minutes=60',
    ]
    options.beConfigs += [
        'report_tablet_interval_seconds=1',
        'schedule_sync_tablets_interval_s=18000',
        'disable_auto_compaction=true',
        'enable_cache_read_from_peer=true',
        'enable_peer_s3_race=true',
        'max_concurrent_peer_races=64',
        'file_cache_each_block_size=131072',
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'file_cache_exit_disk_resource_limit_mode_percent=98',
        'file_cache_enter_need_evict_cache_in_advance_percent=99',
        'file_cache_exit_need_evict_cache_in_advance_percent=98',
    ]
    options.beConfigs.removeAll { it.startsWith('sys_log_verbose_modules=') }
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true
    options.enableDebugPoints()

    def getBrpcMetric = { ip, port, name ->
        def url = "http://${ip}:${port}/brpc_metrics"
        if ((context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false) {
            url = url.replace("http://", "https://") +
                " --cert " + context.config.otherConfigs.get("trustCert") +
                " --cacert " + context.config.otherConfigs.get("trustCACert") +
                " --key " + context.config.otherConfigs.get("trustCAKey")
        }
        def metrics = new URL(url).text
        def matcher = metrics =~ ~"${name}\\s+(\\d+)"
        if (matcher.find()) {
            return matcher[0][1] as long
        }
        return 0L
    }

    docker(options) {
        def tableName = "test_read_from_table"

        def clusterBe = { clusterName ->
            def be = sql_return_maparray("show backends").find { backend ->
                backend.Tag.contains(clusterName)
            }
            assertTrue(be != null, "backend for ${clusterName} should exist")
            be
        }

        def insertFreshRowsOnCompute = { List<String> rows ->
            sql "use @compute_cluster"
            sql "INSERT INTO ${tableName} VALUES ${rows.join(', ')}"
        }

        def queryFreshRows = { clusterName, List<Long> ticketNumbers ->
            sql "use @${clusterName}"
            sql """
                SELECT ss_ticket_number
                FROM ${tableName}
                WHERE ss_ticket_number IN (${ticketNumbers.join(',')})
                ORDER BY ss_ticket_number
            """
        }

        def scanCountRows = { clusterName ->
            sql "use @${clusterName}"
            sql "set enable_push_down_no_group_agg=false"
            sql "SELECT count(ss_ticket_number) FROM ${tableName}"
        }

        def warmFreshRowsOnCompute = { List<Long> ticketNumbers ->
            def rows = queryFreshRows("compute_cluster", ticketNumbers)
            assertEquals(ticketNumbers.size(), rows.size(), "compute_cluster should read all expected fresh rows")
        }

        cluster.addBackend(1, "readS3cluster")
        cluster.addBackend(1, "readPeercluster")

        awaitUntil(120) {
            def backends = sql_return_maparray("show backends")
            backends.size() == 3 && backends.every { backend ->
                backend.Alive.toString() == "true" && backend.TabletNum.toInteger() > 0
            }
        }

        def beS3 = clusterBe("readS3cluster")
        def bePeer = clusterBe("readPeercluster")

        sql "use @compute_cluster"
        sql "DROP TABLE IF EXISTS ${tableName}"
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

        streamLoad {
            table "${tableName}"
            set 'column_separator', '|'
            set 'compress_type', 'GZ'
            file """${getS3Url()}/regression/tpcds/sf1/store_sales.dat.gz"""
            time 10000
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

        def computeCount = scanCountRows("compute_cluster")
        logger.info("count on compute_cluster={}", computeCount)
        assertTrue((computeCount[0][0] as long) > 0L, "compute_cluster should have loaded rows")

        logger.info("=== readS3cluster: first cold read succeeds ===")
        def s3Read = scanCountRows("readS3cluster")
        assertEquals(computeCount, s3Read, "readS3cluster should read loaded source data")

        logger.info("=== readPeercluster: baseline read succeeds and populates peer candidates ===")
        def peerLazyBefore = getBrpcMetric(bePeer.Host, bePeer.BrpcPort, "peer_lazy_fetch_success")
        def baselinePeerRead = scanCountRows("readPeercluster")
        assertEquals(computeCount, baselinePeerRead, "readPeercluster baseline read should succeed")
        awaitUntil(30) {
            getBrpcMetric(bePeer.Host, bePeer.BrpcPort, "peer_lazy_fetch_success") > peerLazyBefore
        }

        logger.info("=== readPeercluster: fresh rowset should read from peer ===")
        def freshTicketNumbers = [900000000001L, 900000000002L]
        def freshRows = [
            "(900001, 1, 1, 900001, 1, 1, 1, 1, 1, 900000000001, 1, 1.00, 2.00, 2.00, 0.00, 2.00, 1.00, 2.00, 0.10, 0.00, 2.10, 2.10, 1.10)",
            "(900002, 2, 2, 900002, 2, 2, 2, 2, 2, 900000000002, 2, 2.00, 3.00, 3.00, 0.00, 6.00, 4.00, 6.00, 0.20, 0.00, 6.20, 6.20, 2.20)"
        ]
        insertFreshRowsOnCompute(freshRows)
        warmFreshRowsOnCompute(freshTicketNumbers)

        def peerWinBefore = getBrpcMetric(bePeer.Host, bePeer.BrpcPort, "peer_race_peer_win")
        def crossCgBefore = getBrpcMetric(bePeer.Host, bePeer.BrpcPort, "peer_cross_compute_group_read")

        def peerRead = queryFreshRows("readPeercluster", freshTicketNumbers)
        assertEquals(freshTicketNumbers.size(), peerRead.size(), "readPeercluster should read fresh rows")

        def peerWinAfter = getBrpcMetric(bePeer.Host, bePeer.BrpcPort, "peer_race_peer_win")
        def crossCgAfter = getBrpcMetric(bePeer.Host, bePeer.BrpcPort, "peer_cross_compute_group_read")
        assertTrue(
            peerWinAfter > peerWinBefore || crossCgAfter > crossCgBefore,
            "peer read counters should increase on readPeercluster " +
            "(peer_race_peer_win ${peerWinBefore}->${peerWinAfter}, " +
            "peer_cross_compute_group_read ${crossCgBefore}->${crossCgAfter})")

        logger.info("=== readPeercluster PASS ===")
        logger.info("readS3cluster brpc={}, readPeercluster brpc={}", beS3.BrpcPort, bePeer.BrpcPort)
    }
}
