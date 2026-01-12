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
import org.apache.doris.regression.util.NodeType

suite("test_cloud_mow_retry_txn_interleave", "multi_cluster,docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)
    options.enableDebugPoints()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'calculate_delete_bitmap_task_timeout_seconds=20',
        'mow_calculate_delete_bitmap_retry_times=3'
    ]
    options.beConfigs += [
        'tablet_rowset_stale_sweep_time_sec=0',
        'vacuum_stale_rowsets_interval_s=10',
    ]

    docker(options) {
        try {
            GetDebugPoint().clearDebugPointsForAllFEs()
            GetDebugPoint().clearDebugPointsForAllBEs()

            def table1 = "test_cloud_mow_retry_txn_interleave"
            sql "DROP TABLE IF EXISTS ${table1} FORCE;"
            sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                    `k1` int NOT NULL,
                    `c1` int,
                    `c2` int
                    )UNIQUE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true",
                    "replication_num" = "1"); """

            // add cluster1
            cluster.addBackend(1, "cluster1")
            def ret = sql_return_maparray """show clusters"""
            logger.info("clusters: " + ret)
            def cluster0 = ret.stream().filter(cluster -> cluster.is_current == "TRUE").findFirst().orElse(null)
            def cluster1 = ret.stream().filter(cluster -> cluster.cluster == "cluster1").findFirst().orElse(null)
            assert cluster0 != null
            assert cluster1 != null
            logger.info("cluster0: " + cluster0)
            logger.info("cluster1: " + cluster1)

            // get tablet in cluster0
            def tablets = sql_return_maparray """ show tablets from ${table1}; """
            logger.info("tablets in cluster 0: " + tablets)
            assert 1 == tablets.size()
            def tablet0 = tablets[0]
            def tablet_id = tablet0.TabletId

            // get tablet in cluster1
            sql """use @${cluster1.cluster}"""
            def tablets1 = sql_return_maparray """ show tablets from ${table1}; """
            logger.info("tablets in cluster 1: " + tablets1)
            assert 1 == tablets1.size()
            def tablet1 = tablets1[0]

            def backends = sql_return_maparray """show backends;"""
            def backend0 = backends.stream().filter(be -> be.BackendId == tablet0.BackendId).findFirst().orElse(null)
            assert backend0 != null
            logger.info("backend0: " + backend0)
            def backend1 = backends.stream().filter(be -> be.BackendId == tablet1.BackendId).findFirst().orElse(null)
            assert backend1 != null
            logger.info("backend1: " + backend1)

            // insert data in cluster0
            sql """use @${cluster0.cluster}"""
            sql """ INSERT INTO ${table1} VALUES (1,1,1),(2,2,2),(3,3,3); """
            sql "sync;"
            // read data from cluster0
            qt_sql "select * from ${table1} order by k1;"

            // read data from cluster1
            sql """use @${cluster1.cluster}"""
            qt_sql "select * from ${table1} order by k1;"

            def newThreadInDocker = { Closure actionSupplier ->
                def connInfo = context.threadLocalConn.get()
                return Thread.start {
                    connect(connInfo.username, connInfo.password, connInfo.conn.getMetaData().getURL(), actionSupplier)
                }
            }

            // let load 1's calc task A response halt before report to FE on cluster0 
            GetDebugPoint().enableDebugPoint(backend0.Host, backend0.HttpPort as int, NodeType.BE, "CloudCalcDbmTask.handle.return.block",
                    [tablet_id: "${tablet_id}"])
            
            // let load 2's calc task B fail after update MS delete bitmap on cluster1 
            GetDebugPoint().enableDebugPoint(backend1.Host, backend1.HttpPort as int, NodeType.BE, "CloudCalcDbmTask.handle.return.inject_err",
                    [tablet_id: "${tablet_id}"])


            // load 1 on cluster0
            def t1 = newThreadInDocker {
                sql """use @${cluster0.cluster}"""
                sql "insert into ${table1} values(1,999,999);"
            }

            Thread.sleep(1000)

            // load 2 on cluster1 will fail
            sql """use @${cluster1.cluster}"""
            test {
                sql "insert into ${table1} values(2,888,888);"
                exception "injected error"
            }

            // load 1 will retry on same version. This time, the calc task C will register fail on BE becuase previous task is not finished yet.
            Thread.sleep(1000)

            // let FE recieve task A's response
            GetDebugPoint().disableDebugPoint(backend0.Host, backend0.HttpPort as int, NodeType.BE, "CloudCalcDbmTask.handle.return.block")
            // wait for load 1 finish
            t1.join()

            // force it read delete bitmaps from MS rather than BE's cache
            GetDebugPoint().enableDebugPointForAllBEs("CloudTxnDeleteBitmapCache::get_delete_bitmap.cache_miss")

            sql """use @${cluster0.cluster}"""
            qt_dup_key_count "select count() from (select k1,count() as cnt from ${table1} group by k1 having cnt > 1) A;"
            qt_sql "select * from ${table1} order by k1,c1,c2;"

        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
            GetDebugPoint().clearDebugPointsForAllFEs()
        }
    }
}
