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

suite('test_min_load_replica_num_simple') {
    def options = new ClusterOptions()
    options.enableDebugPoints()
    options.feConfigs.add('tablet_checker_interval_ms=1000')
    docker(options) {
        def tbl = 'test_min_load_replica_num_simple_tbl'

        sql "DROP TABLE IF EXISTS ${tbl}"

        sql """
         CREATE TABLE ${tbl} (
           `k1` int(11) NULL,
           `k2` varchar(100) NULL
         )
         DUPLICATE KEY(`k1`, `k2`)
         COMMENT 'OLAP'
         DISTRIBUTED BY HASH(`k1`) BUCKETS 1
         PROPERTIES (
           "replication_num"="3"
         );
         """

        sql """
         INSERT INTO ${tbl} (k1, k2)
         VALUES (1, "a"), (2, "b"), (3, "c"), (4, "e");
         """

        order_qt_select_1 "SELECT * from ${tbl}"

        cluster.stopBackends(2, 3)
        cluster.checkBeIsAlive(2, false)
        cluster.checkBeIsAlive(3, false)

        def be1 = cluster.getBeByIndex(1)
        assert be1 != null
        assert be1.alive

        test {
            sql "INSERT INTO ${tbl} (k1, k2) VALUES (5, 'f'), (6, 'g')"

            // REPLICA_FEW_ERR
            exception 'errCode = 3,'
        }

        sql "ALTER TABLE ${tbl} SET ( 'min_load_replica_num' = '1' )"

        sql "INSERT INTO ${tbl} (k1, k2) VALUES (7, 'h'), (8, 'i')"

        order_qt_select_2 "SELECT * from ${tbl}"

        sql "ALTER TABLE ${tbl} SET ( 'min_load_replica_num' = '-1' )"

        test {
            sql "INSERT INTO ${tbl} (k1, k2) VALUES (9, 'j'), (10, 'k')"

            // REPLICA_FEW_ERR
            exception 'errCode = 3,'
        }

        order_qt_select_3 "SELECT * from ${tbl}"

        cluster.startBackends(2, 3)
        cluster.checkBeIsAlive(2, true)
        cluster.checkBeIsAlive(3, true)

        sql "ADMIN REPAIR TABLE ${tbl}"

        // wait clone finish
        sleep(5000)

        cluster.stopBackends(1)
        cluster.checkBeIsAlive(1, false)

        sql "INSERT INTO ${tbl} (k1, k2) VALUES (11, 'l'), (12, 'm')"

        order_qt_select_4 "SELECT * from ${tbl}"

        cluster.startBackends(1)
        cluster.checkBeIsAlive(1, true)

        sql "TRUNCATE TABLE ${tbl}"
        sql "ADMIN SET FRONTEND CONFIG ('disable_tablet_scheduler' = 'true')"
        sql "ADMIN SET FRONTEND CONFIG ('publish_wait_time_second' = '1')"
        sql 'SET GLOBAL insert_visible_timeout_ms = 1000'

        def backends = cluster.getAllBackends(true)
        assertEquals(3, backends.size())

        def beOk = backends.get(0)
        def beMiss1 = backends.get(1)
        def beMiss2 = backends.get(2)

        // all ok
        streamLoad {
            table "${tbl}"
            set 'column_separator', ','
            file 'test_min_load_replica_num_simple.csv'
            directToBe beOk.host, beOk.httpPort
        }

        order_qt_select_5 "SELECT * from ${tbl}"

        // txn succ, but be 2 failed
        beMiss2.enableDebugPoint('TabletsChannel.open:random_failed', [percent:1.0])
        streamLoad {
            table "${tbl}"
            set 'column_separator', ','
            file 'test_min_load_replica_num_simple.csv'
            directToBe beOk.host, beOk.httpPort
        }
        order_qt_select_6 "SELECT * from ${tbl}"

        // txn failed
        beMiss1.enableDebugPoint('TabletsChannel.open:random_failed', [percent:1.0])
        streamLoad {
            table "${tbl}"
            set 'column_separator', ','
            file 'test_min_load_replica_num_simple.csv'
            directToBe beOk.host, beOk.httpPort

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                if (result == null || !result.toString().contains('Fail')) {
                    throw new IllegalStateException("Exception result failed, but got '${result}'")
                }
            }
        }
        order_qt_select_7 "SELECT * from ${tbl}"

        // txn succ, but be 1, 2 failed
        sql "ALTER TABLE ${tbl} SET ( 'min_load_replica_num' = '1' )"
        streamLoad {
            table "${tbl}"
            set 'column_separator', ','
            file 'test_min_load_replica_num_simple.csv'
            directToBe beOk.host, beOk.httpPort
        }
        sleep(1000)
        order_qt_select_8 "SELECT * from ${tbl}"

        def visibleVersion = sql_return_maparray("SHOW PARTITIONS FROM ${tbl}")[0].VisibleVersion as long
        assertEquals(4, visibleVersion)
        def replicas = sql_return_maparray "SHOW TABLETS FROM ${tbl}"
        assertEquals(3, replicas.size())

        for (def replica : replicas) {
            def backendId = replica.BackendId as long
            def version = replica.Version as long
            def lstFailedVersion = replica.LstFailedVersion as long
            if (backendId == beOk.backendId) {
                assertEquals(visibleVersion, version)
                assertEquals(-1L, lstFailedVersion)
            } else if (backendId == beMiss1.backendId) {
                assertEquals(visibleVersion - 1, version)
                assertEquals(visibleVersion, lstFailedVersion)
            } else if (backendId == beMiss2.backendId) {
                assertEquals(visibleVersion - 2, version)
                assertEquals(visibleVersion, lstFailedVersion)
            } else {
                assertTrue(false)
            }
        }
    }
}
