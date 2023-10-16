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
    options.feConfigs.add('tablet_checker_interval_ms=1000')
    docker(options) {
        def tbl = 'test_min_load_replica_num_simple_tbl'

        sql """ DROP TABLE IF EXISTS ${tbl} """

        sql """
         CREATE TABLE ${tbl} (
           `k1` int(11) NULL,
           `k2` char(5) NULL
         )
         DUPLICATE KEY(`k1`, `k2`)
         COMMENT 'OLAP'
         DISTRIBUTED BY HASH(`k1`) BUCKETS 2
         PROPERTIES (
           "replication_num"="3"
         );
         """

        sql """
         INSERT INTO ${tbl} (k1, k2)
         VALUES (1, "a"), (2, "b"), (3, "c"), (4, "e");
         """

        qt_select_1 """ SELECT * from ${tbl} ORDER BY k1; """

        cluster.stopBackends(2, 3)
        cluster.checkBeIsAlive(2, false)
        cluster.checkBeIsAlive(3, false)

        def be1 = cluster.getBeByIndex(1)
        assert be1 != null
        assert be1.alive

        test {
            sql """ INSERT INTO ${tbl} (k1, k2) VALUES (5, "f"), (6, "g") """

            // REPLICA_FEW_ERR
            exception 'errCode = 3,'
        }

        sql """ ALTER TABLE ${tbl} SET ( "min_load_replica_num" = "1" ) """

        sql """ INSERT INTO ${tbl} (k1, k2) VALUES (7, "h"), (8, "i") """

        qt_select_2 """ SELECT * from ${tbl} ORDER BY k1; """

        sql """ ALTER TABLE ${tbl} SET ( "min_load_replica_num" = "-1" ) """

        test {
            sql """ INSERT INTO ${tbl} (k1, k2) VALUES (9, "j"), (10, "k") """

            // REPLICA_FEW_ERR
            exception 'errCode = 3,'
        }

        qt_select_3 """ SELECT * from ${tbl} ORDER BY k1; """

        cluster.startBackends(2, 3)
        cluster.checkBeIsAlive(2, true)
        cluster.checkBeIsAlive(3, true)

        sql """ ADMIN REPAIR TABLE ${tbl} """

        // wait clone finish
        sleep(5000)

        cluster.stopBackends(1)
        cluster.checkBeIsAlive(1, false)

        sql """ INSERT INTO ${tbl} (k1, k2) VALUES (11, "l"), (12, "m") """

        qt_select_4 """ SELECT * from ${tbl} ORDER BY k1; """

        sql """ DROP TABLE IF EXISTS ${tbl} """
    }
}
