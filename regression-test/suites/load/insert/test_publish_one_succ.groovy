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

suite('test_publish_one_succ') {
    def options = new ClusterOptions()
    options.enableDebugPoints()
    docker(options) {
        cluster.injectDebugPoints(NodeType.FE, ['PublishVersionDaemon.stop_publish':null])

        sql 'SET GLOBAL insert_visible_timeout_ms = 1000'
        sql "ADMIN SET FRONTEND CONFIG ('publish_wait_time_second' = '1000000')"
        sql 'CREATE TABLE tbl (k1 int, k2 int) DISTRIBUTED BY HASH(`k1`) BUCKETS 10'
        for (def i = 1; i <= 5; i++) {
            sql "INSERT INTO tbl VALUES (${i}, ${10 * i})"
        }

        cluster.stopBackends(2, 3)
        cluster.checkBeIsAlive(2, false)
        cluster.checkBeIsAlive(3, false)
        cluster.clearFrontendDebugPoints()

        sleep(1000)
        order_qt_select_1 'SELECT * FROM tbl'
        sql "ADMIN SET FRONTEND CONFIG ('publish_wait_time_second' = '2')"
        sleep(2000)
        order_qt_select_2 'SELECT * FROM tbl'
    }
}
