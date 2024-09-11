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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.NodeType

suite("txn_insert_restart_fe", 'docker') {
    def get_observer_fe_url = {
        def fes = sql_return_maparray "show frontends"
        logger.info("frontends: ${fes}")
        if (fes.size() > 1) {
            for (def fe : fes) {
                if (fe.IsMaster == "false") {
                    return "jdbc:mysql://${fe.Host}:${fe.QueryPort}/"
                }
            }
        }
        return null
    }

    def options = new ClusterOptions()
    options.setFeNum(2)
    options.enableDebugPoints()
    options.feConfigs.add('publish_wait_time_second=-1')
    options.feConfigs.add('commit_timeout_second=10')
    options.feConfigs.add('sys_log_verbose_modules=org.apache.doris')
    // options.beConfigs.add('sys_log_verbose_modules=*')
    options.beConfigs.add('enable_java_support=false')
    docker(options) {
        // ---------- test restart fe ----------
        def result = sql 'SELECT DATABASE()'
        def dbName = result[0][0]

        sql 'CREATE TABLE tbl_1 (k1 INT, k2 INT) DISTRIBUTED BY HASH(k1) BUCKETS 10 PROPERTIES ( "light_schema_change" = "false")'
        sql 'INSERT INTO tbl_1 VALUES (1, 11)'
        order_qt_select_1 'SELECT * FROM tbl_1'

        sql 'CREATE TABLE tbl_2 (k1 INT, k2 INT) DISTRIBUTED BY HASH(k1) BUCKETS 10 PROPERTIES ( "light_schema_change" = "false")'
        sql 'INSERT INTO tbl_2 VALUES (2, 11)'
        sql 'begin'
        sql 'INSERT INTO tbl_2 SELECT * FROM tbl_1'
        sql 'INSERT INTO tbl_2 SELECT * FROM tbl_1'
        sql 'commit'
        order_qt_select_2 'SELECT * FROM tbl_2'

        // stop publish, insert succ, txn is commit but not visible
        cluster.injectDebugPoints(NodeType.FE, ['PublishVersionDaemon.stop_publish':null])

        sql 'begin'
        sql 'INSERT INTO tbl_2 SELECT * FROM tbl_1'
        sql 'INSERT INTO tbl_2 SELECT * FROM tbl_1'
        sql 'commit'
        order_qt_select_3 'SELECT * FROM tbl_2'

        sql 'begin'
        sql 'INSERT INTO tbl_2 SELECT * FROM tbl_1'
        sql 'INSERT INTO tbl_2 SELECT * FROM tbl_1'
        sql 'commit'
        order_qt_select_4 'SELECT * FROM tbl_2'

        sql 'INSERT INTO tbl_2 SELECT * FROM tbl_1'
        sql 'INSERT INTO tbl_2 VALUES(3, 11)'
        order_qt_select_5 'SELECT * FROM tbl_2'

        // select from observer
        def observer_fe_url = get_observer_fe_url()
        if (observer_fe_url != null) {
            logger.info("observer url: $observer_fe_url")
            connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = observer_fe_url) {
                order_qt_select_observer """ select * from ${dbName}.tbl_2 """
            }
        }

        result = sql_return_maparray 'SHOW PROC "/transactions"'
        logger.info("show txn result: ${result}")
        def runningTxn = result.find { it.DbName.indexOf(dbName) >= 0 }.RunningTransactionNum as int
        assertEquals(4, runningTxn)

        cluster.restartFrontends()
        sleep(30000)
        context.reconnectFe()

        // should publish visible
        order_qt_select_6 'SELECT * FROM tbl_2'

        sql 'begin'
        sql 'INSERT INTO tbl_2 SELECT * FROM tbl_1'
        sql 'INSERT INTO tbl_2 SELECT * FROM tbl_1'
        sql 'commit'
        order_qt_select_7 'SELECT * FROM tbl_2'

        if (observer_fe_url != null) {
            logger.info("observer url: $observer_fe_url")
            connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = observer_fe_url) {
                order_qt_select_observer_2 """ select * from ${dbName}.tbl_2 """
            }
        }
    }
}
