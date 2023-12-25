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

suite('test_schema_change_concurrent_with_txn') {
    def options = new ClusterOptions()
    options.enableDebugPoints()
    options.feConfigs.add('publish_wait_time_second=-1')
    docker(options) {
        sql 'SET GLOBAL insert_visible_timeout_ms = 2000'

        def result = sql 'SELECT DATABASE()'
        def dbName = result[0][0]

        sql 'CREATE TABLE tbl_1 (k1 INT, k2 INT) DISTRIBUTED BY HASH(k1) BUCKETS 10 PROPERTIES ( "light_schema_change" = "false")'
        sql 'INSERT INTO tbl_1 VALUES (1, 10)'
        sql 'INSERT INTO tbl_1 VALUES (2, 20)'
        order_qt_select_1_1 'SELECT * FROM tbl_1'

        sql 'CREATE TABLE tbl_2 AS SELECT * FROM tbl_1'
        order_qt_select_2_1 'SELECT * FROM tbl_2'

        sql 'CREATE TABLE tbl_3 (k1 INT, k2 INT, v INT SUM) AGGREGATE KEY (k1, k2) DISTRIBUTED BY HASH(k1) BUCKETS 10'
        sql 'INSERT INTO tbl_3 VALUES (1, 11, 111)'
        sql 'INSERT INTO tbl_3 VALUES (2, 22, 222)'
        order_qt_select_3_1 'SELECT * FROM tbl_3'

        sql 'CREATE TABLE tbl_4 (k1 INT, k2 INT) DISTRIBUTED BY HASH(k1) BUCKETS 10'
        sql 'INSERT INTO tbl_4 VALUES (1, 10)'
        sql 'INSERT INTO tbl_4 VALUES (2, 20)'
        order_qt_select_4_1 'SELECT * FROM tbl_4'

        // stop publish, insert succ, txn is commit but not visible
        cluster.injectDebugPoints(NodeType.FE, ['PublishVersionDaemon.stop_publish':null])

        sql 'INSERT INTO tbl_1 VALUES (3, 30)'
        sql 'INSERT INTO tbl_1 VALUES (4, 40)'
        order_qt_select_1_2 'SELECT * FROM tbl_1'

        sql 'INSERT INTO tbl_2 VALUES (3, 30)'
        sql 'INSERT INTO tbl_2 VALUES (4, 40)'
        order_qt_select_2_2 'SELECT * FROM tbl_2'

        sql 'INSERT INTO tbl_3 VALUES (3, 33, 333)'
        sql 'INSERT INTO tbl_3 VALUES (4, 44, 444)'
        order_qt_select_3_2 'SELECT * FROM tbl_3'

        sql 'INSERT INTO tbl_4 VALUES (3, 30)'
        sql 'INSERT INTO tbl_4 VALUES (4, 40)'
        order_qt_select_4_2 'SELECT * FROM tbl_4'

        result = sql_return_maparray 'SHOW PROC "/transactions"'
        def runningTxn = result.find { it.DbName.indexOf(dbName) >= 0 }.RunningTransactionNum as int
        assertEquals(8, runningTxn)

        sql "ALTER TABLE tbl_1 ADD COLUMN k3 INT DEFAULT '-1'"
        sql 'CREATE MATERIALIZED VIEW tbl_2_mv AS SELECT k1, k1 + k2 FROM tbl_2'
        sql 'ALTER TABLE tbl_3 ADD ROLLUP tbl_3_r1(k1, v)'
        sql 'ALTER TABLE tbl_4 ORDER BY (k2, k1)'

        sleep(5000)

        def jobs = null

        jobs = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName = 'tbl_1'"
        assertEquals(1, jobs.size())
        assertEquals('WAITING_TXN', jobs[0].State)

        jobs = sql_return_maparray "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName = 'tbl_2'"
        assertEquals(1, jobs.size())
        assertEquals('WAITING_TXN', jobs[0].State)

        jobs = sql_return_maparray "SHOW ALTER TABLE ROLLUP WHERE TableName = 'tbl_3'"
        assertEquals(1, jobs.size())
        assertEquals('WAITING_TXN', jobs[0].State)

        jobs = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName = 'tbl_4'"
        assertEquals(1, jobs.size())
        assertEquals('WAITING_TXN', jobs[0].State)

        sql 'INSERT INTO tbl_1(k1, k2) VALUES (5, 50)'
        sql 'INSERT INTO tbl_2 VALUES (5, 50)'
        sql 'INSERT INTO tbl_3 VALUES (5, 55, 555)'
        sql 'INSERT INTO tbl_4(k1, k2) VALUES (5, 50)'

        // After fe restart, transaction's loadedTblIndexes will clear,
        // then fe will send publish task to all indexes.
        // But the alter index  may add after commit txn, then publish will failed.
        cluster.restartFrontends()
        sleep(30000)
        context.reconnectFe()

        //cluster.clearFrontendDebugPoints()

        // should publish visible
        order_qt_select_1_3 'SELECT * FROM tbl_1'
        order_qt_select_2_3 'SELECT * FROM tbl_2'
        order_qt_select_3_3 'SELECT * FROM tbl_3'
        order_qt_select_4_3 'SELECT * FROM tbl_4'

        jobs = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName = 'tbl_1'"
        assertEquals(1, jobs.size())
        assertEquals('FINISHED', jobs[0].State)

        jobs = sql_return_maparray "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName = 'tbl_2'"
        assertEquals(1, jobs.size())
        assertEquals('FINISHED', jobs[0].State)

        jobs = sql_return_maparray "SHOW ALTER TABLE ROLLUP WHERE TableName = 'tbl_3'"
        assertEquals(1, jobs.size())
        assertEquals('FINISHED', jobs[0].State)

        jobs = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName = 'tbl_4'"
        assertEquals(1, jobs.size())
        assertEquals('FINISHED', jobs[0].State)
    }
}
